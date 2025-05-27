from utils import *
import pandas as pd
import json  # Ensure json is imported


def generate_excel_for_count_posts(data, platform):
    # 将数据转换为DataFrame
    df = pd.DataFrame.from_dict(data, orient="index")

    if platform == "wb":
        df = df[
            [
                "total_posts",
                "total_replies",
                "total",
                "范围内帖子",
                "范围内评论",
                "范围内总数",
                "hotel_related_posts",
                "hotel_related_replies",
                "有效数据数量",
                "最早时间",
                "最晚时间",
                "内容不完整帖子占比有效帖子",
            ]
        ]
    elif platform == "xhs":
        df = df[
            [
                "total_posts",
                "total_replies",
                "total",
                "范围内帖子",
                "范围内评论",
                "范围内总数",
                "软文数量",
                "hotel_related_posts",
                "hotel_related_replies",
                "有效数据数量",
                "最早时间",
                "最晚时间",
            ]
        ]
    else:
        df = df[
            [
                "total_posts",
                "total_replies",
                "total",
                "范围内帖子",
                "范围内评论",
                "范围内总数",
                "hotel_related_posts",
                "hotel_related_replies",
                "有效数据数量",
                "最早时间",
                "最晚时间",
            ]
        ]

    # 使用ExcelWriter来处理多个sheet的写入
    try:
        with pd.ExcelWriter(
            "analysis_result/数据量统计.xlsx",
            mode="a",
            engine="openpyxl",
            if_sheet_exists="replace",
        ) as writer:
            df.to_excel(writer, sheet_name=f"{platform}帖子统计")
    except FileNotFoundError:
        # 如果文件不存在，创建新文件
        df.to_excel("analysis_result/数据量统计.xlsx", sheet_name=f"{platform}帖子统计")

    print("Excel文件已生成：analysis_result/数据量统计.xlsx")


def count_posts(platform):
    filter = PostsFilter()
    raw_data = get_raw_data(f"raw_data/{platform}.json")
    filtered_data = filter.filter_by_time(raw_data)

    if not raw_data:
        return None

    count = {}
    for hotel in raw_data:
        latest_time = 0
        earliest_time = float("inf")

        posts_count = len(hotel["posts"])
        total_replies = 0
        for post in hotel["posts"]:
            total_replies += len(post["replies"])
        if len(hotel["posts"]) == 0:
            print(f"警告：酒店 '{hotel['hotel']}' 没有帖子，跳过统计。")
            continue
        for post in hotel["posts"]:
            timestamp = datetime.strptime(
                post["timestamp"], "%Y-%m-%d %H:%M"
            ).timestamp()
            if timestamp < earliest_time:
                earliest_time = timestamp
            if timestamp > latest_time:
                latest_time = timestamp
        tmp = {
            "total_posts": posts_count,
            "total_replies": total_replies,
            "total": posts_count + total_replies,
            "最早时间": datetime.fromtimestamp(earliest_time).strftime(
                "%Y-%m-%d %H:%M"
            ),
            "最晚时间": datetime.fromtimestamp(latest_time).strftime("%Y-%m-%d %H:%M"),
        }
        count[hotel["hotel"]] = tmp

    # 读取analyzed数据，只需读取一次
    analyzed_data = None
    try:
        with open(
            f"analysis_result/{platform}_analyzed.json", "r", encoding="utf-8"
        ) as f:
            analyzed_data = json.load(f)
    except FileNotFoundError:
        print(
            f"警告：无法找到 {platform}_analyzed.json 文件，将无法统计有效数据和软文数量。"
        )
    except Exception as e:
        print(f"读取 {platform}_analyzed.json 文件时出错: {e}")

    # 将 analyzed_data 转换为字典以便快速查找
    analyzed_hotel_map = {h["hotel"]: h for h in analyzed_data} if analyzed_data else {}

    for hotel in filtered_data:
        hotel_name = hotel["hotel"]
        replies_count = 0
        hotel_related_posts = 0
        hotel_related_replies = 0
        ad_count = 0  # 初始化 ad_count
        incomplete_posts = 0

        posts_count = len(hotel["posts"])
        if hotel_name not in count:
            print(f"警告：酒店 '{hotel_name}' 在 raw_data 中未找到对应条目，跳过统计。")
            continue

        count[hotel_name]["范围内帖子"] = posts_count

        # 获取对应的 analyzed_hotel 数据
        analyzed_hotel = analyzed_hotel_map.get(hotel_name)

        # --- 修正开始 ---
        # 在这里一次性计算该酒店的软文数量 (ad_count)
        if analyzed_hotel and platform == "xhs":
            for analyzed_post in analyzed_hotel.get("posts", []):
                if analyzed_post.get("is_ad", False):
                    ad_count += 1
            count[hotel_name]["软文数量"] = ad_count
        elif platform == "xhs":
            count[hotel_name]["软文数量"] = 0  # 如果没有 analyzed_hotel，软文数为0
        # --- 修正结束 ---

        for post in hotel["posts"]:
            replies_count += len(post["replies"])

            if "...全文" in post["content"]:
                incomplete_posts += 1

            # 统计酒店相关帖子和评论 (使用 analyzed_hotel)
            if analyzed_hotel:
                # 查找与当前 post 匹配的 analyzed_post
                matching_analyzed_post = None
                for ap in analyzed_hotel.get("posts", []):
                    if ap.get("link") == post.get("link"):  # 假设 link 是唯一标识符
                        matching_analyzed_post = ap
                        break

                if matching_analyzed_post and matching_analyzed_post.get(
                    "is_hotel_related", False
                ):
                    hotel_related_posts += 1
                    # 统计该帖子下的酒店相关评论
                    for reply in matching_analyzed_post.get("replies", []):
                        if reply.get("is_hotel_related", False):
                            hotel_related_replies += 1

        count[hotel_name]["范围内评论"] = replies_count
        count[hotel_name]["范围内总数"] = posts_count + replies_count
        # 有效数据数量统计逻辑不变
        count[hotel_name]["hotel_related_posts"] = hotel_related_posts
        count[hotel_name]["hotel_related_replies"] = hotel_related_replies
        count[hotel_name]["有效数据数量"] = (
            hotel_related_posts + hotel_related_replies - ad_count
        )

        # 内容不完整帖子占比统计逻辑不变
        if platform == "wb":
            if posts_count == 0:
                count[hotel_name]["内容不完整帖子占比有效帖子"] = "0%"
            else:
                count[hotel_name][
                    "内容不完整帖子占比有效帖子"
                ] = f"{round(incomplete_posts/posts_count * 100)}%"

    generate_excel_for_count_posts(count, platform)
    print("共有{}个酒店".format(len(count)))
    print("酒店列表： ", list(count.keys()))


if __name__ == "__main__":
    pass
    # count_posts("xhs")
    # count_posts("wb")
    # count_posts("flyert")
