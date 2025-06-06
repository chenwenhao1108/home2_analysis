from datetime import datetime, timedelta
import json
import os
from pprint import pprint
import random
import re
from unittest import result
from dotenv import load_dotenv

from openai import OpenAI

load_dotenv()


class OpenAIService:
    """Service class for OpenAI API interactions."""

    def __init__(self):
        self.client = OpenAI(
            api_key=os.environ.get("OPENAI_API_KEY"),
            base_url=os.environ.get("OPENAI_API_BASE"),
        )

    def infer(
        self,
        user_prompt: str,
        system_prompt: str,
        model: str = "gpt-4.1-mini",
        temperature: float = 0.8,
        retries: int = 3,
    ):
        """Make an inference using OpenAI API."""
        for attempt in range(retries):
            try:
                completion = self.client.chat.completions.create(
                    model=model,
                    messages=[
                        ({"role": "system", "content": system_prompt}),
                        {"role": "user", "content": user_prompt},
                    ],
                    timeout=300,
                    temperature=temperature,
                )
                res_raw = completion.choices[0].message.content

                # Try to parse JSON if present
                pattern = re.compile(r"```json\s*([\s\S]*?)\s*```")
                matches = pattern.findall(res_raw) if res_raw else None
                if matches:
                    try:
                        return json.loads(matches[0], strict=False)
                    except json.JSONDecodeError as e:
                        user_prompt += f"""**请严格按照要求的json格式返回结果，确保json格式正确，且不要返回多余的解释和注释**
                        请注意避免出现如下报错：
                        ```
                        {e}
                        ```
                        """
                        continue
                else:
                    user_prompt += "**请严格按照要求的json格式返回结果，确保json格式正确，且不要返回多余的解释和注释**"
                    continue

            except Exception as e:
                print(f"OpenAI API call failed (attempt {attempt + 1}/{retries}): {e}")
                if attempt == retries - 1:
                    raise


class PostsFilter:
    def __init__(self, start_date=datetime(2024, 3, 1), end_date=datetime(2025, 2, 28)):
        self.start_date = start_date
        self.end_date = end_date
        self.data = []

    def simplify_data(self, data):
        """
        去掉raw_data中的author字段
        去掉replies中每个reply的commenter_name和commenter_link字段
        """
        simplified_data = []
        for hotel in data:
            simplified_posts = []
            for post in hotel["posts"]:
                simplified_post = {
                    "content": post["content"],
                    "timestamp": post["timestamp"],
                    "link": post["link"],
                    "replies": [
                        {
                            "content": reply["comment_content"],
                            "timestamp": reply["comment_time"],
                        }
                        for reply in post["replies"]
                    ],
                }
                # 检查 'note_id' 是否存在于 post 中，如果存在则添加
                if "note_id" in post:
                    simplified_post["note_id"] = post["note_id"]
                simplified_posts.append(simplified_post)

            simplified_hotel = {"hotel": hotel["hotel"], "posts": simplified_posts}
            simplified_data.append(simplified_hotel)
        return simplified_data

    def filter_by_time(self, raw_data):
        """
        从帖子列表中筛选出指定时间范围内的飞客茶馆帖子, 并且去掉多余字段

        Args:
            raw_data: 原始飞客茶馆数据

        Returns:
            list: 筛选后的帖子列表
        """
        self.data = []
        for hotel in raw_data:
            filtered_posts = []
            for post in hotel["posts"]:
                if post.get("timestamp", None):
                    try:
                        post_time = datetime.strptime(
                            post["timestamp"], "%Y-%m-%d %H:%M"
                        )
                        if self.start_date <= post_time <= self.end_date:
                            # filter replies
                            replies = []
                            for reply in post["replies"]:
                                if reply.get("timestamp", None):
                                    try:
                                        reply_time = datetime.strptime(
                                            reply["comment_time"], "%Y-%m-%d %H:%M"
                                        )
                                        if (
                                            self.start_date
                                            <= reply_time
                                            <= self.end_date
                                        ):
                                            tmp = {
                                                "content": reply["content"],
                                                "timestamp": reply["comment_time"],
                                            }
                                            replies.append(tmp)
                                    except ValueError:
                                        print(
                                            f"无法解析时间格式: {reply['comment_time']}"
                                        )

                            tmp = {
                                "link": post["link"],
                                "timestamp": post["timestamp"],
                                "content": post["content"],
                                "replies": replies,
                            }
                            filtered_posts.append(post)
                    except ValueError:
                        print(f"无法解析时间格式: {post['timestamp']}")
                        print(f"跳过此条帖子: {post['link']}")

            self.data.append(
                {
                    "hotel": hotel["hotel"],
                    "posts": filtered_posts,
                }
            )
        return self.data

    def get_posts_by_hotel(self, raw_data, platform, hotel_name):
        """
        根据酒店名称获取该酒店的所有帖子

        Args:
            hotel_name: 酒店名称

        Returns:
            list: 该酒店的所有帖子
        """
        if platform == "flyert":
            data = self.filter_by_time(raw_data)
        else:
            print("Invalid platform")
            return []
        for hotel in data:
            if hotel["hotel"] == hotel_name:
                return hotel["posts"]
        return []


class Keywords:
    @staticmethod
    def get_keywords():
        keywords_path = "raw_data/keywords.json"
        if os.path.exists(keywords_path):
            with open(keywords_path, "r", encoding="utf-8") as f:
                return json.load(f)
        else:
            raise FileNotFoundError(f"File {keywords_path} does not exist")

    @staticmethod
    def get_all_keywords_str():
        keywords = Keywords.get_keywords()
        formatted_keywords = {
            "primary_keyword": [],
            "secondary_keyword": [],
        }
        for keyword in keywords:
            formatted_keywords["primary_keyword"].append(keyword["primary_keyword"])
            for sec_keyword in keyword["secondary_keywords"]:
                formatted_keywords["secondary_keyword"].append(sec_keyword["keyword"])
        return json.dumps(formatted_keywords, ensure_ascii=False, indent=2)

    @staticmethod
    def get_keywords_with_description():
        keywords_with_description = get_raw_data(
            "analysis_result/keywords_with_description.json"
        )
        if not keywords_with_description:
            raise Exception("keywords_with_description.json文件不存在")
        return keywords_with_description

    @staticmethod
    def get_valid_keywords():
        """从 keywords.json 数据中提取所有有效关键词到一个集合中"""
        keywords_data = Keywords.get_keywords()
        valid_keywords = set()
        if not keywords_data:
            return valid_keywords

        for item in keywords_data:
            if "primary_keyword" in item:
                # 移除可能的空格以便匹配
                valid_keywords.add(item["primary_keyword"].replace(" ", ""))
            if "secondary_keywords" in item:
                for sec_kw in item["secondary_keywords"]:
                    if "keyword" in sec_kw:
                        # 移除可能的空格以便匹配
                        valid_keywords.add(sec_kw["keyword"].replace(" ", ""))
        return valid_keywords

    @staticmethod
    def filter_mentioned_keywords(mentioned_data):
        """过滤单个 'keywords_mentioned' 对象，移除无效关键词"""
        if not isinstance(mentioned_data, dict):
            # 如果输入不是预期的字典格式，返回空字典或进行错误处理
            print(
                f"警告：预期的 mentioned_data 是字典，但收到了 {type(mentioned_data)}"
            )
            return {}

        valid_keywords = Keywords.get_valid_keywords()
        filtered_data = {}

        # 过滤 primary_keyword
        if "primary_keyword" in mentioned_data and isinstance(
            mentioned_data["primary_keyword"], list
        ):
            filtered_primary = []
            for kw in mentioned_data["primary_keyword"]:
                if (
                    isinstance(kw, dict)
                    and kw.get("keyword")
                    and kw["keyword"].replace(" ", "") in valid_keywords
                ):
                    formatted_kw = Keywords.format_keyword(kw["keyword"])
                    kw["keyword"] = formatted_kw
                    if formatted_kw:
                        filtered_primary.append(kw)
            if filtered_primary:  # 只有列表不为空时才添加
                filtered_data["primary_keyword"] = filtered_primary

        # 过滤 secondary_keyword
        if "secondary_keyword" in mentioned_data and isinstance(
            mentioned_data["secondary_keyword"], list
        ):
            filtered_secondary = []
            for kw in mentioned_data["secondary_keyword"]:
                if (
                    isinstance(kw, dict)
                    and kw.get("keyword")
                    and kw["keyword"].replace(" ", "") in valid_keywords
                ):
                    formatted_kw = Keywords.format_keyword(kw["keyword"])
                    kw["keyword"] = formatted_kw
                    if formatted_kw:
                        filtered_secondary.append(kw)
            if filtered_secondary:  # 只有列表不为空时才添加
                filtered_data["secondary_keyword"] = filtered_secondary

        # 如果过滤后 primary 和 secondary 都为空，则返回空字典
        if not filtered_data.get("primary_keyword") and not filtered_data.get(
            "secondary_keyword"
        ):
            return {}

        return filtered_data

    @staticmethod
    def format_keyword(keyword):
        keywords = []
        keywords_data = Keywords.get_keywords()
        for keyword_dict in keywords_data:  # type: ignore
            pk = keyword_dict["primary_keyword"]
            if pk not in keywords:
                keywords.append(pk)

            for sk_dict in keyword_dict["secondary_keywords"]:
                sk = sk_dict["keyword"]
                if sk not in keywords:
                    keywords.append(sk)

        for standard_keyword in keywords:
            if standard_keyword.replace(" ", "") == keyword.replace(" ", ""):
                return standard_keyword

        return None

    @staticmethod
    def get_sk_to_pk_map():
        """
        从关键词数据中提取出sk到pk的映射
        :param keywords_data: 关键词数据
        :return: sk到pk的映射
        """

        keywords_data = Keywords.get_keywords()

        sk_to_kw_map = {}
        for keyword_dict in keywords_data:
            pk = keyword_dict["primary_keyword"]
            for sk_dict in keyword_dict["secondary_keywords"]:
                sk = sk_dict["keyword"]
                sk_to_kw_map[sk] = pk
        return sk_to_kw_map

    @staticmethod
    def is_primary_keyword(keyword):
        """
        检查给定的关键词是否为主关键词
        """
        sk_to_pk_map = Keywords.get_sk_to_pk_map()
        return keyword in sk_to_pk_map.values()


def get_raw_data(path):
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    else:
        print("path不存在")
        return None


def write_to_json(data, path):
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
    except Exception as e:
        print(f"写入文件时发生错误: {e}")
        raise


def rearrange_flyert_data():
    links_path = os.path.join("raw_data", "flyert_links.json")
    absolute_links_path = os.path.abspath(links_path)
    raw_data_path = os.path.join("raw_data", "flyert.json")
    absolute_raw_data_path = os.path.abspath(raw_data_path)
    analyzed_data_path = os.path.join("analysis_result", "flyert_analyzed.json")
    absolute_analyzed_data_path = os.path.abspath(analyzed_data_path)

    with open(absolute_links_path, "r", encoding="utf-8") as f:
        links = json.load(f)

    with open(absolute_raw_data_path, "r", encoding="utf-8") as f:
        raw_data = json.load(f)

    with open(absolute_analyzed_data_path, "r", encoding="utf-8") as f:
        analyzed_data = json.load(f)

    raw_posts_list = [post for hotel in raw_data for post in hotel["posts"]]
    analyzed_posts_list = [post for hotel in analyzed_data for post in hotel["posts"]]

    new_raw_data = []
    new_analyzed_data = []
    for hotel in links:
        hotel_name = hotel["hotel"]
        hotel_links = hotel["links"]
        tmp_raw = {"hotel": hotel_name, "posts": []}
        for post in raw_posts_list:
            if post["link"] in hotel_links:
                tmp_raw["posts"].append(post)

        tmp_analyzed = {"hotel": hotel_name, "posts": []}
        for post in analyzed_posts_list:
            if post["link"] in hotel_links:
                tmp_analyzed["posts"].append(post)
        new_raw_data.append(tmp_raw)
        new_analyzed_data.append(tmp_analyzed)

    with open(analyzed_data_path, "w", encoding="utf-8") as f:
        json.dump(new_analyzed_data, f, ensure_ascii=False, indent=4)

    with open(raw_data_path, "w", encoding="utf-8") as f:
        json.dump(new_raw_data, f, ensure_ascii=False, indent=4)


def format_xhs_data_from_mobile(file_path, hotel_name):
    """
    将从移动端爬取的xhs数据格式化成与flyert数据格式相同的格式，以便于分析
    """
    with open("raw_data/xhs.json", "r", encoding="utf-8") as f:
        existing_data = json.load(f)

    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # 获取已有的帖子内容用于去重
    existing_post_contents = []
    for hotel in existing_data:
        if hotel["hotel"] == hotel_name:
            existing_post_contents = [post["content"] for post in hotel["posts"]]
            break

    new_data = [{"hotel": hotel_name, "posts": []}]
    for post in data:
        content = post.get("title", "") + post.get("body", "")
        if not content:
            continue

        # 通过content进行去重
        if content in existing_post_contents:
            continue

        if post.get("timestamp_location", None):
            parsed_timestamp = parse_timestamp(post["timestamp_location"])
        elif post.get("timestamp", None):
            parsed_timestamp = parse_timestamp(post["timestamp"])
        else:
            print("没有时间戳")
            continue
        if not parsed_timestamp:
            continue
        tmp = {
            "note_id": "",
            "content": content,
            "timestamp": parsed_timestamp,
            "link": "",
            "replies": [],
        }
        if post.get("comments", None):
            for comment in post["comments"]:
                if comment.get("date_location", None):
                    parsed_comment_timestamp = parse_timestamp(comment["date_location"])
                    if not parsed_comment_timestamp:
                        continue
                else:
                    print("没有时间戳")
                    continue

                tmp["replies"].append(
                    {
                        "commenter_name": "",
                        "comment_content": comment["comment_text"],
                        "commenter_link": "",
                        "comment_time": parsed_comment_timestamp,
                    }
                )
        new_data[0]["posts"].append(tmp)

    return new_data


def format_all_xhs_data_from_mobile(data_path, hotel_names):

    all_data = []
    for path, hotel_name in zip(data_path, hotel_names):
        data = format_xhs_data_from_mobile(path, hotel_name)
        all_data.extend(data)

    return all_data


def merge_data(formatted_data, existing_data_path):
    """
    将格式化或分析后的数据合并到已有的数据中，并根据 post['content'] 进行去重。
    """
    existing_data = get_raw_data(existing_data_path)
    if existing_data is None:
        print(f"警告: {existing_data_path} 不存在或为空")
        raise

    for formatted_hotel in formatted_data:
        found_hotel = False
        for existing_hotel in existing_data:
            if formatted_hotel["hotel"] == existing_hotel["hotel"]:
                found_hotel = True
                # 获取现有帖子的内容集合，用于去重
                existing_contents = {
                    post["content"]
                    for post in existing_hotel["posts"]
                    if "content" in post
                }

                new_posts_to_add = []
                for post_to_add in formatted_hotel["posts"]:
                    if (
                        "content" in post_to_add
                        and post_to_add["content"] not in existing_contents
                    ):
                        new_posts_to_add.append(post_to_add)
                        existing_contents.add(
                            post_to_add["content"]
                        )  # 将新添加的内容也加入，防止formatted_hotel内部重复

                existing_hotel["posts"].extend(new_posts_to_add)
                break

        if not found_hotel:
            # 如果在 existing_data 中没有找到对应的酒店，则将整个 formatted_hotel 添加进去
            # (确保其帖子也是唯一的，因为formatted_data内部可能有重复)
            unique_posts_for_new_hotel = []
            seen_contents_for_new_hotel = set()
            for post in formatted_hotel["posts"]:
                if (
                    "content" in post
                    and post["content"] not in seen_contents_for_new_hotel
                ):
                    unique_posts_for_new_hotel.append(post)
                    seen_contents_for_new_hotel.add(post["content"])
            if unique_posts_for_new_hotel:  # 只有当有帖子时才添加酒店
                existing_data.append(
                    {
                        "hotel": formatted_hotel["hotel"],
                        "posts": unique_posts_for_new_hotel,
                    }
                )

    write_to_json(existing_data, existing_data_path)
    return existing_data


def format_iso_timestamp_to_custom(iso_timestamp_str):
    """
    将ISO格式的时间戳字符串 (例如 '2024-08-09 16:55:35+08:00')
    转换为 'YYYY-MM-DD HH:MM' 格式。

    Args:
        iso_timestamp_str (str): ISO格式的时间戳字符串。

    Returns:
        str: 'YYYY-MM-DD HH:MM' 格式的时间戳字符串，如果解析失败则返回 None。
    """
    if not iso_timestamp_str or not isinstance(iso_timestamp_str, str):
        # print(f"输入无效: {iso_timestamp_str} 不是一个有效的字符串")
        return None
    try:
        # datetime.fromisoformat 可以直接处理这种带时区信息的格式
        # 对于 '2024-08-09 16:55:35+08:00' 这种中间有空格的，需要替换成 'T'
        # 或者，如果确定总是 +08:00，也可以先移除时区部分再解析
        # 更通用的方法是尝试直接解析，如果失败，再尝试替换空格
        dt_object = None
        try:
            dt_object = datetime.fromisoformat(iso_timestamp_str)
        except ValueError:
            # 尝试替换空格为 'T'，因为 fromisoformat 期望 'T' 作为日期和时间的分隔符
            # 对于 'YYYY-MM-DD HH:MM:SS+HH:MM' 格式，Python 3.11+ 的 fromisoformat 可以直接处理空格
            # 但为了兼容性，我们还是处理一下
            if " " in iso_timestamp_str and "+" in iso_timestamp_str:
                parts = iso_timestamp_str.split(" ")
                if len(parts) == 2 and ":" in parts[1]:  # 确保是 日期 时间+时区 的形式
                    # 检查时间部分是否包含秒和时区
                    time_part_with_tz = parts[1]
                    if time_part_with_tz.count(":") >= 2 and (
                        "+" in time_part_with_tz or "-" in time_part_with_tz
                    ):
                        # 看起来像 'HH:MM:SS+HH:MM' 或 'HH:MM:SS-HH:MM'
                        iso_compatible_str = parts[0] + "T" + parts[1]
                        try:
                            dt_object = datetime.fromisoformat(iso_compatible_str)
                        except ValueError:
                            # print(f"尝试替换空格后仍然无法解析时间格式: {iso_timestamp_str}")
                            return None  # 如果替换后还不行，则放弃
                    else:
                        # print(f"时间部分格式不符合预期: {time_part_with_tz}")
                        return None  # 时间部分不符合 'HH:MM:SS+HH:MM' 格式
                else:
                    # print(f"字符串格式不符合 '日期 时间+时区' 的预期: {iso_timestamp_str}")
                    return None  # 格式不符合预期
            else:
                # print(f"无法直接解析且格式不包含空格和时区指示: {iso_timestamp_str}")
                return None  # 无法解析

        if dt_object:
            return dt_object.strftime("%Y-%m-%d %H:%M")
        else:
            # print(f"最终未能成功解析时间对象: {iso_timestamp_str}")
            return None

    except ValueError as e:
        # print(f"解析时间格式时发生错误 '{iso_timestamp_str}': {e}")
        return None
    except Exception as e:
        # print(f"处理时间戳 '{iso_timestamp_str}' 时发生未知错误: {e}")
        return None


def format_wb_timestamp():
    data = get_raw_data("raw_data/wb.json")
    if data is None:
        return
    for hotel in data:
        for post in hotel["posts"]:
            post["timestamp"] = format_iso_timestamp_to_custom(post["timestamp"])
            for reply in post["replies"]:
                reply["comment_time"] = format_iso_timestamp_to_custom(
                    reply["comment_time"]
                )

    write_to_json(data, "raw_data/wb.json")


def parse_timestamp(timestamp_str):
    """
    解析移动端xhs爬虫获取的时间戳
    """
    now = datetime.now()
    # 定义随机时间范围
    start_random_date = datetime(2024, 3, 1, 0, 0)
    end_random_date = datetime(2025, 2, 28, 0, 0)
    time_difference_seconds = int((end_random_date - start_random_date).total_seconds())

    def get_random_timestamp():
        random_seconds = random.randint(0, time_difference_seconds)
        random_date = start_random_date + timedelta(seconds=random_seconds)
        return random_date.strftime("%Y-%m-%d %H:%M")

    original_timestamp_str = timestamp_str
    timestamp_str = timestamp_str.strip()

    # 0. X minute(s) ago (可能带有地点)
    # Example: "50 minute(s) ago"
    # Try matching with optional location first
    match = re.fullmatch(
        r"(\d+)\s+minute(?:s)?\s+ago(?:\s+[A-Za-z\u4e00-\u9fa5]+)?",
        timestamp_str,
        re.IGNORECASE,
    )
    if not match:  # Then try matching without location if the first attempt failed
        match = re.fullmatch(
            r"(\d+)\s+minute(?:s)?\s+ago", timestamp_str, re.IGNORECASE
        )
    if match:
        minutes_ago = int(match.group(1))
        dt = now - timedelta(minutes=minutes_ago)
        return dt.strftime("%Y-%m-%d %H:%M")

    # 1. YYYY-MM-DD (可能带有地点)
    match = re.fullmatch(
        r"(\d{4})-(\d{2})-(\d{2})(?:\s+[A-Za-z\u4e00-\u9fa5]+)?", timestamp_str
    )
    if not match:
        core_date_match = re.match(r"(\d{4}-\d{2}-\d{2})", timestamp_str)
        if core_date_match:
            timestamp_str_cleaned = core_date_match.group(1)
            if re.fullmatch(
                r"\d{4}-\d{2}-\d{2}", timestamp_str_cleaned
            ):  # Ensure it's just the date
                try:
                    dt = datetime.strptime(timestamp_str_cleaned, "%Y-%m-%d")
                    return dt.strftime("%Y-%m-%d %H:%M")
                except ValueError:
                    pass
    elif match:  # if fullmatch with optional location worked
        try:
            dt = datetime.strptime(
                match.group(1) + "-" + match.group(2) + "-" + match.group(3), "%Y-%m-%d"
            )
            return dt.strftime("%Y-%m-%d %H:%M")
        except ValueError:
            pass

    # 2. MM-DD (current year, 可能带有地点)
    # Example: "04-30 Jiangsu"
    match = re.fullmatch(
        r"(\d{2})-(\d{2})(?:\s+[A-Za-z\u4e00-\u9fa5]+)?", timestamp_str
    )
    if not match:
        core_date_match = re.match(r"(\d{2}-\d{2})", timestamp_str)
        if core_date_match:
            timestamp_str_cleaned = core_date_match.group(1)
            if re.fullmatch(r"\d{2}-\d{2}", timestamp_str_cleaned):
                try:
                    dt = datetime.strptime(
                        f"{now.year}-{timestamp_str_cleaned}", "%Y-%m-%d"
                    )
                    return dt.strftime("%Y-%m-%d %H:%M")
                except ValueError:
                    pass
    elif match:
        try:
            dt = datetime.strptime(
                f"{now.year}-{match.group(1)}-{match.group(2)}", "%Y-%m-%d"
            )
            return dt.strftime("%Y-%m-%d %H:%M")
        except ValueError:
            pass

    # 3. X hour(s) ago (可能带有地点)
    match = re.fullmatch(
        r"(\d+)\s+hour(?:s)?\s+ago(?:\s+[A-Za-z\u4e00-\u9fa5]+)?",
        timestamp_str,
        re.IGNORECASE,
    )
    if not match:
        match = re.fullmatch(r"(\d+)\s+hour(?:s)?\s+ago", timestamp_str, re.IGNORECASE)
    if match:
        hours_ago = int(match.group(1))
        dt = now - timedelta(hours=hours_ago)
        return dt.strftime("%Y-%m-%d %H:%M")

    # 4. X day(s) ago (可能带有地点)
    # Example: "2 days ago Guangdong", "2 day(s) ago", "4 days ago Shanghai"
    # Regex updated to handle "day", "days", and "day(s)"
    match = re.fullmatch(
        r"(\d+)\s+day(?:s|\(s\))?\s+ago(?:\s+[A-Za-z\u4e00-\u9fa5]+)?",
        timestamp_str,
        re.IGNORECASE,
    )
    if not match:
        match = re.fullmatch(
            r"(\d+)\s+day(?:s|\(s\))?\s+ago", timestamp_str, re.IGNORECASE
        )
    if match:
        days_ago = int(match.group(1))
        dt = now - timedelta(days=days_ago)
        return dt.strftime("%Y-%m-%d %H:%M")

    # 5. Yesterday HH:MM (AM/PM) (可能带有地点)
    match = re.fullmatch(
        r"Yesterday\s+(\d{1,2}):(\d{2})(?:\s+(AM|PM))?(?:\s+[A-Za-z\u4e00-\u9fa5]+)?",
        timestamp_str,
        re.IGNORECASE,
    )
    if not match:
        match = re.fullmatch(
            r"Yesterday\s+(\d{1,2}):(\d{2})(?:\s+(AM|PM))?",
            timestamp_str,
            re.IGNORECASE,
        )
    if match:
        yesterday = now - timedelta(days=1)
        hour = int(match.group(1))
        minute = int(match.group(2))
        am_pm = match.group(3)
        if am_pm and am_pm.upper() == "PM" and hour < 12:
            hour += 12
        elif am_pm and am_pm.upper() == "AM" and hour == 12:
            hour = 0
        try:
            dt = yesterday.replace(hour=hour, minute=minute, second=0, microsecond=0)
            return dt.strftime("%Y-%m-%d %H:%M")
        except ValueError:
            pass

    month_map = {
        "Jan": 1,
        "Feb": 2,
        "Mar": 3,
        "Apr": 4,
        "May": 5,
        "Jun": 6,
        "Jul": 7,
        "Aug": 8,
        "Sep": 9,
        "Oct": 10,
        "Nov": 11,
        "Dec": 12,
    }

    # 6. Mon DD (current year, e.g., Apr 03) (可能带有地点)
    # 7. Edited on Mon DD (current year, e.g., Edited on Mar 19) (可能带有地点)
    # 8. Mon DDLocation (current year, e.g., Apr 26Hebei, Apr 17Jiangsu, Edited on Apr 25Fujian, Apr 23Liaoning, Apr 24Shanghai)
    # Regex to capture "Edited on", month, day, and allow location to be directly attached or spaced, and can be English/Chinese.
    # Also allows for extra text after the location part.
    pattern_mon_dd = re.compile(
        r"(?:Edited\s+on\s+)?([A-Za-z]{3})\s+(\d{1,2})(?:\s*([A-Za-z\u4e00-\u9fa5]+))?(?:\s+.*)?",
        re.IGNORECASE,
    )
    # We use re.match here because we only care about the beginning of the string matching the date pattern.
    match = pattern_mon_dd.match(
        timestamp_str.strip()
    )  # Changed from fullmatch to match
    if match:
        # Check if the matched part is the whole string or if there's only location/extra text after the core date.
        # This is a bit tricky. We want to ensure we are not just partially matching something unintended.
        # A simpler way is to extract what we need and ignore the rest if the core pattern matches.

        # Extract the parts that form the date and optional location
        core_date_text = match.group(0)  # The entire matched part by pattern_mon_dd
        # Attempt to re-match with a more restrictive pattern to ensure we got a valid date at the start
        strict_match = re.match(
            r"(?:Edited\s+on\s+)?([A-Za-z]{3})\s+(\d{1,2})(?:\s*([A-Za-z\u4e00-\u9fa5]+))?",
            core_date_text,
            re.IGNORECASE,
        )
        if strict_match:
            month_str, day_str, location_part = (
                strict_match.group(1),
                strict_match.group(2),
                strict_match.group(3),
            )
            month = month_map.get(month_str.capitalize())
            if month:
                try:
                    day = int(day_str)
                    parsed_date_current_year = datetime(now.year, month, day)
                    year_to_use = now.year
                    if parsed_date_current_year > now + timedelta(days=1):
                        year_to_use = now.year - 1
                    dt = datetime(year_to_use, month, day)
                    return dt.strftime("%Y-%m-%d %H:%M")
                except ValueError:
                    pass  # Will fall through to the next pattern or random date

    # 9. Mon/DD/YYYY (e.g., Aug/25/2024) (可能带有地点)
    # Example: "Edited on Aug/16/2024"
    # Also allows for extra text after the location part.
    pattern_mon_dd_yyyy = re.compile(
        r"(?:Edited\s+on\s+)?([A-Za-z]{3})/(\d{1,2})/(\d{4})(?:\s*([A-Za-z\u4e00-\u9fa5]+))?(?:\s+.*)?",
        re.IGNORECASE,
    )
    match = pattern_mon_dd_yyyy.match(
        timestamp_str.strip()
    )  # Changed from fullmatch to match
    if match:
        strict_match = re.match(
            r"(?:Edited\s+on\s+)?([A-Za-z]{3})/(\d{1,2})/(\d{4})(?:\s*([A-Za-z\u4e00-\u9fa5]+))?",
            match.group(0),
            re.IGNORECASE,
        )
        if strict_match:
            month_str, day_str, year_str, location_part = (
                strict_match.group(1),
                strict_match.group(2),
                strict_match.group(3),
                strict_match.group(4),
            )
            month = month_map.get(month_str.capitalize())
            if month:
                try:
                    day = int(day_str)
                    year = int(year_str)
                    dt = datetime(year, month, day)
                    return dt.strftime("%Y-%m-%d %H:%M")
                except ValueError:
                    pass  # Will fall through to the next pattern or random date

    # If no format matches
    print(f"无法解析时间格式: {original_timestamp_str}")
    # 可选配置：返回范围内随机时间或None
    # return get_random_timestamp()
    return None


def collect_huiting_content_by_keyword(data):
    """
    按关键词收集内容，以便于进行高频词汇提取
    """
    huiting_posts = []
    for hotel in data:
        if hotel["hotel"] == "惠庭":
            huiting_posts.extend(hotel["posts"])
            for post in hotel["posts"]:
                huiting_posts.extend(post["replies"])

    # 收集包含关键字的帖子内容
    keyword_posts = {}
    keywords = Keywords.get_keywords()
    for k in keywords:
        keyword_posts[k["primary_keyword"].strip()] = {}
        for secondary_keyword in k["secondary_keywords"]:
            keyword_posts[k["primary_keyword"]][
                secondary_keyword["keyword"].strip()
            ] = []

    keywords_map = {}
    for k in keywords:
        for sk in k["secondary_keywords"]:
            keywords_map[sk["keyword"].strip()] = k[
                "primary_keyword"
            ].strip()  # map secondary keyword to primary keyword

    for post in huiting_posts:
        content = post["content"]
        keywords_mentioned = post.get("keywords_mentioned", None)
        if keywords_mentioned:
            sks = keywords_mentioned.get("secondary_keyword", [])
            for sk in sks:
                keyword_posts[keywords_map[sk["keyword"].strip()]][
                    sk["keyword"].strip()
                ].append(content)

    return keyword_posts


def get_unanalyzed_posts(all_data, analyzed_data, platform):
    """
    从所有数据中获取未分析过的帖子
    """
    if platform == "wb":
        unique_item = "note_id"
    elif platform == "flyert":
        unique_item = "link"
    elif platform == "xhs":
        unique_item = "content"
    analyzed_note_ids = [
        post[unique_item] for hotel in analyzed_data for post in hotel["posts"]
    ]
    unanalyzed_posts = []
    for hotel in all_data:
        tmp = {
            "hotel": hotel["hotel"],
            "posts": [],
        }
        for post in hotel["posts"]:
            if post[unique_item] not in analyzed_note_ids:
                tmp["posts"].append(post)
        unanalyzed_posts.append(tmp)
    return unanalyzed_posts


def format_wb_data_from_media_crawler_by_hotel(posts, comments, hotel_name):
    existing_data = get_raw_data("raw_data/wb.json")

    if not existing_data:
        existing_data = []

    # 查找已存在的数据用于去重
    existing_note_ids = []
    for hotel in existing_data:
        if hotel["hotel"] == hotel_name:
            existing_note_ids = [post["note_id"] for post in hotel["posts"]]
            break

    # 新增数据内部去重
    # 使用字典来存储去重后的posts
    unique_posts = {}
    for post in posts:
        if (
            post["note_id"] not in unique_posts
            and post["note_id"] not in existing_note_ids
        ):
            unique_posts[post["note_id"]] = post

    # 使用字典来存储去重后的comments
    unique_comments = {}
    for comment in comments:
        if comment["comment_id"] not in unique_comments:
            unique_comments[comment["comment_id"]] = comment

    # 将去重后的数据转换回列表
    posts = list(unique_posts.values())
    comments = list(unique_comments.values())

    # 将posts和comments合并成flyert.json格式
    hotel_posts = {}  # 用于按酒店名分类存储帖子

    for post in posts:
        post_comments = []
        # 查找属于这个post的所有comments
        for comment in comments:
            if comment["note_id"] == post["note_id"]:
                # 格式化评论时间
                comment_time = datetime.fromtimestamp(
                    int(comment.get("create_time", 0))
                ).strftime("%Y-%m-%d %H:%M")
                post_comments.append(
                    {
                        "commenter_name": comment.get("nickname", ""),
                        "comment_content": comment.get("content", ""),
                        "commenter_link": comment.get("profile_url", ""),
                        "comment_time": comment_time,
                    }
                )

        # 格式化帖子时间
        post_time = datetime.fromtimestamp(int(post.get("create_time", 0))).strftime(
            "%Y-%m-%d %H:%M"
        )

        # 构建符合flyert.json格式的数据结构
        merged_post = {
            "note_id": post.get("note_id", ""),
            "content": post.get("content", ""),
            "timestamp": post_time,
            "link": post.get("note_url", ""),
            "replies": post_comments,
        }

        if hotel_name not in hotel_posts:
            hotel_posts[hotel_name] = {"hotel": hotel_name, "posts": []}
        hotel_posts[hotel_name]["posts"].append(merged_post)

    # 将字典转换为列表格式
    merged_data = list(hotel_posts.values())

    return merged_data


def format_keywords_for_all_analyzed_file(file_paths):
    count = 0
    record = {}
    for path in file_paths:
        data = get_raw_data(path)
        if not data:
            return
        for hotel_entry in data:
            for post in hotel_entry["posts"]:

                keywords_mentioned = post.get("keywords_mentioned", None)
                if keywords_mentioned and keywords_mentioned.get(
                    "primary_keyword", None
                ):
                    for keyword_dict in keywords_mentioned["primary_keyword"]:
                        keyword = keyword_dict["keyword"]
                        formatted_keyword = Keywords.format_keyword(keyword)
                        if not formatted_keyword:
                            print(keyword)
                            continue
                        if keyword != formatted_keyword:
                            keyword_dict["keyword"] = formatted_keyword
                            count += 1
                            if not record.get(keyword):
                                record[keyword] = formatted_keyword

                if keywords_mentioned and keywords_mentioned.get(
                    "secondary_keyword", None
                ):
                    for keyword_dict in keywords_mentioned["secondary_keyword"]:
                        keyword = keyword_dict["keyword"]
                        formatted_keyword = Keywords.format_keyword(keyword)
                        if not formatted_keyword:
                            print(keyword)
                            continue
                        if keyword != formatted_keyword:
                            keyword_dict["keyword"] = formatted_keyword
                            count += 1
                            if not record.get(keyword):
                                record[keyword] = formatted_keyword

                for reply_dict in post["replies"]:
                    keywords_mentioned = reply_dict.get("keywords_mentioned", None)
                    if keywords_mentioned and keywords_mentioned.get(
                        "primary_keyword", None
                    ):
                        for keyword_dict in keywords_mentioned["primary_keyword"]:
                            keyword = keyword_dict["keyword"]
                            formatted_keyword = Keywords.format_keyword(keyword)
                            if not formatted_keyword:
                                print(keyword)
                                continue
                            if keyword != formatted_keyword:
                                keyword_dict["keyword"] = formatted_keyword
                                count += 1
                                if not record.get(keyword):
                                    record[keyword] = formatted_keyword

                    if keywords_mentioned and keywords_mentioned.get(
                        "secondary_keyword", None
                    ):
                        for keyword_dict in keywords_mentioned["secondary_keyword"]:
                            keyword = keyword_dict["keyword"]
                            formatted_keyword = Keywords.format_keyword(keyword)
                            if not formatted_keyword:
                                print(keyword)
                                continue
                            if keyword != formatted_keyword:
                                keyword_dict["keyword"] = formatted_keyword
                                count += 1
                                if not record.get(keyword):
                                    record[keyword] = formatted_keyword

        write_to_json(data, path)
    print(f"共修正了{count}条关键词")
    pprint(record)


def get_huiting_content(get_replies=False):
    wb_data = get_raw_data("analysis_result/wb_analyzed.json")
    flyert_data = get_raw_data("analysis_result/flyert_analyzed.json")
    xhs_data = get_raw_data("analysis_result/xhs_analyzed.json")

    all_data = wb_data + flyert_data + xhs_data  # type: ignore

    content = []
    for hotel in all_data:
        if hotel["hotel"] == "惠庭":
            for post in hotel["posts"]:
                if (
                    post.get("is_hotel_related", False) == True
                    and post.get("is_ad", False) == False
                ):
                    content.append(post["content"])

                if get_replies:
                    for reply in post["replies"]:
                        if reply.get("is_hotel_related", False) == True:
                            content.append(reply["content"])
    print(f"共获取到 {len(content)} 条内容")

    return content


if __name__ == "__main__":
    pass
