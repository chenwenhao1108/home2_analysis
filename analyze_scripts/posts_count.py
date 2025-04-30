from concurrent.futures import ThreadPoolExecutor
from utils import *
from pprint import pprint
import pandas as pd

def generate_excel(data, platform):
    # 将数据转换为DataFrame
    df = pd.DataFrame.from_dict(data, orient='index')

    if platform == "wb":
        df = df[['total', '范围内帖子', '范围内评论', '有效数据数量', '最早时间', '最晚时间', '内容不完整帖子占比有效帖子']]
    elif platform == "xhs":
        df = df[['total', '范围内帖子', '范围内评论', '软文数量', '有效数据数量', '最早时间', '最晚时间']]
    else:
        df = df[['total', '范围内帖子', '范围内评论', '有效数据数量', '最早时间', '最晚时间']]

    # 使用ExcelWriter来处理多个sheet的写入
    try:
        with pd.ExcelWriter('analysis_result/酒店帖子统计.xlsx', mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
            df.to_excel(writer, sheet_name=f'{platform}帖子统计')
    except FileNotFoundError:
        # 如果文件不存在，创建新文件
        df.to_excel('analysis_result/数据量统计.xlsx', sheet_name=f'{platform}帖子统计')

    print("Excel文件已生成：analysis_result/数据量统计.xlsx")

def count_posts(platform):
    filter = PostsFilter()
    raw_data = get_raw_data(f'raw_data/{platform}.json')
    filtered_data = filter.filter_by_time(raw_data)
    
    if not raw_data:
        return None
    
    count = {}
    for hotel in raw_data:
        latest_time = 0
        earliest_time = float('inf')

        posts_count = len(hotel["posts"])
        
        for post in hotel["posts"]:
            timestamp = datetime.strptime(post["timestamp"], '%Y-%m-%d %H:%M').timestamp()
            if timestamp < earliest_time:
                earliest_time = timestamp
            if timestamp > latest_time:
                latest_time = timestamp
        tmp = {
            "total": posts_count,
            "最早时间": datetime.fromtimestamp(earliest_time).strftime('%Y-%m-%d %H:%M'),
            "最晚时间": datetime.fromtimestamp(latest_time).strftime('%Y-%m-%d %H:%M'),
        }
        count[hotel["hotel"]] = tmp
        
    # 读取analyzed数据，只需读取一次
    analyzed_data = None
    try:
        with open(f'analysis_result/{platform}_analyzed.json', 'r', encoding='utf-8') as f:
            analyzed_data = json.load(f)
    except FileNotFoundError:
        print(f"警告：无法找到 {platform}_analyzed.json 文件，将无法统计有效数据和软文数量。")
    except Exception as e:
        print(f"读取 {platform}_analyzed.json 文件时出错: {e}")

    # 将 analyzed_data 转换为字典以便快速查找
    analyzed_hotel_map = {h['hotel']: h for h in analyzed_data} if analyzed_data else {}

    for hotel in filtered_data:
        hotel_name = hotel["hotel"]
        replies_count = 0
        hotel_related_posts = 0
        hotel_related_replies = 0
        ad_count = 0 # 初始化 ad_count
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
             count[hotel_name]["软文数量"] = 0 # 如果没有 analyzed_hotel，软文数为0
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
                    if ap.get("link") == post.get("link"): # 假设 link 是唯一标识符
                        matching_analyzed_post = ap
                        break
                
                if matching_analyzed_post and matching_analyzed_post.get("is_hotel_related", False):
                    hotel_related_posts += 1
                    # 统计该帖子下的酒店相关评论
                    for reply in matching_analyzed_post.get("replies", []):
                        if reply.get("is_hotel_related", False):
                            hotel_related_replies += 1
                
        count[hotel_name]["范围内评论"] = replies_count
        # 有效数据数量统计逻辑不变
        count[hotel_name]["有效数据数量"] = hotel_related_posts + hotel_related_replies - ad_count
        
        # 内容不完整帖子占比统计逻辑不变
        if platform == "wb":
            if posts_count == 0:
                count[hotel_name]["内容不完整帖子占比有效帖子"] = "0%"
            else:
                count[hotel_name]["内容不完整帖子占比有效帖子"] = f"{round(incomplete_posts/posts_count * 100)}%"
        
    generate_excel(count, platform)
    print("共有{}个酒店".format(len(count)))
    print("酒店列表： ", list(count.keys()))

def merge_wb_posts():
    with open("raw_data/亚朵轻居微博posts.json", "r", encoding="utf-8") as f:
        posts = json.load(f)
    with open("raw_data/亚朵轻居微博comments.json", "r", encoding="utf-8") as f:
        comments = json.load(f)
    
    # 使用字典来存储去重后的posts
    unique_posts = {}
    for post in posts:
        if post['note_id'] not in unique_posts:
            unique_posts[post['note_id']] = post
    
    # 使用字典来存储去重后的comments
    unique_comments = {}
    for comment in comments:
        if comment['comment_id'] not in unique_comments:
            unique_comments[comment['comment_id']] = comment
    
    # 将去重后的数据转换回列表
    posts = list(unique_posts.values())
    comments = list(unique_comments.values())
    
    # 将posts和comments合并成flyert.json格式
    hotel_posts = {}  # 用于按酒店名分类存储帖子
    
    for post in posts:
        post_comments = []
        # 查找属于这个post的所有comments
        for comment in comments:
            if comment['note_id'] == post['note_id']:
                # 格式化评论时间
                comment_time = datetime.fromtimestamp(int(comment.get('create_time', 0))).strftime('%Y-%m-%d %H:%M')
                post_comments.append({
                    "commenter_name": comment.get('nickname', ''),
                    "comment_content": comment.get('content', ''),
                    "commenter_link": comment.get('profile_url', ''),
                    "comment_time": comment_time
                })
        
        # 格式化帖子时间
        post_time = datetime.fromtimestamp(int(post.get('create_time', 0))).strftime('%Y-%m-%d %H:%M')
        
        # 构建符合flyert.json格式的数据结构
        merged_post = {
            "content": post.get('content', ''),
            "timestamp": post_time,
            "link": post.get('note_url', ''),
            "replies": post_comments
        }
        
        # 根据source_keyword分类
        hotel_name = post.get('source_keyword', '未分类')
        if hotel_name not in hotel_posts:
            hotel_posts[hotel_name] = {
                "hotel": hotel_name,
                "posts": []
            }
        hotel_posts[hotel_name]["posts"].append(merged_post)
    
    # 将字典转换为列表格式
    merged_data = list(hotel_posts.values())
    
    with open("raw_data/wb.json", "r", encoding="utf-8") as f:
        wb_data = json.load(f)
    
    wb_data.extend(merged_data)
    
    # 写入wb.json文件
    with open("raw_data/wb.json", "w", encoding="utf-8") as f:
        json.dump(wb_data, f, ensure_ascii=False, indent=4)

def merge_xhs_posts(posts_path, comments_path, new_hotel_name):
    with open(posts_path, "r", encoding="utf-8") as f:
        posts = json.load(f)
    with open(comments_path, "r", encoding="utf-8") as f:
        comments = json.load(f)

    # 使用字典去重posts
    unique_posts = {}
    for post in posts:
        if post['note_id'] not in unique_posts:
            unique_posts[post['note_id']] = post

    # 使用字典去重comments
    unique_comments = {}
    for comment in comments:
        if comment['comment_id'] not in unique_comments:
            unique_comments[comment['comment_id']] = comment

    # 将评论按note_id分组
    comments_by_post = {}
    for comment in unique_comments.values():
        note_id = comment['note_id']
        if note_id not in comments_by_post:
            comments_by_post[note_id] = []
        # 格式化评论时间
        comment_time = datetime.fromtimestamp(int(comment.get('create_time', 0)) / 1000).strftime('%Y-%m-%d %H:%M')
        formatted_comment = {
            "commenter_name": comment.get('nickname', ''),
            "comment_content": comment.get('content', ''),
            "commenter_link": comment.get('profile_url', ''),
            "comment_time": comment_time
        }
        comments_by_post[note_id].append(formatted_comment)

    formatted_posts = []
    for post in posts:
        # 格式化帖子时间
        post_time = datetime.fromtimestamp(int(post.get('last_update_time', 0))/1000).strftime('%Y-%m-%d %H:%M')
        formatted_post = {
            "note_id": post['note_id'],
            "content": post.get('title', '') + post.get('desc', ''),
            "timestamp": post_time,
            "link": post.get('note_url', ''),
            "replies": comments_by_post.get(post['note_id'], [])
        }
        formatted_posts.append(formatted_post)
            
    with open("raw_data/xhs.json", "r", encoding="utf-8") as f:
        xhs_data = json.load(f)
    
    # 将现有的 xhs_data 转换为以 hotel 为键的字典
    hotel_dict = {}
    for item in xhs_data:
        hotel_name = item['hotel']
        if hotel_name not in hotel_dict:
            hotel_dict[hotel_name] = item
        else:
            # 如果已存在该酒店，将 posts 添加到现有列表中
            hotel_dict[hotel_name]['posts'].extend(item['posts'])
        
    if new_hotel_name in hotel_dict:
        print(f"酒店{new_hotel_name}已存在，将新帖子添加到现有数据中...")
        print(f"现有帖子数量：{len(hotel_dict[new_hotel_name]['posts'])}")
        existing_note_ids = [post['note_id'] for post in hotel_dict[hotel_name]['posts']]
        for post in formatted_posts:
            if post['note_id'] not in existing_note_ids:
                hotel_dict[new_hotel_name]['posts'].append(post)
        print(f"添加成功！新帖子数量：{len(hotel_dict[new_hotel_name]['posts'])}")
    else:
        # 如果是新酒店，直接添加
        hotel_dict[new_hotel_name] = {
            "hotel": new_hotel_name,
            "posts": formatted_posts
        }
        print(f"新酒店{hotel_name}添加成功！帖子数量：{len(formatted_posts)}")
    
    # 将字典转换回列表形式
    final_data = list(hotel_dict.values())
    
    # 写入文件
    with open("raw_data/xhs.json", "w", encoding="utf-8") as f:
        json.dump(final_data, f, ensure_ascii=False, indent=4)
   
   
def tmp_count(posts_path, comments_path, hotel_name):
    with open("raw_data/xhs.json", "r", encoding="utf-8") as f:
        old_data = json.load(f)
    with open(posts_path, "r", encoding="utf-8") as f:
        posts = json.load(f)
    with open(comments_path, "r", encoding="utf-8") as f:
        comments = json.load(f)
        
    for hotel in old_data:
        if hotel["hotel"] == hotel_name:
            existing_posts = hotel["posts"]
            break
    else:
        print(f"酒店{hotel_name}未找到！")
        return
        
    existing_note_ids = set()
    for post in existing_posts:
        if post['note_id'] not in existing_note_ids:
            existing_note_ids.add(post['note_id'])
            
    # 使用字典去重posts
    unique_posts = {}
    for post in posts:
        if post['note_id'] not in unique_posts:
            unique_posts[post['note_id']] = post

    # 使用字典去重comments
    unique_comments = {}
    for comment in comments:
        if comment['comment_id'] not in unique_comments:
            unique_comments[comment['comment_id']] = comment

    # 将评论按note_id分组
    comments_by_post = {}
    for comment in unique_comments.values():
        note_id = comment['note_id']
        if note_id not in comments_by_post:
            comments_by_post[note_id] = []
        # 格式化评论时间
        comment_time = datetime.fromtimestamp(int(comment.get('create_time', 0)) / 1000).strftime('%Y-%m-%d %H:%M')
        formatted_comment = {
            "commenter_name": comment.get('nickname', ''),
            "comment_content": comment.get('content', ''),
            "commenter_link": comment.get('profile_url', ''),
            "comment_time": comment_time
        }
        comments_by_post[note_id].append(formatted_comment)
            
    formatted_posts = []
    for post in posts:
        if post['note_id'] in existing_note_ids:
            continue
        post_time = datetime.fromtimestamp(int(post.get('last_update_time', 0))/1000).strftime('%Y-%m-%d %H:%M')
        formatted_post = {
            "note_id": post['note_id'],
            "content": post.get('title', '') + post.get('desc', ''),
            "timestamp": post_time,
            "link": post.get('note_url', ''),
            "replies": comments_by_post.get(post['note_id'], [])
        }
        formatted_posts.append(formatted_post)
    tmp = [{
        "hotel": hotel_name,
        "posts": formatted_posts
    }]
    with open("raw_data/xhs_test.json", "w", encoding="utf-8") as f:
        json.dump(tmp, f, ensure_ascii=False, indent=4)


if __name__ == "__main__":
    count_posts("xhs")
    # count_posts("wb")
    # count_posts("flyert")
    # merge_wb_posts()
    # merge_xhs_posts('raw_data/xhs/凯悦嘉寓_contents_2025-04-30.json', 'raw_data/xhs/凯悦嘉寓_comments_2025-04-30.json', '凯悦嘉寓')
    # is_time_descending()
    # tmp_count("raw_data/xhs/城际_contents_2025-04-30.json", "raw_data/xhs/城际_comments_2025-04-30.json", "城际")