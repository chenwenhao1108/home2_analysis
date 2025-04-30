from datetime import datetime
import json
import os
import re

from openai import OpenAI


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
        model: str = "gpt-4.1",
        temperature: float = 0.8,
        retries: int = 3,
    ):
        """Make an inference using OpenAI API."""
        for attempt in range(retries):
            try:
                completion = self.client.chat.completions.create(
                    model=model,
                    messages=[
                        (
                            {"role": "system", "content": system_prompt}
                        ),
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
                    # "note_id": post["note_id"], # 移除原来的直接赋值
                    "content": post["content"],
                    "timestamp": post["timestamp"],
                    "link": post["link"],
                    "replies": [{
                        "content": reply["comment_content"],
                        "timestamp": reply["comment_time"]
                    } for reply in post["replies"]]
                }
                # 检查 'note_id' 是否存在于 post 中，如果存在则添加
                if "note_id" in post:
                    simplified_post["note_id"] = post["note_id"]
                simplified_posts.append(simplified_post)

            simplified_hotel = {
                "hotel": hotel["hotel"],
                "posts": simplified_posts # 使用修改后的 simplified_posts
            }
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
    def get_exclusive_keywords_str():
        keywords = Keywords.get_keywords()
        formatted_keywords = {
            "primary_keyword": [],
            "secondary_keyword": [],
        }
        for keyword in keywords:
            formatted_keywords["primary_keyword"].append(keyword["primary_keyword"])
            for sec_keyword in keyword["secondary_keywords"]:
                if not sec_keyword['is_exclusive']:
                    formatted_keywords["secondary_keyword"].append(sec_keyword["keyword"])
        return json.dumps(formatted_keywords, ensure_ascii=False, indent=2)
    
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
    def get_valid_keywords():
        """从 keywords.json 数据中提取所有有效关键词到一个集合中"""
        keywords_data = Keywords.get_keywords()
        valid_keywords = set()
        if not keywords_data:
            return valid_keywords

        for item in keywords_data:
            if 'primary_keyword' in item:
                # 移除可能的空格以便匹配
                valid_keywords.add(item['primary_keyword'].replace(' ', '')) 
            if 'secondary_keywords' in item:
                for sec_kw in item['secondary_keywords']:
                    if 'keyword' in sec_kw:
                        # 移除可能的空格以便匹配
                        valid_keywords.add(sec_kw['keyword'].replace(' ', '')) 
        return valid_keywords

    @staticmethod
    def filter_mentioned_keywords(mentioned_data):
        """过滤单个 'keywords_mentioned' 对象，移除无效关键词"""
        if not isinstance(mentioned_data, dict):
            # 如果输入不是预期的字典格式，返回空字典或进行错误处理
            print(f"警告：预期的 mentioned_data 是字典，但收到了 {type(mentioned_data)}")
            return {}

        valid_keywords = Keywords.get_valid_keywords()
        filtered_data = {}

        # 过滤 primary_keyword
        if 'primary_keyword' in mentioned_data and isinstance(mentioned_data['primary_keyword'], list):
            filtered_primary = [
                kw for kw in mentioned_data['primary_keyword']
                if isinstance(kw, dict) and kw.get('keyword') and kw['keyword'].replace(' ', '') in valid_keywords
            ]
            if filtered_primary: # 只有列表不为空时才添加
                 filtered_data['primary_keyword'] = filtered_primary

        # 过滤 secondary_keyword
        if 'secondary_keyword' in mentioned_data and isinstance(mentioned_data['secondary_keyword'], list):
            filtered_secondary = [
                kw for kw in mentioned_data['secondary_keyword']
                if isinstance(kw, dict) and kw.get('keyword') and kw['keyword'].replace(' ', '') in valid_keywords
            ]
            if filtered_secondary: # 只有列表不为空时才添加
                filtered_data['secondary_keyword'] = filtered_secondary
        
        # 如果过滤后 primary 和 secondary 都为空，则返回空字典
        if not filtered_data.get('primary_keyword') and not filtered_data.get('secondary_keyword'):
            return {}

        return filtered_data


def get_raw_data(path):
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    else:
        print("path不存在")
        return None

