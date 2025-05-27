from itertools import count, islice
import keyword

from utils import *
from prompt import *
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime


def analyzer(system_prompt, user_prompt):
    openai_service = OpenAIService()
    try:
        analysis = openai_service.infer(
            user_prompt=user_prompt,
            system_prompt=system_prompt,
        )
    except Exception as e:
        print(f"Error analyzing content: {e}")
        return None
    if analysis:
        return analysis
    else:
        return None


def analyze_is_hotel_related(raw_data, max_workers=200):
    start_time = datetime.now()

    # 按平台读取所有酒店
    filter = PostsFilter()

    if not raw_data:
        print("Raw data is empty, please check your input")
        return

    filtered_data = filter.filter_by_time(raw_data)
    simplified_data = filter.simplify_data(filtered_data)

    # 计算总帖子数和回复数
    total_posts_to_analyze = 0
    total_replies_to_analyze = 0
    for hotel in simplified_data:
        total_posts_to_analyze += len(hotel["posts"])
        for post in hotel["posts"]:
            # 只有在帖子分析后才确定哪些回复需要分析，但为了进度条，先计算所有回复
            # 稍后在提交任务时会跳过内容过短的回复
            total_replies_to_analyze += len(post["replies"])

    print(
        f"共发现 {total_posts_to_analyze} 个帖子和 {total_replies_to_analyze} 个回复需要分析"
    )

    analyzed_posts_count = 0
    analyzed_replies_count = 0
    tasks_submitted = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures_map = {}

        # 提交帖子分析任务
        for hotel_index, hotel in enumerate(simplified_data):
            for post_index, post in enumerate(hotel["posts"]):
                content = post.get("title", "") + "\n" + post["content"]
                future = executor.submit(
                    analyzer,
                    is_hotel_related_system_prompt,
                    is_hotel_related_user_prompt.format(post_content=content),
                )
                futures_map[future] = {
                    "type": "post",
                    "location": (hotel_index, post_index),
                }
                tasks_submitted += 1

        # 处理帖子分析结果，并根据结果提交回复分析任务
        post_futures = {
            f: info for f, info in futures_map.items() if info["type"] == "post"
        }
        for future in as_completed(post_futures):
            try:
                partial_res = future.result()
                analyzed_posts_count += 1
                task_info = futures_map[future]
                hotel_index, post_index = task_info["location"]

                is_related = False
                reason = "分析失败或无结果"
                if partial_res:
                    is_related = partial_res.get("is_hotel_related", False)
                    is_hotel_related_reason = partial_res.get(
                        "is_hotel_related_reason", "无原因"
                    )
                    is_ad = partial_res.get("is_ad", False)
                    is_ad_reason = partial_res.get("is_ad_reason", "无原因")

                # 更新帖子的分析结果
                simplified_data[hotel_index]["posts"][post_index][
                    "is_hotel_related"
                ] = is_related
                simplified_data[hotel_index]["posts"][post_index][
                    "is_hotel_related_reason"
                ] = is_hotel_related_reason
                simplified_data[hotel_index]["posts"][post_index]["is_ad"] = is_ad
                simplified_data[hotel_index]["posts"][post_index][
                    "is_ad_reason"
                ] = is_ad_reason

                # 如果帖子相关，则提交其回复的分析任务
                if is_related:
                    post = simplified_data[hotel_index]["posts"][post_index]
                    for reply_index, reply in enumerate(post["replies"]):
                        # 对于内容长度小于10的评论，直接标记为False，不提交分析
                        if len(reply["content"]) < 10:
                            simplified_data[hotel_index]["posts"][post_index][
                                "replies"
                            ][reply_index]["is_hotel_related"] = False
                            simplified_data[hotel_index]["posts"][post_index][
                                "replies"
                            ][reply_index]["is_hotel_related_reason"] = "评论内容过短"
                            # 从总回复数中减去，因为它不被分析
                            total_replies_to_analyze -= 1
                            continue

                        reply_future = executor.submit(
                            analyzer,
                            is_hotel_related_system_prompt,
                            is_hotel_related_user_prompt.format(
                                post_content=reply["content"]
                            ),
                        )
                        futures_map[reply_future] = {
                            "type": "reply",
                            "location": (hotel_index, post_index, reply_index),
                        }
                        tasks_submitted += 1
                else:
                    # 如果帖子不相关，其所有回复也不相关，从总回复数中减去
                    total_replies_to_analyze -= len(
                        simplified_data[hotel_index]["posts"][post_index]["replies"]
                    )
                    # 标记所有回复为不相关
                    for reply_index, reply in enumerate(
                        simplified_data[hotel_index]["posts"][post_index]["replies"]
                    ):
                        simplified_data[hotel_index]["posts"][post_index]["replies"][
                            reply_index
                        ]["is_hotel_related"] = False
                        simplified_data[hotel_index]["posts"][post_index]["replies"][
                            reply_index
                        ]["is_hotel_related_reason"] = "所属帖子与酒店无关"

                # 打印帖子分析进度
                post_progress = (
                    (analyzed_posts_count / total_posts_to_analyze) * 100
                    if total_posts_to_analyze > 0
                    else 0
                )
                print(
                    f"\r分析帖子进度: {post_progress:.2f}% ({analyzed_posts_count}/{total_posts_to_analyze})",
                    end="",
                )

            except Exception as exc:
                print(f"\n处理帖子结果时发生错误: {exc}")
                # 标记帖子分析失败
                task_info = futures_map[future]
                hotel_index, post_index = task_info["location"]
                simplified_data[hotel_index]["posts"][post_index][
                    "is_hotel_related"
                ] = False
                simplified_data[hotel_index]["posts"][post_index][
                    "is_hotel_related_reason"
                ] = f"处理错误: {exc}"
                # 同样，其回复也不再分析
                total_replies_to_analyze -= len(
                    simplified_data[hotel_index]["posts"][post_index]["replies"]
                )
                continue
        print("\n帖子分析完成，开始处理回复...")

        # 处理回复分析结果
        reply_futures = {
            f: info for f, info in futures_map.items() if info["type"] == "reply"
        }
        for future in as_completed(reply_futures):
            try:
                partial_res = future.result()
                analyzed_replies_count += 1
                task_info = futures_map[future]
                hotel_index, post_index, reply_index = task_info["location"]

                is_related = False
                reason = "分析失败或无结果"
                if partial_res:
                    is_related = partial_res.get("is_hotel_related", False)
                    reason = partial_res.get("is_hotel_related_reason", "无原因")

                # 更新回复的分析结果
                simplified_data[hotel_index]["posts"][post_index]["replies"][
                    reply_index
                ]["is_hotel_related"] = is_related
                simplified_data[hotel_index]["posts"][post_index]["replies"][
                    reply_index
                ]["is_hotel_related_reason"] = reason

                # 打印回复分析进度
                # 确保 total_replies_to_analyze 不为零
                if total_replies_to_analyze > 0:
                    reply_progress = (
                        analyzed_replies_count / total_replies_to_analyze
                    ) * 100
                    print(
                        f"\r分析回复进度: {reply_progress:.2f}% ({analyzed_replies_count}/{total_replies_to_analyze})",
                        end="",
                    )
                else:
                    print(
                        f"\r分析回复进度: 无需分析的回复 ({analyzed_replies_count}/0)",
                        end="",
                    )

            except Exception as exc:
                print(f"\n处理回复结果时发生错误: {exc}")
                # 标记回复分析失败
                task_info = futures_map[future]
                hotel_index, post_index, reply_index = task_info["location"]
                simplified_data[hotel_index]["posts"][post_index]["replies"][
                    reply_index
                ]["is_hotel_related"] = False
                simplified_data[hotel_index]["posts"][post_index]["replies"][
                    reply_index
                ]["is_hotel_related_reason"] = f"处理错误: {exc}"
                continue
        print("\n回复分析完成!")

    # 统计分析结果
    final_hotel_related_posts = sum(
        1
        for hotel in simplified_data
        for post in hotel["posts"]
        if post.get("is_hotel_related")
    )
    final_is_ad_posts = sum(
        1 for hotel in simplified_data for post in hotel["posts"] if post.get("is_ad")
    )
    final_hotel_related_replies = sum(
        1
        for hotel in simplified_data
        for post in hotel["posts"]
        for reply in post["replies"]
        if reply.get("is_hotel_related")
    )

    # 计算总耗时
    end_time = datetime.now()
    duration = end_time - start_time

    print(f"\n分析完成! 统计结果:")
    print(f"总帖子数: {total_posts_to_analyze}")
    print(f"- 与酒店相关帖子数: {final_hotel_related_posts}")
    print(f"- 广告帖子数: {final_is_ad_posts}")
    # 使用修正后的总回复数
    print(f"总回复数 (实际分析): {total_replies_to_analyze}")
    print(f"- 与酒店相关回复数: {final_hotel_related_replies}")
    print(f"总耗时: {duration}")

    return simplified_data


def analyze_keywords(analyzed_data, max_workers=500):
    start_time = datetime.now()
    total_posts = 0
    total_replies = 0
    analyzed_posts = 0
    analyzed_replies = 0

    # 计算需要分析的帖子和评论数量
    for hotel in analyzed_data:
        for post in hotel["posts"]:
            if post.get("is_hotel_related"):
                total_posts += 1
                total_replies += sum(
                    1 for reply in post["replies"] if reply.get("is_hotel_related")
                )

    print(f"找到 {total_posts} 个相关帖子和 {total_replies} 个相关回复")

    # 合并分析帖子和评论
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures_map = {}

        for hotel_index, hotel in enumerate(analyzed_data):
            hotel_name = hotel["hotel"]
            keywords = Keywords.get_keywords_with_description()

            for post_index, post in enumerate(hotel["posts"]):
                if post.get("is_hotel_related"):
                    post_content = post.get("title", "") + "\n" + post["content"]
                    # 提交帖子分析任务
                    future_post = executor.submit(
                        analyzer,
                        analyze_post_system_prompt.format(
                            keywords=keywords, hotel=hotel_name
                        ),
                        analyze_post_user_prompt.format(post_content=post_content),
                    )
                    futures_map[future_post] = {
                        "type": "post",
                        "location": (hotel_index, post_index),
                    }

                    # 提交评论分析任务
                    for reply_index, reply in enumerate(post["replies"]):
                        if reply.get("is_hotel_related"):
                            future_reply = executor.submit(
                                analyzer,
                                analyze_reply_system_prompt.format(
                                    keywords=keywords, hotel=hotel_name
                                ),
                                analyze_reply_user_prompt.format(
                                    reply_content=reply["content"],
                                    post_content=post_content,
                                ),
                            )
                            futures_map[future_reply] = {
                                "type": "reply",
                                "location": (hotel_index, post_index, reply_index),
                            }

        # 处理结果
        for future in as_completed(futures_map):
            try:
                partial_res = future.result()
                task_info = futures_map[future]
                task_type = task_info["type"]
                location = task_info["location"]

                if partial_res:
                    if task_type == "post":
                        hotel_index, post_index = location
                        filtered_keywords = Keywords.filter_mentioned_keywords(
                            partial_res.get("keywords_mentioned", {})
                        )

                        analyzed_data[hotel_index]["posts"][post_index][
                            "keywords_mentioned"
                        ] = filtered_keywords
                        analyzed_posts += 1
                    elif task_type == "reply":
                        hotel_index, post_index, reply_index = location
                        filtered_keywords = Keywords.filter_mentioned_keywords(
                            partial_res.get("keywords_mentioned", {})
                        )
                        analyzed_data[hotel_index]["posts"][post_index]["replies"][
                            reply_index
                        ]["keywords_mentioned"] = filtered_keywords
                        analyzed_replies += 1

                # 更新进度显示
                post_progress = (
                    (analyzed_posts / total_posts) * 100 if total_posts > 0 else 0
                )
                reply_progress = (
                    (analyzed_replies / total_replies) * 100 if total_replies > 0 else 0
                )
                print(
                    f"\r分析进度: 帖子 {post_progress:.2f}% ({analyzed_posts}/{total_posts}) | 回复 {reply_progress:.2f}% ({analyzed_replies}/{total_replies})",
                    end="",
                    flush=True,
                )

            except Exception as exc:
                print(f"\n处理结果时发生错误: {exc}")
                continue
        print("\n分析完成!")

    # 计算总耗时
    end_time = datetime.now()
    duration = end_time - start_time

    print(f"\n分析完成! 统计结果:")
    print(f"分析的帖子数: {total_posts}")
    print(f"分析的回复数: {total_replies}")
    print(f"总耗时: {duration}")

    return analyzed_data


def extract_frequent_mentioned_words(keyword_content_map, max_workers=50):
    """
    从每个二级关键词对应的内容列表中提取前N条内容，合并后提取高频词汇。
    将提取的高频词汇列表替换掉原来的内容列表。

    :param keyword_content_map: 结构为 {primary_keyword: {secondary_keyword: [content1, content2, ...]}}
    :param max_workers: ThreadPoolExecutor的最大工作线程数
    :return: 更新后的字典，结构为 {primary_keyword: {secondary_keyword: [{"word": "w1", "sentiment": "s1"}, ...]}}
    """

    updated_keyword_map = {}
    tasks = []
    # 用于在回调中定位原始数据位置的辅助信息
    task_info_map = {}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for p_keyword, s_keywords_map in keyword_content_map.items():
            if p_keyword not in updated_keyword_map:
                updated_keyword_map[p_keyword] = {}
            for s_keyword, contents in s_keywords_map.items():
                top_contents = contents[:10]
                if not top_contents:
                    updated_keyword_map[p_keyword][
                        s_keyword
                    ] = []  # 如果没有内容，则设置为空列表
                    continue

                # 合并内容为一个字符串
                combined_content = "\n".join(top_contents)

                future = executor.submit(
                    analyzer,
                    extract_frequent_words_system_prompt,
                    extract_frequent_words_user_prompt.format(
                        text_content=combined_content,
                        primary_keyword=p_keyword,
                        secondary_keyword=s_keyword,
                    ),
                )
                tasks.append(future)
                task_info_map[future] = (p_keyword, s_keyword)

        processed_count = 0
        total_tasks = len(tasks)
        print(f"开始提取高频词汇，总任务数: {total_tasks}")

        for future in as_completed(tasks):
            p_keyword, s_keyword = task_info_map[future]
            try:
                frequent_words_list = future.result()
                updated_keyword_map[p_keyword][s_keyword] = frequent_words_list
            except Exception as exc:
                print(f"Error processing {p_keyword} -> {s_keyword}: {exc}")
                updated_keyword_map[p_keyword][s_keyword] = []  # 出错时也设置为空列表
            finally:
                processed_count += 1
                progress = (
                    (processed_count / total_tasks) * 100 if total_tasks > 0 else 0
                )
                print(
                    f"\r提取高频词汇进度: {progress:.2f}% ({processed_count}/{total_tasks})",
                    end="",
                )

    print("\n高频词汇提取完成!")
    return updated_keyword_map


def extract_typical_reviews_by_primary_keyword(keyword_content_map, max_workers=200):
    """
    为每个一级关键词提取典型的正面和负面评价案例。

    :param keyword_content_map: 结构为 {primary_keyword: {secondary_keyword: [content1, ...]}}
    :param max_workers: ThreadPoolExecutor的最大工作线程数
    :return: 字典，键为一级关键词，值为包含典型评价的JSON对象
             例如: {primary_keyword1: {"typical_positive_reviews": [...], "typical_negative_reviews": [...]}}
    """

    def get_typical_reviews_for_primary_keyword(p_keyword, all_contents_for_p_keyword):
        openai_service = OpenAIService()
        combined_content = "\n".join(all_contents_for_p_keyword)
        if not combined_content.strip():
            return {
                "typical_positive_reviews": [],
                "typical_negative_reviews": [],
            }  # 如果内容为空，返回空结果

        try:
            analysis = openai_service.infer(
                user_prompt=extract_typical_reviews_user_prompt.format(
                    primary_keyword=p_keyword, text_content=combined_content
                ),
                system_prompt=extract_typical_reviews_system_prompt,
            )
        except Exception as e:
            print(
                f"Error analyzing typical reviews for primary keyword '{p_keyword}': {e}"
            )
            return {
                "typical_positive_reviews": [],
                "typical_negative_reviews": [],
            }  # 分析失败返回空结果

        if (
            analysis
            and isinstance(analysis, dict)
            and "typical_positive_reviews" in analysis
            and "typical_negative_reviews" in analysis
        ):
            return analysis
        else:
            print(
                f"Warning: Analysis for primary keyword '{p_keyword}' did not return the expected dict format. Got: {analysis}"
            )
            return {"typical_positive_reviews": [], "typical_negative_reviews": []}

    typical_reviews_result = {}
    tasks = []
    task_info_map = {}

    # 准备任务：按一级关键词聚合所有内容
    primary_keyword_all_contents = {}
    for p_keyword, s_keywords_map in keyword_content_map.items():
        if p_keyword not in primary_keyword_all_contents:
            primary_keyword_all_contents[p_keyword] = []
        for s_keyword, contents in s_keywords_map.items():
            if isinstance(contents, list):
                primary_keyword_all_contents[p_keyword].extend(contents)
            else:
                print(
                    f"Warning: Contents for {p_keyword} -> {s_keyword} is not a list, skipping."
                )

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for p_keyword, all_contents in primary_keyword_all_contents.items():
            if not all_contents:
                typical_reviews_result[p_keyword] = {
                    "typical_positive_reviews": [],
                    "typical_negative_reviews": [],
                }
                continue

            # 限制传递给模型的内容长度，避免过长，例如取前 N 条或总字符数限制
            # 这里简单地取前 50 条内容，根据需要调整策略
            contents_to_analyze = all_contents[:50]

            future = executor.submit(
                get_typical_reviews_for_primary_keyword, p_keyword, contents_to_analyze
            )
            tasks.append(future)
            task_info_map[future] = p_keyword

        processed_count = 0
        total_tasks = len(tasks)
        print(f"开始提取典型评价案例，总任务数: {total_tasks}")

        for future in as_completed(tasks):
            p_keyword = task_info_map[future]
            try:
                reviews_summary = future.result()
                typical_reviews_result[p_keyword] = reviews_summary
            except Exception as exc:
                print(f"Error processing typical reviews for {p_keyword}: {exc}")
                typical_reviews_result[p_keyword] = {
                    "typical_positive_reviews": [],
                    "typical_negative_reviews": [],
                }
            finally:
                processed_count += 1
                progress = (
                    (processed_count / total_tasks) * 100 if total_tasks > 0 else 0
                )
                print(
                    f"\r提取典型评价案例进度: {progress:.2f}% ({processed_count}/{total_tasks})",
                    end="",
                )

    print("\n典型评价案例提取完成!")
    return typical_reviews_result


def extract_user_focus(data, max_workers=20):
    """
    从分析结果中提取用户关注的关键词
    """

    def chunk_list(lst, n):
        it = iter(lst)
        size = (len(lst) + n - 1) // n  # 向上取整
        return list(next(islice(it, size), []) for _ in range(n))

    chunks = [
        f"**帖子{i+1}:**\n".join(chunk) for i, chunk in enumerate(chunk_list(data, 20))
    ]

    user_focus_list = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures_res = {
            executor.submit(
                analyzer,
                extract_user_focus_system_prompt,
                extract_user_focus_user_prompt.format(content_chunk=chunk),
            ): chunk
            for chunk in chunks
        }

        for future in as_completed(futures_res):
            try:
                partial_res = future.result()
                if partial_res:
                    user_focus_list.extend(partial_res)
            except Exception as exc:
                print(f"Res {futures_res[future]} generated an exception: {exc}")
                continue

    merged_user_focus_list = analyzer(
        merge_user_focus_system_prompt,
        merge_user_focus_user_prompt.format(
            user_focus_keywords=", ".join(user_focus_list)
        ),
    )
    write_to_json(merged_user_focus_list, "analysis_result/user_focused_keywords.json")
    return merged_user_focus_list


def distribute_content_to_user_focus(contents):
    """
    根据用户关注的关键词将内容分配到对应的关键词下并进行统计
    """
    user_focus_keywords = get_raw_data("analysis_result/user_focused_keywords.json")
    if not user_focus_keywords:
        return {}

    result = {keyword: {"count": 0, "contents": []} for keyword in user_focus_keywords}
    with ThreadPoolExecutor(max_workers=200) as executor:
        futures_map = {
            executor.submit(
                analyzer,
                distribute_user_focus_system_prompt.format(
                    user_focus_keywords=user_focus_keywords
                ),
                distribute_user_focus_user_prompt.format(content=content),
            ): content
            for content in contents
        }

        for future in as_completed(futures_map):
            try:
                partial_res = future.result()
                if partial_res:
                    for keyword in partial_res:
                        if keyword not in result:
                            continue
                        result[keyword]["count"] += 1
                        result[keyword]["contents"].append(futures_map[future])
            except Exception as exc:
                print(f"Res {futures_map[future]} generated an exception: {exc}")
                continue

    return result


def summurize_user_focus():
    user_focus_keywords_count = get_raw_data(
        "analysis_result/user_focus_keywords_count.json"
    )
    if not user_focus_keywords_count:
        return
    for item in user_focus_keywords_count.values():
        if "summary" in item:
            del item["summary"]

    with ThreadPoolExecutor(max_workers=20) as executor:
        future_res = {
            executor.submit(
                analyzer,
                summarize_user_focus_system_prompt.format(keyword=keyword),
                summarize_user_focus_user_prompt.format(
                    content=f"帖子内容：\n".join(keyword_dict["contents"])
                ),
            ): keyword
            for keyword, keyword_dict in user_focus_keywords_count.items()
        }

        for future in as_completed(future_res):
            keyword = future_res[future]
            res = future.result()
            if res:
                advantage = res["advantage"]
                disadvantage = res["disadvantage"]
            user_focus_keywords_count[keyword]["advantage"] = advantage
            user_focus_keywords_count[keyword]["disadvantage"] = disadvantage

    write_to_json(
        user_focus_keywords_count, "analysis_result/user_focus_keywords_count.json"
    )


def main():
    pass

    # analyze more xhs raw data from mobile crawler
    hotels = [
        # "城际",
        "惠庭",
        # "桔子水晶",
        # "凯悦嘉轩",
        # "凯悦嘉寓",
        # "丽枫",
        # "美居",
        # "诺富特",
        # "途家盛捷",
        # "万枫",
        # "维也纳国际",
        # "馨乐庭",
        # "亚朵",
        # "亚朵轻居",
        # "源宿",
        # "智选假日",
    ]
    paths = [f"raw_data/xhs/5-19/filtered/xhs_{hotel}_all.json" for hotel in hotels]
    formatted_data = format_all_xhs_data_from_mobile(paths, hotels)
    first_analyzed_data = analyze_is_hotel_related(formatted_data)
    analyzed_data = analyze_keywords(first_analyzed_data)
    merge_data(formatted_data, "raw_data/xhs.json")
    merge_data(analyzed_data, "analysis_result/xhs_analyzed.json")
    print("XHS 的数据分析完毕")


if __name__ == "__main__":
    main()
