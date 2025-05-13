import json
from utils import *
from prompt import *
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

def analyze_is_hotel_related(raw_data, max_workers=200):
    def get_is_hotel_related(content):
        openai_service = OpenAIService()
        try:
            analysis = openai_service.infer(
                user_prompt=is_hotel_related_user_prompt.format(post_content=content),
                system_prompt=is_hotel_related_system_prompt,
            )
        except Exception as e:
            print(f"Error analyzing content starting with {content[:20]}...: {e}")
            return None
        if analysis:
            return analysis
        else:
            return None

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
        total_posts_to_analyze += len(hotel['posts'])
        for post in hotel['posts']:
            # 只有在帖子分析后才确定哪些回复需要分析，但为了进度条，先计算所有回复
            # 稍后在提交任务时会跳过内容过短的回复
            total_replies_to_analyze += len(post['replies'])
    
    print(f"共发现 {total_posts_to_analyze} 个帖子和 {total_replies_to_analyze} 个回复需要分析")
    
    analyzed_posts_count = 0
    analyzed_replies_count = 0
    tasks_submitted = 0
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures_map = {}
        
        # 提交帖子分析任务
        for hotel_index, hotel in enumerate(simplified_data):
            for post_index, post in enumerate(hotel['posts']):
                content = post.get('title', '') + '\n' + post['content']
                future = executor.submit(get_is_hotel_related, content)
                futures_map[future] = {'type': 'post', 'location': (hotel_index, post_index)}
                tasks_submitted += 1
        
        # 处理帖子分析结果，并根据结果提交回复分析任务
        post_futures = {f: info for f, info in futures_map.items() if info['type'] == 'post'}
        for future in as_completed(post_futures):
            try:
                partial_res = future.result()
                analyzed_posts_count += 1
                task_info = futures_map[future]
                hotel_index, post_index = task_info['location']
                
                is_related = False
                reason = "分析失败或无结果"
                if partial_res:
                    is_related = partial_res.get('is_hotel_related', False)
                    is_hotel_related_reason = partial_res.get('is_hotel_related_reason', '无原因')
                    is_ad = partial_res.get('is_ad', False)
                    is_ad_reason = partial_res.get('is_ad_reason', '无原因')
                    
                # 更新帖子的分析结果
                simplified_data[hotel_index]['posts'][post_index]['is_hotel_related'] = is_related
                simplified_data[hotel_index]['posts'][post_index]['is_hotel_related_reason'] = is_hotel_related_reason
                simplified_data[hotel_index]['posts'][post_index]['is_ad'] = is_ad
                simplified_data[hotel_index]['posts'][post_index]['is_ad_reason'] = is_ad_reason
                
                # 如果帖子相关，则提交其回复的分析任务
                if is_related:
                    post = simplified_data[hotel_index]['posts'][post_index]
                    for reply_index, reply in enumerate(post['replies']):
                        # 对于内容长度小于10的评论，直接标记为False，不提交分析
                        if len(reply['content']) < 10:
                            simplified_data[hotel_index]['posts'][post_index]['replies'][reply_index]['is_hotel_related'] = False
                            simplified_data[hotel_index]['posts'][post_index]['replies'][reply_index]['is_hotel_related_reason'] = "评论内容过短"
                            # 从总回复数中减去，因为它不被分析
                            total_replies_to_analyze -= 1 
                            continue
                            
                        reply_future = executor.submit(get_is_hotel_related, reply['content'])
                        futures_map[reply_future] = {'type': 'reply', 'location': (hotel_index, post_index, reply_index)}
                        tasks_submitted += 1
                else:
                        # 如果帖子不相关，其所有回复也不相关，从总回复数中减去
                        total_replies_to_analyze -= len(simplified_data[hotel_index]['posts'][post_index]['replies'])
                        # 标记所有回复为不相关
                        for reply_index, reply in enumerate(simplified_data[hotel_index]['posts'][post_index]['replies']):
                            simplified_data[hotel_index]['posts'][post_index]['replies'][reply_index]['is_hotel_related'] = False
                            simplified_data[hotel_index]['posts'][post_index]['replies'][reply_index]['is_hotel_related_reason'] = "所属帖子与酒店无关"

                # 打印帖子分析进度
                post_progress = (analyzed_posts_count / total_posts_to_analyze) * 100 if total_posts_to_analyze > 0 else 0
                print(f"\r分析帖子进度: {post_progress:.2f}% ({analyzed_posts_count}/{total_posts_to_analyze})", end="")

            except Exception as exc:
                print(f"\n处理帖子结果时发生错误: {exc}")
                # 标记帖子分析失败
                task_info = futures_map[future]
                hotel_index, post_index = task_info['location']
                simplified_data[hotel_index]['posts'][post_index]['is_hotel_related'] = False
                simplified_data[hotel_index]['posts'][post_index]['is_hotel_related_reason'] = f"处理错误: {exc}"
                # 同样，其回复也不再分析
                total_replies_to_analyze -= len(simplified_data[hotel_index]['posts'][post_index]['replies'])
                continue
        print("\n帖子分析完成，开始处理回复...")

        # 处理回复分析结果
        reply_futures = {f: info for f, info in futures_map.items() if info['type'] == 'reply'}
        for future in as_completed(reply_futures):
            try:
                partial_res = future.result()
                analyzed_replies_count += 1
                task_info = futures_map[future]
                hotel_index, post_index, reply_index = task_info['location']
                
                is_related = False
                reason = "分析失败或无结果"
                if partial_res:
                    is_related = partial_res.get('is_hotel_related', False)
                    reason = partial_res.get('is_hotel_related_reason', '无原因')
                    
                # 更新回复的分析结果
                simplified_data[hotel_index]['posts'][post_index]['replies'][reply_index]['is_hotel_related'] = is_related
                simplified_data[hotel_index]['posts'][post_index]['replies'][reply_index]['is_hotel_related_reason'] = reason

                # 打印回复分析进度
                # 确保 total_replies_to_analyze 不为零
                if total_replies_to_analyze > 0:
                    reply_progress = (analyzed_replies_count / total_replies_to_analyze) * 100
                    print(f"\r分析回复进度: {reply_progress:.2f}% ({analyzed_replies_count}/{total_replies_to_analyze})", end="")
                else:
                        print(f"\r分析回复进度: 无需分析的回复 ({analyzed_replies_count}/0)", end="")

            except Exception as exc:
                print(f"\n处理回复结果时发生错误: {exc}")
                # 标记回复分析失败
                task_info = futures_map[future]
                hotel_index, post_index, reply_index = task_info['location']
                simplified_data[hotel_index]['posts'][post_index]['replies'][reply_index]['is_hotel_related'] = False
                simplified_data[hotel_index]['posts'][post_index]['replies'][reply_index]['is_hotel_related_reason'] = f"处理错误: {exc}"
                continue
        print("\n回复分析完成!")

    # 统计分析结果
    final_hotel_related_posts = sum(1 for hotel in simplified_data 
                                    for post in hotel['posts'] 
                                    if post.get('is_hotel_related'))
    final_is_ad_posts = sum(1 for hotel in simplified_data
                                for post in hotel['posts']
                                if post.get('is_ad'))
    final_hotel_related_replies = sum(1 for hotel in simplified_data 
                                    for post in hotel['posts'] 
                                    for reply in post['replies'] 
                                    if reply.get('is_hotel_related'))
    
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


def  analyze_keywords(analyzed_data, max_workers=500):
    def analyze_keywords_for_post(keywords, post_content, reply_content='', type='post'):
        openai_service = OpenAIService()
        
        try:
            if type == 'post':
                analysis = openai_service.infer(
                    user_prompt=analyze_post_user_prompt.format(post_content=post_content),
                    system_prompt=analyze_post_system_prompt.format(keywords=keywords),
                )
            elif type == 'reply':
                analysis = openai_service.infer(
                    user_prompt=analyze_reply_user_prompt.format(reply_content=reply_content, post_content=post_content),
                    system_prompt=analyze_reply_system_prompt.format(keywords=keywords),
                )
            else:
                raise ValueError(f"Invalid type: {type}")
        except Exception as e:
            if type == 'post':
                print(f"Error analyzing post {post_content[20:]}: {e}")
            elif type == 'reply':
                print(f"Error analyzing reply {reply_content[20:]}: {e}")
            return None
        if analysis:
            return analysis
        else:
            return None

    start_time = datetime.now()
    total_posts = 0
    total_replies = 0
    analyzed_posts = 0
    analyzed_replies = 0
        
    # 计算需要分析的帖子和评论数量
    for hotel in analyzed_data:
        for post in hotel['posts']:
            if post.get('is_hotel_related'):
                total_posts += 1
                total_replies += sum(1 for reply in post['replies'] 
                                    if reply.get('is_hotel_related'))
    
    print(f"找到 {total_posts} 个相关帖子和 {total_replies} 个相关回复")
    
    # 合并分析帖子和评论
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures_map = {}
        
        for hotel_index, hotel in enumerate(analyzed_data):
            if hotel['hotel'] == '惠庭':
                keywords = Keywords.get_exclusive_keywords_str()
            else:
                keywords = Keywords.get_all_keywords_str()
                
            for post_index, post in enumerate(hotel['posts']):
                if post.get('is_hotel_related'):
                    post_content = post.get('title', '') + '\n' + post['content']
                    # 提交帖子分析任务
                    future_post = executor.submit(analyze_keywords_for_post, keywords, post_content, '', 'post')
                    futures_map[future_post] = {'type': 'post', 'location': (hotel_index, post_index)}
                    
                    # 提交评论分析任务
                    for reply_index, reply in enumerate(post['replies']):
                        if reply.get('is_hotel_related'):
                            future_reply = executor.submit(analyze_keywords_for_post, keywords, post_content, reply['content'], 'reply')
                            futures_map[future_reply] = {'type': 'reply', 'location': (hotel_index, post_index, reply_index)}
        
        # 处理结果
        for future in as_completed(futures_map):
            try:
                partial_res = future.result()
                task_info = futures_map[future]
                task_type = task_info['type']
                location = task_info['location']
                
                if partial_res:
                    if task_type == 'post':
                        hotel_index, post_index = location
                        filtered_keywords = Keywords.filter_mentioned_keywords(partial_res.get('keywords_mentioned', {}))
                        analyzed_data[hotel_index]['posts'][post_index]['keywords_mentioned'] = filtered_keywords
                        analyzed_posts += 1
                    elif task_type == 'reply':
                        hotel_index, post_index, reply_index = location
                        filtered_keywords = Keywords.filter_mentioned_keywords(partial_res.get('keywords_mentioned', {}))
                        analyzed_data[hotel_index]['posts'][post_index]['replies'][reply_index]['keywords_mentioned'] = filtered_keywords
                        analyzed_replies += 1
                
                # 更新进度显示
                post_progress = (analyzed_posts / total_posts) * 100 if total_posts > 0 else 0
                reply_progress = (analyzed_replies / total_replies) * 100 if total_replies > 0 else 0
                print(f"\r分析进度: 帖子 {post_progress:.2f}% ({analyzed_posts}/{total_posts}) | 回复 {reply_progress:.2f}% ({analyzed_replies}/{total_replies})", end="", flush=True)
                
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
    
    # 内部的 get_frequent_mentioned_word 函数定义
    # 注意：此函数内部的 user_prompt 和 system_prompt 需要被修改为适合提取高频词汇的任务，
    # 并且其返回的 analysis 结构应为 [{'word': 'xx', 'sentiment': 'positive or negative'}]
    def get_frequent_mentioned_word(content, primary_keyword, secondary_keyword):
        openai_service = OpenAIService()
        try:
            analysis = openai_service.infer(
                user_prompt=extract_frequent_words_user_prompt.format(text_content=content, primary_keyword=primary_keyword, secondary_keyword=secondary_keyword),
                system_prompt=extract_frequent_words_system_prompt,
            )
        except Exception as e:
            print(f"Error analyzing content for frequent words starting with {content[:10]}...: {e}")
            return [] # 返回空列表表示分析失败或无结果
        
        if analysis and isinstance(analysis, list):
            return analysis 
        else:
            print(f"Warning: Analysis for content starting with {content[:30]}... did not return the expected list format. Got: {analysis}")
            return []

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
                    updated_keyword_map[p_keyword][s_keyword] = [] # 如果没有内容，则设置为空列表
                    continue
                
                # 合并内容为一个字符串
                combined_content = "\n".join(top_contents)
                
                future = executor.submit(get_frequent_mentioned_word, combined_content, p_keyword, s_keyword)
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
                updated_keyword_map[p_keyword][s_keyword] = [] # 出错时也设置为空列表
            finally:
                processed_count += 1
                progress = (processed_count / total_tasks) * 100 if total_tasks > 0 else 0
                print(f"\r提取高频词汇进度: {progress:.2f}% ({processed_count}/{total_tasks})", end="")
    
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
            return {"typical_positive_reviews": [], "typical_negative_reviews": []} # 如果内容为空，返回空结果
        
        try:
            analysis = openai_service.infer(
                user_prompt=extract_typical_reviews_user_prompt.format(primary_keyword=p_keyword, text_content=combined_content),
                system_prompt=extract_typical_reviews_system_prompt,
            )
        except Exception as e:
            print(f"Error analyzing typical reviews for primary keyword '{p_keyword}': {e}")
            return {"typical_positive_reviews": [], "typical_negative_reviews": []} # 分析失败返回空结果
        
        if analysis and isinstance(analysis, dict) and \
           "typical_positive_reviews" in analysis and "typical_negative_reviews" in analysis:
            return analysis
        else:
            print(f"Warning: Analysis for primary keyword '{p_keyword}' did not return the expected dict format. Got: {analysis}")
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
                print(f"Warning: Contents for {p_keyword} -> {s_keyword} is not a list, skipping.")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for p_keyword, all_contents in primary_keyword_all_contents.items():
            if not all_contents:
                typical_reviews_result[p_keyword] = {"typical_positive_reviews": [], "typical_negative_reviews": []}
                continue
            
            # 限制传递给模型的内容长度，避免过长，例如取前 N 条或总字符数限制
            # 这里简单地取前 50 条内容，您可以根据需要调整策略
            contents_to_analyze = all_contents[:50] 
            
            future = executor.submit(get_typical_reviews_for_primary_keyword, p_keyword, contents_to_analyze)
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
                typical_reviews_result[p_keyword] = {"typical_positive_reviews": [], "typical_negative_reviews": []}
            finally:
                processed_count += 1
                progress = (processed_count / total_tasks) * 100 if total_tasks > 0 else 0
                print(f"\r提取典型评价案例进度: {progress:.2f}% ({processed_count}/{total_tasks})", end="")
    
    print("\n典型评价案例提取完成!")
    return typical_reviews_result
    
def main():
    pass
    # analyze frequent words
    # data = [*get_raw_data('analysis_result/wb_analyzed.json'), *get_raw_data('analysis_result/xhs_analyzed.json'), *get_raw_data('analysis_result/flyert_analyzed.json')]
    # collected_content = collect_huiting_content_by_keyword(data)
    # pprint(collected_content['Minibar生活吧']['做饭/烹饪 Cooking'])
    # frequent_mentioned_words = extract_frequent_mentioned_words(collected_content)
    # write_to_json(frequent_mentioned_words, "analysis_result/huiting_frequent_mentioned_words.json")
    
    # extract typical reviews
    # data = [*get_raw_data('analysis_result/wb_analyzed.json'), *get_raw_data('analysis_result/xhs_analyzed.json'), *get_raw_data('analysis_result/flyert_analyzed.json')]
    # keyword_content_map = collect_huiting_content_by_keyword(data)
    # typical_reviews = extract_typical_reviews_by_primary_keyword(keyword_content_map)
    # write_to_json(typical_reviews, "analysis_result/huiting_typical_reviews.json")
    
    # analyzed more wb data
    # all_wb_data = get_raw_data("raw_data/wb.json")
    # analyzed_wb_data = get_raw_data("analysis_result/wb_analyzed.json")
    # unanalyzed_wb_data = get_unanalyzed_posts(all_wb_data, analyzed_wb_data)
    # first_analyzed_wb_data = analyze_is_hotel_related(unanalyzed_wb_data)
    # analyzed_wb_data = analyze_keywords(first_analyzed_wb_data)
    # print("分析完毕，正在写入文件")
    # merge_data(analyzed_wb_data, "analysis_result/wb_analyzed.json")
    
    # analyze more wb raw data from media crawler
    hotels = ['亚朵轻居']
    for hotel in hotels:
        print(f"开始分析 {hotel} 的数据")
        posts = get_raw_data(f"raw_data/wb/search_contents_2025-05-09_{hotel}.json")
        comments = get_raw_data(f"raw_data/wb/search_comments_2025-05-09_{hotel}.json")
        formatted_data = format_wb_data_from_media_crawler_by_hotel(posts, comments, hotel)
        first_analyzed_data = analyze_is_hotel_related(formatted_data)
        analyzed_data = analyze_keywords(first_analyzed_data)
        merge_data(formatted_data, 'raw_data/wb.json')
        merge_data(analyzed_data, 'analysis_result/wb_analyzed.json')
        print(f"{hotel} 的数据分析完毕")
    
    # analyze more xhs raw data from mobile crawler
    # paths = ['raw_data/xhs/xhs_馨乐庭_all.json']
    # hotels = ['馨乐庭']
    # formatted_data = format_all_xhs_data_from_mobile(paths, hotels)
    # first_analyzed_data = analyze_is_hotel_related(formatted_data)
    # analyzed_data = analyze_keywords(first_analyzed_data)
    # merge_data(formatted_data, 'raw_data/xhs.json')
    # merge_data(analyzed_data, 'analysis_result/xhs_analyzed.json')
    # print("XHS 的数据分析完毕")

if __name__ == "__main__":
    main()