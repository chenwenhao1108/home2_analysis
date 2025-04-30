import json
from utils import *
from prompt import *
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

def analyze_is_hotel_related(max_workers=200, platforms=['flyert', 'wb', 'xhs']):
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
    for platform in platforms:
        print(f"\n开始分析平台: {platform}")
        raw_data = get_raw_data(f'raw_data/{platform}.json')
        
        if not raw_data:
            print(f"Failed to get posts from {platform}")
            continue
        
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
        
        # 将更新后的数据写入新文件
        output_path = f'analysis_result/{platform}_analyzed.json'
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(simplified_data, f, ensure_ascii=False, indent=4)
        print(f"分析结果已保存到 {output_path}")


def analyze_keywords(max_workers=500, platforms=['flyert', 'weibo', 'xhs']):
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
    
    # 按平台读取已分析的数据
    for platform in platforms:
        print(f"\n开始分析平台: {platform}")
        try:
            with open(f'analysis_result/{platform}_analyzed.json', 'r', encoding='utf-8') as f:
                analyzed_data = json.load(f)
        except Exception as e:
            print(f"Failed to read analyzed data from {platform}: {e}")
            continue
        
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
        
        # 更新原文件
        with open(f'analysis_result/{platform}_analyzed.json', 'w', encoding='utf-8') as f:
            json.dump(analyzed_data, f, ensure_ascii=False, indent=4)
        print(f"分析结果已更新到 {platform}_analyzed.json")


def main():
    analyze_is_hotel_related(platforms=['xhs_test'])
    analyze_keywords(platforms=['xhs_test'])

if __name__ == "__main__":
    main()