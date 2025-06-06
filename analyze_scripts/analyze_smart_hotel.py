from utils import *
from prompt import *

def main():
    smart_hotel_data = get_raw_data("raw_data/flyert-smart-hotel.json")
    post_contents = ""
    post_count = 0
    for hotel in smart_hotel_data:
        for post in hotel["posts"]:
            content = "**TITLE: ** " + post["title"] + "\n" + "**CONTENT: ** " + post["content"]
            post_count += 1
            post_contents += f"**POST {post_count}: **\n" + content + "\n"

    openai_service = OpenAIService()
    analysis = openai_service.infer(
        model="gpt-4.1",
        user_prompt=analyze_smart_hotel_user_prompt.format(content=post_contents),
        system_prompt=analyze_smart_hotel_system_prompt,
    )

    write_to_json(analysis, "analyze_result/ai_insights.json")

if __name__ == "__main__":
    main()