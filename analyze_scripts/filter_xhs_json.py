#!/usr/bin/env python3
"""
filter_xhs_json.py — 根据 count_xhs_json.py 的严格模式过滤小红书 JSON 数据
========================================================================

用法
----

参数
----
* ``<input_file.json>``      要过滤的原始 JSON 文件路径。
* ``<output_file.json>``     保存过滤后数据的 JSON 文件路径。
* ``-k, --keyword``          关键词过滤（可选，逻辑与 count_xhs_json.py 中 strict 模式一致）。

说明
----
此脚本基于 `count_xhs_json.py` 中的 **strict** 模式过滤规则：
1. 跳过 ``type`` 为 ``"skipped_timestamp"`` 或 ``"video"`` 的条目。
2. 跳过只包含 ``list_view_content_desc``（及可选 ``scraped_at``）的占位条目。
3. 若指定 ``--keyword/-k``，只有当笔记的标题、正文或其任意层级评论文本 **包含** 关键词时，
   该笔记及其有效评论才会被保留。
4. 对所有层级评论，若 ``comment_text`` 为空或全空白，则该评论不被包含，但其子评论仍可能被处理。

输出
----
脚本会将符合严格模式条件的笔记及其有效评论输出到指定的 ``<output_file.json>``。
如果一个笔记被保留，但其所有评论都被过滤掉，那么输出的笔记中可能不包含 "comments" 键，
或者 "comments" 键对应一个空列表。
如果一个评论本身文本为空，但其子评论有效，则该空文本评论节点可能会被保留以维持结构，
或者其有效子评论被“提升”（具体行为取决于实现，此版本倾向于保留结构）。
此版本保留空文本父评论节点，如果它包含有效子评论。

"""

import json
import sys
import copy  # For deepcopy
from pathlib import Path
from typing import Sequence, Mapping, Any, Tuple, List, Dict, Optional

# ------------------------------------------------------------
# Helpers (adapted from count_xhs_json.py)
# ------------------------------------------------------------


def comment_contains_keyword(comment: Mapping[str, Any], kw_lower: str) -> bool:
    """检查评论及其子评论是否包含关键词（大小写不敏感）。"""
    if kw_lower in comment.get("comment_text", "").lower():
        return True
    return any(
        comment_contains_keyword(child, kw_lower)
        for child in comment.get("sub_comments", [])
    )


def note_passes_strict_rules(
    note: Mapping[str, Any], keyword: Optional[str]
) -> Tuple[bool, bool, bool]:
    """
    检查笔记是否符合严格模式的过滤规则。
    返回 (should_skip_note, has_keyword_if_searched, is_placeholder) 三元组。
    """
    # 规则 1：跳过特定 type
    note_type = note.get("type", "")
    if note_type in {"skipped_timestamp", "video"}:
        return True, False, False  # Skip, no keyword (not searched), not placeholder

    # 规则 2：占位条目
    # A note is a placeholder if all its keys are among these two
    meaningful_keys = {
        k for k in note.keys() if k not in {"list_view_content_desc", "scraped_at"}
    }
    if not meaningful_keys:
        return True, False, True  # Skip, no keyword (not searched), is placeholder

    # 规则 3：关键词过滤
    if keyword:
        keyword_lower = keyword.lower()
        title = note.get("title", "").lower()
        body = note.get("body", "").lower()

        if keyword_lower in title or keyword_lower in body:
            return False, True, False  # Keep, has keyword, not placeholder

        # 关键词不在标题或正文，则搜索评论文本
        for c in note.get("comments", []):
            if comment_contains_keyword(c, keyword_lower):
                return (
                    False,
                    True,
                    False,
                )  # Keep, has keyword (in comments), not placeholder

        # 未找到关键词 (且关键词被指定) ⇒ 跳过
        return True, False, False  # Skip, no keyword found, not placeholder

    # 无关键词参数 ⇒ 笔记通过此项检查 (不因关键词缺失而跳过)
    return (
        False,
        True,
        False,
    )  # Keep, keyword search not applicable or passed, not placeholder


def filter_valid_comments_recursive(
    comments_list: Sequence[Mapping[str, Any]],
) -> List[Mapping[str, Any]]:
    """
    递归过滤评论列表，将所有层级的评论（包括子评论）放入同一个扁平数组中。
    每个评论对象都会包含相同的键结构。
    返回一个包含所有有效评论的扁平列表。
    """
    processed_comments: List[Mapping[str, Any]] = []

    def process_comment(comment: Mapping[str, Any]) -> None:
        # 创建标准化的评论对象结构
        comment_text = comment.get("comment_text", "").strip()
        if not comment_text:  # 跳过空评论
            return

        standard_comment = {
            "unique_id": comment.get("unique_id", ""),
            "comment_text": comment_text,
            "date_location": comment.get("date_location", ""),
        }
        processed_comments.append(standard_comment)

        # 递归处理子评论
        for sub_comment in comment.get("sub_comments", []):
            process_comment(sub_comment)

    # 处理所有顶层评论及其子评论
    for comment in comments_list:
        process_comment(comment)

    return processed_comments


# ------------------------------------------------------------
# Main Processing Logic
# ------------------------------------------------------------


def filter_data_strict_mode(
    data: List[Dict[str, Any]], keyword: Optional[str]
) -> List[Dict[str, Any]]:
    """
    根据严格模式过滤笔记列表。
    """
    filtered_notes: List[Dict[str, Any]] = []

    for original_note in data:
        # 首先，决定是否要跳过整个笔记
        should_skip_note, _, _ = note_passes_strict_rules(original_note, keyword)

        if should_skip_note:
            continue

        # 如果笔记本身不跳过，则处理其评论
        # 创建笔记的深拷贝，以免修改原始数据（如果后续需要）
        # 并且确保我们是在修改一个独立的副本
        note_to_keep = copy.deepcopy(original_note)

        original_comments = note_to_keep.get("comments", [])
        if original_comments:
            valid_comments = filter_valid_comments_recursive(original_comments)
            if valid_comments:
                note_to_keep["comments"] = valid_comments
            else:
                # 如果所有评论都被过滤掉了，移除 "comments" 键
                note_to_keep.pop("comments", None)

        filtered_notes.append(note_to_keep)

    return filtered_notes


# ------------------------------------------------------------
# Script Entry Point
# ------------------------------------------------------------


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Filter Xiaohongshu JSON data based on strict mode rules from count_xhs_json.py."
    )
    parser.add_argument("input_json_file", help="Path to the input JSON file.")
    parser.add_argument("output_json_file", help="Path to save the filtered JSON data.")
    parser.add_argument(
        "-k",
        "--keyword",
        default=None,
        help="Keyword filter (case-insensitive). If provided, notes (and their comments) "
        "are kept only if the keyword appears in the note's title, body, or any comment text.",
    )

    args = parser.parse_args()

    input_path = Path(args.input_json_file)
    output_path = Path(args.output_json_file)

    if not input_path.is_file():
        print(f"Error: Input file not found: {input_path}", file=sys.stderr)
        sys.exit(1)

    try:
        with open(input_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        print(f"Error: Could not decode JSON from {input_path}. {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error reading file {input_path}: {e}", file=sys.stderr)
        sys.exit(1)

    if not isinstance(data, list):
        print("Error: JSON root must be a list of notes.", file=sys.stderr)
        sys.exit(1)

    # Perform the filtering
    filtered_data = filter_data_strict_mode(data, args.keyword)

    # Write the filtered data to the output file
    try:
        output_path.parent.mkdir(
            parents=True, exist_ok=True
        )  # Ensure output directory exists
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(filtered_data, f, ensure_ascii=False, indent=2)
        print(f"Filtered data successfully written to: {output_path}")
        if not filtered_data:
            print("Warning: The filtered data is empty. No notes matched the criteria.")
    except Exception as e:
        print(f"Error writing filtered data to {output_path}: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
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
    for hotel in hotels:
        input_path = f"raw_data/xhs/5-19/xhs_{hotel}_all.json"
        output_path = f"raw_data/xhs/5-19/filtered/xhs_{hotel}_all.json"
        sys.argv = ["filter_xhs_json.py", input_path, output_path, "-k", hotel]

        main()
