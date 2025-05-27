#!/usr/bin/env python3
"""
count_xhs_json.py — 统计小红书抓取 JSON 中的节点数量
===================================================

用法
----
```
python count_xhs_json.py <file.json> [-m MODE] [-k KEYWORD]
```

参数
----
* ``<file.json>``            要统计的 JSON 文件路径。
* ``-m, --mode``             计数模式，取值 ``raw`` 或 ``strict``，默认 ``strict``。
* ``-k, --keyword``          关键词过滤（仅对 **strict** 模式下的完整图文笔记生效）。

两种模式说明
--------------
* **raw**   —— 保留原始脚本行为：
  * 统计所有笔记条目，不区分 ``type``；
  * 统计所有顶层评论与回复（包括空文本）。

python analyze_scripts/count_xhs_json.py raw_data/xhs/5-15/xhs_惠庭_all.json -k 惠庭
python analyze_scripts/count_xhs_json.py raw_data/xhs/5-15/filtered/xhs_惠庭_all.json -k 惠庭
* **strict**（默认） —— 按如下严格规则过滤后再计数：
  1. 跳过 ``type`` 为 ``"skipped_timestamp"`` 或 ``"video"`` 的条目。
  2. 跳过只包含 ``list_view_content_desc``（及可选 ``scraped_at``）的占位条目。
  3. 若指定 ``--keyword/-k``，只有当标题、正文或任意层级评论文本 **包含** 关键词时才计入该笔记及其评论。
  4. 对所有层级评论，若 ``comment_text`` 为空或全空白，则该评论不计数。

输出
----
脚本会打印：
```
Notes: <笔记数>
First-level comments: <顶层评论数>
Replies (≥2nd level): <二级及更深层回复数>
All comments: <所有评论数>
Notes + All comments: <笔记+评论总数>
```

"""

import json
import re
import sys
from pathlib import Path
from typing import Sequence, Mapping, Any, Tuple


# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------


def recursive_count(replies: Sequence[Mapping[str, Any]], *, skip_empty: bool) -> int:
    """递归统计 ``sub_comments``。当 ``skip_empty`` 为 True 时，忽略空文本评论。"""
    total = 0
    for r in replies:
        # 条件 4：跳过空字符串评论
        if skip_empty and not r.get("comment_text", "").strip():
            # 仍需深入其 sub_comments，以防有有效回复嵌套其中
            total += recursive_count(r.get("sub_comments", []), skip_empty=skip_empty)
            continue
        sub = r.get("sub_comments", [])
        total += len(sub) + recursive_count(sub, skip_empty=skip_empty)
    return total


def note_passes_strict_rules(
    note: Mapping[str, Any], keyword: str | None
) -> Tuple[bool, bool, bool]:
    """返回 (skip_entire_note, has_keyword, is_placeholder) 三元组。"""
    # 规则 1：跳过特定 type
    note_type = note.get("type", "")
    if note_type in {"skipped_timestamp", "video"}:
        return True, False, False

    # 规则 2：占位条目
    meaningful_keys = {
        k for k in note.keys() if k not in {"list_view_content_desc", "scraped_at"}
    }
    if not meaningful_keys:
        return True, False, True

    # 规则 3：关键词过滤
    if keyword:
        keyword_lower = keyword.lower()
        title = note.get("title", "").lower()
        body = note.get("body", "").lower()
        if keyword_lower in title or keyword_lower in body:
            return False, True, False
        # 搜索评论文本
        for c in note.get("comments", []):
            if comment_contains_keyword(c, keyword_lower):
                return False, True, False
        # 未找到关键词 ⇒ 跳过
        return True, False, False

    # 无关键词 ⇒ 通过
    return False, True, False


def comment_contains_keyword(comment: Mapping[str, Any], kw: str) -> bool:
    if kw in comment.get("comment_text", "").lower():
        return True
    return any(
        comment_contains_keyword(child, kw) for child in comment.get("sub_comments", [])
    )


# ------------------------------------------------------------
# Main
# ------------------------------------------------------------


def count_notes(data: list[dict], *, mode: str, keyword: str | None):
    """根据模式与关键词返回计数结果字典。"""
    skip_empty = mode == "strict"

    note_cnt = 0
    first_lvl_cnt = 0
    reply_cnt = 0

    for note in data:
        if mode == "raw":
            # 原始行为：不跳过任何条目
            note_cnt += 1
            comments = note.get("comments", [])
            first_lvl_cnt += len(comments)
            reply_cnt += recursive_count(comments, skip_empty=False)
            continue

        # strict 模式
        skip_note, _, _ = note_passes_strict_rules(note, keyword)
        if skip_note:
            continue

        note_cnt += 1
        comments = note.get("comments", [])
        # 规则 4：顶层评论也需检查空文本
        first_level_valid = [c for c in comments if c.get("comment_text", "").strip()]
        first_lvl_cnt += len(first_level_valid)
        reply_cnt += recursive_count(first_level_valid, skip_empty=skip_empty)

    all_comments_cnt = first_lvl_cnt + reply_cnt
    total_nodes_cnt = note_cnt + all_comments_cnt

    return {
        "Notes": note_cnt,
        "First-level comments": first_lvl_cnt,
        "Replies (≥2nd level)": reply_cnt,
        "All comments": all_comments_cnt,
        "Notes + All comments": total_nodes_cnt,
    }


def main(fp: Path, mode: str, keyword: str | None):
    try:
        data = json.loads(fp.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"Error reading or parsing JSON: {e}", file=sys.stderr)
        sys.exit(1)

    if not isinstance(data, list):
        print("JSON root must be a list", file=sys.stderr)
        sys.exit(1)

    stats = count_notes(data, mode=mode, keyword=keyword)
    for k, v in stats.items():
        print(f"{k}: {v}")


if __name__ == "__main__":
    import argparse

    sys.argv = [
        "count_xhs_json.py",
        "-k",
        "惠庭",
        "raw_data/xhs/5-16/filtered/xhs_惠庭_all.json",
    ]

    parser = argparse.ArgumentParser(
        description="Count notes/comments in a Xiaohongshu‑scraper JSON."
    )
    parser.add_argument("json_file", help="Path to the JSON file")
    parser.add_argument(
        "-m",
        "--mode",
        choices=["raw", "strict"],
        default="strict",
        help="Counting mode: 'raw' = original behaviour; 'strict' = apply all skip rules (default)",
    )
    parser.add_argument(
        "-k",
        "--keyword",
        default=None,
        help="Keyword filter applied in strict mode (case‑insensitive Chinese/English ok)",
    )

    args = parser.parse_args()
    main(Path(args.json_file), mode=args.mode, keyword=args.keyword)
