"""AKShare 数据字典解析与导出工具。"""

from __future__ import annotations

import csv
import json
import re
from dataclasses import asdict, dataclass
from datetime import datetime
from html import unescape
from pathlib import Path
from typing import Final
from urllib.parse import urljoin

ARTICLE_BODY_MARKER: Final[str] = '<div itemprop="articleBody">'
TOCTREE_WRAPPER_MARKER: Final[str] = '<div class="toctree-wrapper compound">'
DEFAULT_DOC_BASE_URL: Final[str] = "https://akshare.akfamily.xyz/data/"

LINK_PATTERN: Final[re.Pattern[str]] = re.compile(
    r'<li class="toctree-l(?P<level>\d+)">\s*'
    r'<a class="reference internal" href="(?P<href>[^"]*)">(?P<title>.*?)</a>',
    flags=re.DOTALL,
)
TAG_PATTERN: Final[re.Pattern[str]] = re.compile(r"<[^>]+>")
DIV_TOKEN_PATTERN: Final[re.Pattern[str]] = re.compile(r"<div\b[^>]*>|</div>", re.IGNORECASE)


@dataclass(frozen=True, slots=True)
class DictionaryNode:
    """AKShare 数据字典中的一个层级节点。"""

    index: int
    level: int
    title: str
    href: str
    full_url: str
    doc_path: str
    anchor: str
    parent_title: str
    root_title: str
    path: str
    is_leaf: bool


def _extract_balanced_div(html_text: str, start_index: int) -> str:
    """从指定位置提取完整的 div 片段。"""
    depth = 0
    for match in DIV_TOKEN_PATTERN.finditer(html_text, pos=start_index):
        token = match.group(0).lower()
        if token.startswith("<div"):
            depth += 1
            continue
        depth -= 1
        if depth == 0:
            return html_text[start_index : match.end()]
    raise ValueError("未能定位 toctree 包裹节点结束位置。")


def _normalize_title(raw_title: str) -> str:
    """清理 HTML 标签并解码标题文本。"""
    title = TAG_PATTERN.sub("", unescape(raw_title)).strip()
    return title or "(空标题)"


def _split_href(href: str) -> tuple[str, str]:
    """拆分文档路径和锚点。"""
    if "#" not in href:
        return href, ""
    doc_path, _, anchor = href.partition("#")
    return doc_path, anchor


def _build_full_url(base_url: str, href: str) -> str:
    """根据基础地址与相对 href 构造完整链接。"""
    normalized_base = base_url if base_url.endswith("/") else f"{base_url}/"
    return urljoin(normalized_base, href)


def extract_toctree_html(html_text: str) -> str:
    """提取正文中的 toctree HTML 片段。"""
    article_start = html_text.find(ARTICLE_BODY_MARKER)
    search_start = article_start if article_start >= 0 else 0
    wrapper_start = html_text.find(TOCTREE_WRAPPER_MARKER, search_start)
    if wrapper_start < 0:
        raise ValueError("未找到 AKShare 数据字典 toctree 节点。")
    return _extract_balanced_div(html_text, wrapper_start)


def parse_akshare_dictionary(
    html_text: str,
    base_url: str = DEFAULT_DOC_BASE_URL,
) -> list[DictionaryNode]:
    """将 AKShare 数据字典 HTML 解析为层级节点列表。"""
    toctree_html = extract_toctree_html(html_text)
    raw_nodes: list[dict[str, str | int]] = []
    stack: list[str] = []

    for match in LINK_PATTERN.finditer(toctree_html):
        level = int(match.group("level"))
        href = unescape(match.group("href")).strip()
        title = _normalize_title(match.group("title"))

        while len(stack) >= level:
            stack.pop()
        if len(stack) < level - 1:
            stack.extend(["(缺失层级)"] * (level - 1 - len(stack)))

        parent_title = stack[-1] if stack else ""
        stack.append(title)

        doc_path, anchor = _split_href(href)
        raw_nodes.append(
            {
                "level": level,
                "title": title,
                "href": href,
                "doc_path": doc_path,
                "anchor": anchor,
                "parent_title": parent_title,
                "root_title": stack[0],
                "path": " / ".join(stack),
            }
        )

    if not raw_nodes:
        raise ValueError("未解析到任何节点，请检查 HTML 内容是否完整。")

    nodes: list[DictionaryNode] = []
    for idx, node in enumerate(raw_nodes):
        level = int(node["level"])
        next_level = int(raw_nodes[idx + 1]["level"]) if idx + 1 < len(raw_nodes) else 0
        nodes.append(
            DictionaryNode(
                index=idx + 1,
                level=level,
                title=str(node["title"]),
                href=str(node["href"]),
                full_url=_build_full_url(base_url=base_url, href=str(node["href"])),
                doc_path=str(node["doc_path"]),
                anchor=str(node["anchor"]),
                parent_title=str(node["parent_title"]),
                root_title=str(node["root_title"]),
                path=str(node["path"]),
                is_leaf=next_level <= level,
            )
        )

    return nodes


def write_dictionary_csv(nodes: list[DictionaryNode], output_path: Path) -> None:
    """将解析节点导出为 CSV。"""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "index",
        "level",
        "title",
        "href",
        "full_url",
        "doc_path",
        "anchor",
        "parent_title",
        "root_title",
        "path",
        "is_leaf",
    ]
    with output_path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        for node in nodes:
            row = asdict(node)
            row["is_leaf"] = int(node.is_leaf)
            writer.writerow(row)


def write_dictionary_json(
    nodes: list[DictionaryNode],
    output_path: Path,
    source_path: Path | None = None,
) -> None:
    """将解析节点导出为 JSON。"""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "source_path": str(source_path) if source_path is not None else None,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "node_count": len(nodes),
        "leaf_count": sum(node.is_leaf for node in nodes),
        "nodes": [asdict(node) for node in nodes],
    }
    output_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def export_akshare_dictionary(
    html_path: Path,
    csv_path: Path,
    json_path: Path,
    base_url: str = DEFAULT_DOC_BASE_URL,
) -> tuple[int, int]:
    """从 HTML 导出 AKShare 数据字典的 CSV 与 JSON。"""
    html_text = html_path.read_text(encoding="utf-8")
    nodes = parse_akshare_dictionary(html_text=html_text, base_url=base_url)
    write_dictionary_csv(nodes, csv_path)
    write_dictionary_json(nodes, json_path, source_path=html_path)
    return len(nodes), sum(node.is_leaf for node in nodes)
