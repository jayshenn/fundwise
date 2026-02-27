"""将 AKShare 数据字典 HTML 导出为 CSV 与 JSON。"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))


def parse_args() -> argparse.Namespace:
    """解析命令行参数。"""
    parser = argparse.ArgumentParser(description="导出 AKShare 数据字典为结构化 CSV/JSON。")
    parser.add_argument(
        "--input-html",
        type=Path,
        default=PROJECT_ROOT / "docs" / "AKShare数据字典.html",
        help="AKShare 数据字典 HTML 路径。",
    )
    parser.add_argument(
        "--output-csv",
        type=Path,
        default=PROJECT_ROOT / "docs" / "AKShare数据字典.csv",
        help="CSV 输出路径。",
    )
    parser.add_argument(
        "--output-json",
        type=Path,
        default=PROJECT_ROOT / "docs" / "AKShare数据字典.json",
        help="JSON 输出路径。",
    )
    parser.add_argument(
        "--base-url",
        default="https://akshare.akfamily.xyz/data/",
        help="AKShare 文档基础地址，用于生成 full_url 字段。",
    )
    return parser.parse_args()


def main() -> int:
    """执行导出流程。"""
    from fundwise.data_adapter.akshare_dictionary import export_akshare_dictionary

    args = parse_args()
    if not args.input_html.exists():
        raise FileNotFoundError(f"输入文件不存在: {args.input_html}")

    node_count, leaf_count = export_akshare_dictionary(
        html_path=args.input_html,
        csv_path=args.output_csv,
        json_path=args.output_json,
        base_url=args.base_url,
    )
    print(f"[完成] 节点总数: {node_count}")
    print(f"[完成] 叶子节点: {leaf_count}")
    print(f"[完成] CSV: {args.output_csv}")
    print(f"[完成] JSON: {args.output_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
