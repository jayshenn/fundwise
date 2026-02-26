"""执行日常投研批处理流水线。"""

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
    parser = argparse.ArgumentParser(description="执行一键日常投研流水线。")
    parser.add_argument(
        "--symbols",
        default="",
        help="逗号分隔的标准代码列表，例如 600519.SH,000333.SZ,00700.HK",
    )
    parser.add_argument(
        "--symbols-file",
        type=Path,
        default=None,
        help="代码文件路径（每行一个标准 symbol）。",
    )
    parser.add_argument("--start-date", default=None, help="行情起始日期（YYYY-MM-DD）")
    parser.add_argument("--end-date", default=None, help="行情结束日期（YYYY-MM-DD）")
    parser.add_argument(
        "--run-date",
        default=None,
        help="归档目录日期（YYYY-MM-DD），默认使用当天日期。",
    )
    parser.add_argument(
        "--report-root",
        type=Path,
        default=PROJECT_ROOT / "reports" / "daily",
        help="日报归档根目录。",
    )
    parser.add_argument(
        "--normalized-root",
        type=Path,
        default=PROJECT_ROOT / "data" / "normalized",
        help="标准化数据归档根目录。",
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=PROJECT_ROOT / "data" / "metadata" / "fundwise.db",
        help="SQLite 元数据库路径。",
    )
    parser.add_argument(
        "--non-strict",
        action="store_true",
        help="非严格模式：接口失败时返回空数据而非异常。",
    )
    parser.add_argument(
        "--allow-partial-success",
        action="store_true",
        help="允许部分成功时返回 0；默认存在失败标的即返回非 0。",
    )
    return parser.parse_args()


def main() -> int:
    """执行流水线并返回退出码。"""
    from fundwise.data_adapter import AkshareDataAdapter
    from fundwise.pipeline import PipelineConfig, load_symbols, run_daily_pipeline

    args = parse_args()
    symbols = load_symbols(args.symbols, args.symbols_file)
    if not symbols:
        raise ValueError("未提供有效 symbol，请使用 --symbols 或 --symbols-file")

    adapter = AkshareDataAdapter(strict=not args.non_strict)
    result = run_daily_pipeline(
        PipelineConfig(
            symbols=symbols,
            start_date=args.start_date,
            end_date=args.end_date,
            report_root=args.report_root,
            normalized_root=args.normalized_root,
            db_path=args.db_path,
            run_date=args.run_date,
        ),
        adapter=adapter,
    )

    print(f"[完成] 执行日期：{result.run_date}")
    print(f"[完成] 成功标的：{len(result.success_symbols)}")
    print(f"[完成] 失败标的：{len(result.failed_symbols)}")
    print(f"[完成] 摘要 Markdown：{result.summary_markdown_path}")
    print(f"[完成] 摘要 JSON：{result.summary_json_path}")

    if result.failed_symbols:
        print("[告警] 存在失败标的，请查看摘要文件中的失败明细。")
        if not args.allow_partial_success:
            return 2

    if not result.success_symbols:
        print("[告警] 本次执行无成功标的。")
        return 3

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
