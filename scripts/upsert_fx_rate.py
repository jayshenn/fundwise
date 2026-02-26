"""写入或更新本地 SQLite 汇率记录。"""

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
    parser = argparse.ArgumentParser(description="写入汇率记录到 SQLite 元数据库。")
    parser.add_argument("--date", required=True, help="汇率日期（YYYY-MM-DD）")
    parser.add_argument("--base-currency", required=True, help="基准币种，例如 HKD")
    parser.add_argument("--quote-currency", default="CNY", help="计价币种，默认 CNY")
    parser.add_argument("--rate", required=True, type=float, help="汇率值（必须 > 0）")
    parser.add_argument("--source", default="manual", help="数据来源，例如 manual/akshare")
    parser.add_argument(
        "--db-path",
        type=Path,
        default=PROJECT_ROOT / "data" / "metadata" / "fundwise.db",
        help="SQLite 元数据库路径",
    )
    return parser.parse_args()


def main() -> int:
    """执行汇率写入流程。"""
    from fundwise.storage import init_sqlite_metadata_db, upsert_fx_rate

    args = parse_args()
    db_path = init_sqlite_metadata_db(args.db_path)
    upsert_fx_rate(
        db_path=db_path,
        date=args.date,
        base_currency=args.base_currency,
        quote_currency=args.quote_currency,
        rate=args.rate,
        source=args.source,
    )
    print(
        "[完成] 汇率已写入："
        f"{args.date} 1 {args.base_currency.upper()} = {args.rate} {args.quote_currency.upper()}"
    )
    print(f"[完成] 元数据库：{db_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
