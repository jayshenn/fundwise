"""生成观察池评分报告，并写入报告索引。"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))


def parse_args() -> argparse.Namespace:
    """解析命令行参数。"""
    parser = argparse.ArgumentParser(description="生成观察池评分报告。")
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
        "--report-dir",
        type=Path,
        default=PROJECT_ROOT / "reports" / "watchlist_screener",
        help="报告输出目录",
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=PROJECT_ROOT / "data" / "metadata" / "fundwise.db",
        help="SQLite 元数据库路径",
    )
    parser.add_argument(
        "--non-strict",
        action="store_true",
        help="非严格模式：接口失败时返回空数据而非异常。",
    )
    return parser.parse_args()


def _load_symbols(args: argparse.Namespace) -> list[str]:
    """加载并去重 symbol 列表。"""
    values: list[str] = []
    if args.symbols:
        values.extend([item.strip() for item in args.symbols.split(",") if item.strip()])
    if args.symbols_file is not None and args.symbols_file.exists():
        lines = args.symbols_file.read_text(encoding="utf-8").splitlines()
        values.extend(
            [
                line.strip()
                for line in lines
                if line.strip() and not line.strip().startswith("#")
            ]
        )

    seen: set[str] = set()
    result: list[str] = []
    for item in values:
        upper_item = item.upper()
        if upper_item in seen:
            continue
        seen.add(upper_item)
        result.append(upper_item)
    return result


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    """将评分明细写入 CSV 文件。"""
    fieldnames = [
        "symbol",
        "total_score",
        "tier",
        "growth",
        "quality",
        "valuation",
        "momentum",
        "as_of_date",
        "notes",
    ]
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    """执行观察池评分报告生成流程。"""
    from fundwise.company_dossier import build_company_dossier
    from fundwise.data_adapter import AkshareDataAdapter
    from fundwise.report_engine import generate_watchlist_charts
    from fundwise.storage import (
        JOB_FAILED,
        JOB_SUCCESS,
        finish_data_job,
        init_sqlite_metadata_db,
        record_report,
        resolve_fx_to_cny,
        start_data_job,
        upsert_symbol,
    )
    from fundwise.watchlist_screener import (
        render_watchlist_markdown,
        score_company_dossier,
    )

    args = parse_args()
    symbols = _load_symbols(args)
    if not symbols:
        raise ValueError("未提供有效 symbol，请使用 --symbols 或 --symbols-file")

    adapter = AkshareDataAdapter(strict=not args.non_strict)
    db_path = init_sqlite_metadata_db(args.db_path)
    upsert_symbol(
        db_path=db_path,
        symbol="WATCHLIST",
        market="META",
        currency="CNY",
        name="观察池",
        is_active=True,
    )
    job_id = start_data_job(
        db_path=db_path,
        job_type="generate_watchlist_report",
        symbol="WATCHLIST",
    )

    try:
        scores = []
        for symbol in symbols:
            symbol_info = adapter.get_symbol_info(symbol)
            upsert_symbol(
                db_path=db_path,
                symbol=symbol_info.symbol,
                market=symbol_info.market,
                currency=symbol_info.currency,
                name=None,
                is_active=True,
            )
            dataset = adapter.build_company_dataset(
                symbol=symbol_info.symbol,
                start_date=args.start_date,
                end_date=args.end_date,
            )
            fx_to_cny = resolve_fx_to_cny(
                db_path=db_path,
                currency=symbol_info.currency,
                date=args.end_date,
            )
            dossier = build_company_dossier(
                symbol=symbol_info.symbol,
                dataset=dataset,
                fx_to_cny=fx_to_cny,
            )
            score = score_company_dossier(dossier)
            scores.append(score)
            print(f"[完成] {symbol_info.symbol} 评分={score.total_score:.2f}（{score.tier}）")

        as_of_date = max(
            [item.as_of_date for item in scores if item.as_of_date] or ["unknown-date"]
        )
        args.report_dir.mkdir(parents=True, exist_ok=True)
        charts = generate_watchlist_charts(scores=scores, report_dir=args.report_dir)
        markdown = render_watchlist_markdown(
            scores,
            charts=[(item.title, item.relative_path) for item in charts],
        )
        markdown_path = args.report_dir / f"watchlist-{as_of_date}.md"
        markdown_path.write_text(markdown, encoding="utf-8")

        csv_rows: list[dict[str, object]] = []
        for item in scores:
            csv_rows.append(
                {
                    "symbol": item.symbol,
                    "total_score": item.total_score,
                    "tier": item.tier,
                    "growth": item.factor_scores.growth,
                    "quality": item.factor_scores.quality,
                    "valuation": item.factor_scores.valuation,
                    "momentum": item.factor_scores.momentum,
                    "as_of_date": item.as_of_date,
                    "notes": "；".join(item.notes),
                }
            )

        csv_path = args.report_dir / f"watchlist-{as_of_date}.csv"
        _write_csv(csv_path, rows=csv_rows)

        record_report(
            db_path=db_path,
            symbol="WATCHLIST",
            report_type="watchlist_screener",
            report_date=as_of_date,
            file_path=markdown_path,
        )
        finish_data_job(db_path=db_path, job_id=job_id, status=JOB_SUCCESS)

        print(f"[完成] 观察池报告：{markdown_path}")
        print(f"[完成] 评分明细：{csv_path}")
        print(f"[完成] 报告索引库：{db_path}")
        return 0
    except Exception as exc:  # noqa: BLE001
        finish_data_job(
            db_path=db_path,
            job_id=job_id,
            status=JOB_FAILED,
            error_message=f"{type(exc).__name__}: {exc}",
        )
        raise


if __name__ == "__main__":
    raise SystemExit(main())
