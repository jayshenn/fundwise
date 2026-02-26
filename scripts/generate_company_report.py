"""生成单公司 Markdown 分析卡，并写入报告索引。"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    """解析命令行参数。"""
    parser = argparse.ArgumentParser(description="生成单公司分析卡（Markdown）。")
    parser.add_argument("--symbol", required=True, help="标准 symbol，例如 600519.SH 或 00700.HK")
    parser.add_argument("--start-date", default=None, help="行情起始日期（YYYY-MM-DD）")
    parser.add_argument("--end-date", default=None, help="行情结束日期（YYYY-MM-DD）")
    parser.add_argument(
        "--report-dir",
        type=Path,
        default=PROJECT_ROOT / "reports" / "company_dossier",
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
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="日志级别，默认 INFO。",
    )
    return parser.parse_args()


def _configure_logging(level: str) -> None:
    """初始化脚本日志。"""
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def main() -> int:
    """执行报告生成流程。"""
    from fundwise.company_dossier import build_company_dossier
    from fundwise.data_adapter import AkshareDataAdapter
    from fundwise.report_engine import (
        generate_company_dossier_charts,
        render_company_dossier_markdown,
    )
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

    args = parse_args()
    _configure_logging(args.log_level)
    started_at = time.perf_counter()
    logger.info(
        "开始生成单公司报告: symbol=%s, start_date=%s, end_date=%s, strict=%s",
        args.symbol,
        args.start_date or "N/A",
        args.end_date or "N/A",
        not args.non_strict,
    )

    adapter = AkshareDataAdapter(strict=not args.non_strict)
    db_path = init_sqlite_metadata_db(args.db_path)
    logger.info("元数据库已初始化: %s", db_path)
    symbol_info = adapter.get_symbol_info(args.symbol)
    logger.info(
        "symbol 解析完成: input=%s, normalized=%s, market=%s, currency=%s",
        args.symbol,
        symbol_info.symbol,
        symbol_info.market,
        symbol_info.currency,
    )
    upsert_symbol(
        db_path=db_path,
        symbol=symbol_info.symbol,
        market=symbol_info.market,
        currency=symbol_info.currency,
        name=None,
        is_active=True,
    )
    job_id = start_data_job(
        db_path=db_path,
        job_type="generate_company_report",
        symbol=symbol_info.symbol,
    )
    logger.info("任务已登记: job_id=%s", job_id)

    try:
        logger.info("开始构建数据集...")
        dataset = adapter.build_company_dataset(
            symbol=symbol_info.symbol,
            start_date=args.start_date,
            end_date=args.end_date,
        )
        for dataset_type, frame in dataset.items():
            logger.info("数据集完成: %s, rows=%d", dataset_type, len(frame))

        fx_to_cny = resolve_fx_to_cny(
            db_path=db_path,
            currency=symbol_info.currency,
            date=args.end_date,
        )
        logger.info("汇率查询完成: fx_to_cny=%s", fx_to_cny if fx_to_cny is not None else "N/A")
        dossier = build_company_dossier(
            symbol=symbol_info.symbol,
            dataset=dataset,
            fx_to_cny=fx_to_cny,
        )
        logger.info("分析卡构建完成: as_of_date=%s", dossier.as_of_date or "N/A")

        report_date = dossier.as_of_date or "unknown-date"
        symbol_dir = args.report_dir / symbol_info.symbol.replace(".", "_")
        symbol_dir.mkdir(parents=True, exist_ok=True)
        logger.info("图表目录就绪: %s", symbol_dir / "charts")
        charts = generate_company_dossier_charts(
            symbol=symbol_info.symbol,
            dataset=dataset,
            report_dir=symbol_dir,
        )
        logger.info("图表生成完成: count=%d", len(charts))
        markdown = render_company_dossier_markdown(
            dossier=dossier,
            dataset=dataset,
            charts=[(item.title, item.relative_path) for item in charts],
        )
        report_path = symbol_dir / f"company-dossier-{report_date}.md"
        report_path.write_text(markdown, encoding="utf-8")
        logger.info("Markdown 写入完成: %s", report_path)

        record_report(
            db_path=db_path,
            symbol=symbol_info.symbol,
            report_type="company_dossier",
            report_date=report_date,
            file_path=report_path,
        )
        finish_data_job(db_path=db_path, job_id=job_id, status=JOB_SUCCESS)
        logger.info(
            "任务成功: job_id=%s, elapsed=%.2fs",
            job_id,
            time.perf_counter() - started_at,
        )

        print(f"[完成] 报告已生成：{report_path}")
        print(f"[完成] 报告索引库：{db_path}")
        return 0
    except Exception as exc:  # noqa: BLE001
        finish_data_job(
            db_path=db_path,
            job_id=job_id,
            status=JOB_FAILED,
            error_message=f"{type(exc).__name__}: {exc}",
        )
        logger.exception("任务失败: job_id=%s", job_id)
        raise


if __name__ == "__main__":
    raise SystemExit(main())
