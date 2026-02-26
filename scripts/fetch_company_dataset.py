"""拉取单公司数据并写入本地快照索引。"""

from __future__ import annotations

import argparse
import hashlib
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    """解析命令行参数。"""
    parser = argparse.ArgumentParser(description="拉取单公司标准化数据并保存。")
    parser.add_argument("--symbol", required=True, help="标准 symbol，例如 600519.SH 或 00700.HK")
    parser.add_argument("--start-date", default=None, help="行情起始日期（YYYY-MM-DD）")
    parser.add_argument("--end-date", default=None, help="行情结束日期（YYYY-MM-DD）")
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=PROJECT_ROOT / "data" / "normalized",
        help="数据输出目录",
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


def _latest_as_of_date(data: pd.DataFrame) -> str:
    """获取数据快照日期（优先使用最新 date 列）。"""
    if "date" in data.columns and not data.empty:
        parsed = pd.to_datetime(data["date"], errors="coerce").dropna()
        if not parsed.empty:
            return parsed.max().strftime("%Y-%m-%d")
    return datetime.now().strftime("%Y-%m-%d")


def _save_frame(data: pd.DataFrame, file_base: Path) -> Path:
    """优先保存为 parquet；不可用时回退 CSV。"""
    parquet_path = file_base.with_suffix(".parquet")
    try:
        data.to_parquet(parquet_path, index=False)
        return parquet_path
    except Exception:  # noqa: BLE001
        csv_path = file_base.with_suffix(".csv")
        data.to_csv(csv_path, index=False)
        return csv_path


def _file_checksum(path: Path) -> str:
    """计算文件 SHA256 校验值。"""
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(8192), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> int:
    """执行单公司数据拉取与快照索引写入。"""
    from fundwise.data_adapter.akshare_adapter import AkshareDataAdapter
    from fundwise.storage.sqlite_store import (
        JOB_FAILED,
        JOB_SUCCESS,
        finish_data_job,
        init_sqlite_metadata_db,
        record_dataset_snapshot,
        start_data_job,
        upsert_symbol,
    )

    args = parse_args()
    _configure_logging(args.log_level)
    started_at = time.perf_counter()
    logger.info(
        "开始拉取单公司数据: symbol=%s, start_date=%s, end_date=%s, strict=%s",
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
        job_type="fetch_company_dataset",
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

        symbol_dir = args.out_dir / symbol_info.symbol.replace(".", "_")
        symbol_dir.mkdir(parents=True, exist_ok=True)

        for dataset_type, frame in dataset.items():
            logger.info("数据集完成: %s, rows=%d", dataset_type, len(frame))
            as_of_date = _latest_as_of_date(frame)
            file_base = symbol_dir / f"{dataset_type}-{as_of_date}"
            saved_path = _save_frame(frame, file_base=file_base)
            checksum = _file_checksum(saved_path)
            record_dataset_snapshot(
                db_path=db_path,
                symbol=symbol_info.symbol,
                dataset_type=dataset_type,
                as_of_date=as_of_date,
                file_path=saved_path,
                row_count=len(frame),
                checksum=checksum,
            )
            print(f"[完成] {dataset_type}: 行数={len(frame)} -> {saved_path}")

        finish_data_job(db_path=db_path, job_id=job_id, status=JOB_SUCCESS)
        logger.info(
            "任务成功: job_id=%s, elapsed=%.2fs",
            job_id,
            time.perf_counter() - started_at,
        )
        print(f"已完成，索引数据库：{db_path}")
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
