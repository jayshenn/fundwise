"""检查日常流水线健康状态。"""

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
    parser = argparse.ArgumentParser(description="检查日常流水线最近一次执行状态。")
    parser.add_argument(
        "--db-path",
        type=Path,
        default=PROJECT_ROOT / "data" / "metadata" / "fundwise.db",
        help="SQLite 元数据库路径。",
    )
    parser.add_argument(
        "--job-type",
        default="run_daily_pipeline",
        help="待检查的任务类型，默认 run_daily_pipeline。",
    )
    parser.add_argument(
        "--max-delay-hours",
        type=int,
        default=36,
        help="距离最近执行的最大允许小时数。",
    )
    return parser.parse_args()


def main() -> int:
    """执行健康检查并返回退出码。"""
    from fundwise.pipeline import evaluate_pipeline_health
    from fundwise.storage import init_sqlite_metadata_db, list_data_jobs

    args = parse_args()
    db_path = init_sqlite_metadata_db(args.db_path)
    jobs = list_data_jobs(
        db_path=db_path,
        job_type=args.job_type,
        limit=1,
    )
    health = evaluate_pipeline_health(
        jobs,
        max_delay_hours=args.max_delay_hours,
    )

    print(f"[检查] 时间：{health.checked_at}")
    print(f"[检查] 状态：{'正常' if health.ok else '异常'}（{health.code}）")
    print(f"[检查] 说明：{health.message}")
    latest_job_id = health.latest_job_id if health.latest_job_id is not None else "N/A"
    print(f"[检查] 最新任务 ID：{latest_job_id}")
    print(f"[检查] 最新状态：{health.latest_status or 'N/A'}")
    print(f"[检查] 最新开始时间：{health.latest_started_at or 'N/A'}")
    if health.stale_hours is not None:
        print(f"[检查] 距今小时数：{health.stale_hours:.2f}")

    return 0 if health.ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
