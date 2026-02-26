"""生成流水线运行历史报告。"""

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))


def parse_args() -> argparse.Namespace:
    """解析命令行参数。"""
    parser = argparse.ArgumentParser(description="生成流水线运行历史 Markdown 报告。")
    parser.add_argument(
        "--db-path",
        type=Path,
        default=PROJECT_ROOT / "data" / "metadata" / "fundwise.db",
        help="SQLite 元数据库路径。",
    )
    parser.add_argument(
        "--job-type",
        default="run_daily_pipeline",
        help="任务类型，默认 run_daily_pipeline。",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=30,
        help="报告中展示的最近任务数量。",
    )
    parser.add_argument(
        "--max-delay-hours",
        type=int,
        default=36,
        help="健康检查允许的最大延迟小时数。",
    )
    parser.add_argument(
        "--report-dir",
        type=Path,
        default=PROJECT_ROOT / "reports" / "ops",
        help="运维报告输出目录。",
    )
    return parser.parse_args()


def main() -> int:
    """执行流水线历史报告生成。"""
    from fundwise.pipeline import evaluate_pipeline_health, render_pipeline_history_markdown
    from fundwise.storage import (
        init_sqlite_metadata_db,
        list_data_jobs,
        record_report,
        upsert_symbol,
    )

    args = parse_args()
    db_path = init_sqlite_metadata_db(args.db_path)
    jobs = list_data_jobs(
        db_path=db_path,
        job_type=args.job_type,
        limit=args.limit,
    )
    health = evaluate_pipeline_health(
        jobs,
        max_delay_hours=args.max_delay_hours,
    )
    markdown = render_pipeline_history_markdown(
        jobs,
        health=health,
        title="日常流水线运行历史",
    )

    args.report_dir.mkdir(parents=True, exist_ok=True)
    report_date = date.today().strftime("%Y-%m-%d")
    output_path = args.report_dir / f"pipeline-history-{report_date}.md"
    output_path.write_text(markdown, encoding="utf-8")

    upsert_symbol(
        db_path=db_path,
        symbol="PIPELINE",
        market="META",
        currency="CNY",
        name="日常流水线",
        is_active=True,
    )
    record_report(
        db_path=db_path,
        symbol="PIPELINE",
        report_type="pipeline_ops",
        report_date=report_date,
        file_path=output_path,
    )

    print(f"[完成] 历史报告：{output_path}")
    print(f"[完成] 健康状态：{'正常' if health.ok else '异常'}（{health.code}）")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
