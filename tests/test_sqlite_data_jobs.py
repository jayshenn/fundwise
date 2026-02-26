"""SQLite 数据任务日志测试。"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from fundwise.storage import (
    JOB_FAILED,
    JOB_SUCCESS,
    finish_data_job,
    init_sqlite_metadata_db,
    list_data_jobs,
    start_data_job,
    upsert_symbol,
)


def _query_job_row(db_path: Path, job_id: int) -> tuple[str, str | None, str | None, str | None]:
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT status, started_at, finished_at, error_message
            FROM data_jobs
            WHERE job_id = ?;
            """,
            (job_id,),
        ).fetchone()
    assert row is not None
    return str(row[0]), row[1], row[2], row[3]


def test_start_and_finish_data_job_success(tmp_path: Path) -> None:
    """任务日志应能记录运行到成功状态。"""
    db_path = init_sqlite_metadata_db(tmp_path / "fundwise.db")
    upsert_symbol(
        db_path=db_path,
        symbol="600519.SH",
        market="CN",
        currency="CNY",
        name="贵州茅台",
        is_active=True,
    )

    job_id = start_data_job(
        db_path=db_path,
        job_type="generate_company_report",
        symbol="600519.SH",
    )
    assert job_id > 0

    finish_data_job(db_path=db_path, job_id=job_id, status=JOB_SUCCESS)
    status, started_at, finished_at, error_message = _query_job_row(db_path, job_id)
    assert status == JOB_SUCCESS
    assert started_at is not None
    assert finished_at is not None
    assert error_message is None


def test_finish_data_job_failed_with_error_message(tmp_path: Path) -> None:
    """失败任务应记录错误信息。"""
    db_path = init_sqlite_metadata_db(tmp_path / "fundwise.db")

    job_id = start_data_job(
        db_path=db_path,
        job_type="generate_watchlist_report",
        symbol=None,
    )

    finish_data_job(
        db_path=db_path,
        job_id=job_id,
        status=JOB_FAILED,
        error_message="RuntimeError: demo",
    )
    status, _, _, error_message = _query_job_row(db_path, job_id)
    assert status == JOB_FAILED
    assert error_message == "RuntimeError: demo"


def test_finish_data_job_rejects_invalid_status(tmp_path: Path) -> None:
    """非法状态应抛出异常。"""
    db_path = init_sqlite_metadata_db(tmp_path / "fundwise.db")
    job_id = start_data_job(db_path=db_path, job_type="demo", symbol=None)

    with pytest.raises(ValueError):
        finish_data_job(db_path=db_path, job_id=job_id, status="done")


def test_list_data_jobs_filters_and_order(tmp_path: Path) -> None:
    """任务查询应支持过滤并按最新优先返回。"""
    db_path = init_sqlite_metadata_db(tmp_path / "fundwise.db")
    upsert_symbol(
        db_path=db_path,
        symbol="600519.SH",
        market="CN",
        currency="CNY",
        name="贵州茅台",
        is_active=True,
    )
    first_id = start_data_job(
        db_path=db_path,
        job_type="run_daily_pipeline",
        symbol="600519.SH",
    )
    second_id = start_data_job(
        db_path=db_path,
        job_type="run_daily_pipeline",
        symbol="600519.SH",
    )
    finish_data_job(db_path=db_path, job_id=first_id, status=JOB_SUCCESS)
    finish_data_job(db_path=db_path, job_id=second_id, status=JOB_FAILED, error_message="demo")

    latest_only = list_data_jobs(
        db_path=db_path,
        job_type="run_daily_pipeline",
        limit=1,
    )
    assert len(latest_only) == 1
    assert latest_only[0]["job_id"] == second_id

    failed_jobs = list_data_jobs(
        db_path=db_path,
        job_type="run_daily_pipeline",
        status=JOB_FAILED,
    )
    assert len(failed_jobs) == 1
    assert failed_jobs[0]["status"] == JOB_FAILED
    assert failed_jobs[0]["error_message"] == "demo"
