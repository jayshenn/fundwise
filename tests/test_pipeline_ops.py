"""流水线运维模块测试。"""

from __future__ import annotations

from datetime import datetime

from fundwise.pipeline import evaluate_pipeline_health, render_pipeline_history_markdown


def test_evaluate_pipeline_health_no_runs() -> None:
    """无运行记录时应返回 no_runs。"""
    health = evaluate_pipeline_health([], max_delay_hours=36, now=datetime(2026, 2, 26, 10, 0, 0))
    assert health.ok is False
    assert health.code == "no_runs"


def test_evaluate_pipeline_health_recent_success() -> None:
    """最近成功记录应判定为健康。"""
    jobs = [
        {
            "job_id": 10,
            "job_type": "run_daily_pipeline",
            "symbol": "PIPELINE",
            "status": "success",
            "started_at": "2026-02-26 08:00:00",
            "finished_at": "2026-02-26 08:05:00",
            "error_message": None,
            "created_at": "2026-02-26 08:00:00",
        }
    ]
    health = evaluate_pipeline_health(
        jobs,
        max_delay_hours=36,
        now=datetime(2026, 2, 26, 10, 0, 0),
    )
    assert health.ok is True
    assert health.code == "ok"


def test_evaluate_pipeline_health_stale_success() -> None:
    """过期成功记录应判定为 stale。"""
    jobs = [
        {
            "job_id": 9,
            "job_type": "run_daily_pipeline",
            "symbol": "PIPELINE",
            "status": "success",
            "started_at": "2026-02-23 01:00:00",
            "finished_at": "2026-02-23 01:05:00",
            "error_message": None,
            "created_at": "2026-02-23 01:00:00",
        }
    ]
    health = evaluate_pipeline_health(
        jobs,
        max_delay_hours=36,
        now=datetime(2026, 2, 26, 10, 0, 0),
    )
    assert health.ok is False
    assert health.code == "stale"


def test_evaluate_pipeline_health_latest_failed() -> None:
    """最新失败记录应判定异常。"""
    jobs = [
        {
            "job_id": 11,
            "job_type": "run_daily_pipeline",
            "symbol": "PIPELINE",
            "status": "failed",
            "started_at": "2026-02-26 08:00:00",
            "finished_at": "2026-02-26 08:01:00",
            "error_message": "demo",
            "created_at": "2026-02-26 08:00:00",
        }
    ]
    health = evaluate_pipeline_health(
        jobs,
        max_delay_hours=36,
        now=datetime(2026, 2, 26, 10, 0, 0),
    )
    assert health.ok is False
    assert health.code == "latest_failed"


def test_render_pipeline_history_markdown_contains_table() -> None:
    """历史渲染应包含健康章节和任务表。"""
    jobs = [
        {
            "job_id": 11,
            "job_type": "run_daily_pipeline",
            "symbol": "PIPELINE",
            "status": "failed",
            "started_at": "2026-02-26 08:00:00",
            "finished_at": "2026-02-26 08:01:00",
            "error_message": "demo",
            "created_at": "2026-02-26 08:00:00",
        }
    ]
    health = evaluate_pipeline_health(
        jobs,
        max_delay_hours=36,
        now=datetime(2026, 2, 26, 10, 0, 0),
    )
    content = render_pipeline_history_markdown(jobs, health=health, title="测试历史")
    assert "# 测试历史" in content
    assert "## 健康状态" in content
    assert "## 最近任务" in content
    assert "| job_id | job_type | symbol | status |" in content
