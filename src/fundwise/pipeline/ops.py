"""流水线运维辅助能力。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta


@dataclass(frozen=True, slots=True)
class PipelineHealth:
    """流水线健康状态。"""

    ok: bool
    code: str
    message: str
    checked_at: str
    latest_job_id: int | None
    latest_status: str | None
    latest_started_at: str | None
    stale_hours: float | None


def evaluate_pipeline_health(
    jobs: list[dict[str, str | int | None]],
    *,
    max_delay_hours: int = 36,
    now: datetime | None = None,
) -> PipelineHealth:
    """根据最新任务日志评估流水线健康状态。"""
    if max_delay_hours <= 0:
        raise ValueError("max_delay_hours 必须大于 0")

    current = now or datetime.now()
    checked_at = current.strftime("%Y-%m-%d %H:%M:%S")
    if not jobs:
        return PipelineHealth(
            ok=False,
            code="no_runs",
            message="未找到流水线运行记录。",
            checked_at=checked_at,
            latest_job_id=None,
            latest_status=None,
            latest_started_at=None,
            stale_hours=None,
        )

    latest = jobs[0]
    latest_job_id = _safe_int(latest.get("job_id"))
    latest_status = _safe_str(latest.get("status"))
    latest_started_at = _safe_str(latest.get("started_at"))
    started_dt = _parse_sqlite_datetime(latest_started_at)
    stale_hours = _calc_stale_hours(current=current, started_at=started_dt)

    if latest_status == "failed":
        return PipelineHealth(
            ok=False,
            code="latest_failed",
            message="最新一次流水线执行失败，请检查错误日志。",
            checked_at=checked_at,
            latest_job_id=latest_job_id,
            latest_status=latest_status,
            latest_started_at=latest_started_at,
            stale_hours=stale_hours,
        )

    if latest_status == "running":
        overdue = started_dt is not None and current - started_dt > timedelta(hours=max_delay_hours)
        if overdue:
            return PipelineHealth(
                ok=False,
                code="running_timeout",
                message="流水线处于运行中且已超时，疑似卡死。",
                checked_at=checked_at,
                latest_job_id=latest_job_id,
                latest_status=latest_status,
                latest_started_at=latest_started_at,
                stale_hours=stale_hours,
            )
        return PipelineHealth(
            ok=True,
            code="running",
            message="流水线正在执行中。",
            checked_at=checked_at,
            latest_job_id=latest_job_id,
            latest_status=latest_status,
            latest_started_at=latest_started_at,
            stale_hours=stale_hours,
        )

    if latest_status != "success":
        return PipelineHealth(
            ok=False,
            code="unknown_status",
            message=f"最新任务状态异常：{latest_status or 'UNKNOWN'}",
            checked_at=checked_at,
            latest_job_id=latest_job_id,
            latest_status=latest_status,
            latest_started_at=latest_started_at,
            stale_hours=stale_hours,
        )

    if started_dt is None:
        return PipelineHealth(
            ok=False,
            code="missing_time",
            message="最新成功任务缺少开始时间，无法评估新鲜度。",
            checked_at=checked_at,
            latest_job_id=latest_job_id,
            latest_status=latest_status,
            latest_started_at=latest_started_at,
            stale_hours=None,
        )

    if current - started_dt > timedelta(hours=max_delay_hours):
        return PipelineHealth(
            ok=False,
            code="stale",
            message="流水线成功记录已过期，请确认定时任务是否按期执行。",
            checked_at=checked_at,
            latest_job_id=latest_job_id,
            latest_status=latest_status,
            latest_started_at=latest_started_at,
            stale_hours=stale_hours,
        )

    return PipelineHealth(
        ok=True,
        code="ok",
        message="流水线健康，最新任务成功且在有效时窗内。",
        checked_at=checked_at,
        latest_job_id=latest_job_id,
        latest_status=latest_status,
        latest_started_at=latest_started_at,
        stale_hours=stale_hours,
    )


def render_pipeline_history_markdown(
    jobs: list[dict[str, str | int | None]],
    *,
    health: PipelineHealth | None = None,
    title: str = "流水线运行历史",
) -> str:
    """渲染流水线运行历史 Markdown。"""
    lines = [f"# {title}", ""]
    latest_job_id = health.latest_job_id if health is not None else None
    latest_job_text = str(latest_job_id) if latest_job_id is not None else "N/A"
    if health is not None:
        lines.extend(
            [
                "## 健康状态",
                "",
                f"- 检查时间：{health.checked_at}",
                f"- 状态：{'正常' if health.ok else '异常'}（{health.code}）",
                f"- 说明：{health.message}",
                f"- 最新任务 ID：{latest_job_text}",
                f"- 最新状态：{health.latest_status or 'N/A'}",
                f"- 最新开始时间：{health.latest_started_at or 'N/A'}",
                (
                    f"- 距今小时数：{health.stale_hours:.2f}"
                    if health.stale_hours is not None
                    else "- 距今小时数：N/A"
                ),
                "",
            ]
        )

    lines.extend(
        [
            "## 最近任务",
            "",
            "| job_id | job_type | symbol | status | started_at | finished_at | error_message |",
            "| ---: | --- | --- | --- | --- | --- | --- |",
        ]
    )
    for item in jobs:
        lines.append(
            "| "
            f"{_safe_int(item.get('job_id')) or 'N/A'} | "
            f"{_safe_str(item.get('job_type')) or 'N/A'} | "
            f"{_safe_str(item.get('symbol')) or 'N/A'} | "
            f"{_safe_str(item.get('status')) or 'N/A'} | "
            f"{_safe_str(item.get('started_at')) or 'N/A'} | "
            f"{_safe_str(item.get('finished_at')) or 'N/A'} | "
            f"{_safe_str(item.get('error_message')) or ''} |"
        )
    if not jobs:
        lines.append("| N/A | N/A | N/A | N/A | N/A | N/A | N/A |")
    lines.append("")
    return "\n".join(lines)


def _parse_sqlite_datetime(value: str | None) -> datetime | None:
    """解析 SQLite 时间字符串。"""
    if value is None:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None


def _calc_stale_hours(*, current: datetime, started_at: datetime | None) -> float | None:
    """计算距今小时数。"""
    if started_at is None:
        return None
    diff = current - started_at
    return diff.total_seconds() / 3600.0


def _safe_str(value: str | int | None) -> str | None:
    """安全转换为字符串。"""
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _safe_int(value: str | int | None) -> int | None:
    """安全转换为整数。"""
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
