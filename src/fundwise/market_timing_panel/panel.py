"""市场择时面板计算逻辑。"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from statistics import median
from typing import Any, Literal

from fundwise.company_dossier import CompanyDossier
from fundwise.watchlist_screener import WatchlistScore, score_company_dossier

MarketState = Literal["偏多", "中性", "偏谨慎"]


@dataclass(frozen=True, slots=True)
class MarketTimingPanel:
    """市场状态与仓位建议结果。"""

    as_of_date: str | None
    sample_size: int
    risk_temperature: float
    market_state: MarketState
    suggested_position_range: str
    breadth_positive_ratio: float | None
    median_total_score: float
    median_growth_score: float
    median_quality_score: float
    median_momentum_score: float
    notes: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        """转换为可序列化字典。"""
        data = asdict(self)
        data["notes"] = "；".join(self.notes)
        return data


def build_market_timing_panel(
    dossiers: list[CompanyDossier],
) -> tuple[MarketTimingPanel, list[WatchlistScore]]:
    """根据观察池公司分析卡构建市场择时面板。"""
    if not dossiers:
        raise ValueError("dossiers 不能为空")

    scores = [score_company_dossier(item) for item in dossiers]
    total_scores = [item.total_score for item in scores]
    growth_scores = [item.factor_scores.growth for item in scores]
    quality_scores = [item.factor_scores.quality for item in scores]
    momentum_scores = [item.factor_scores.momentum for item in scores]

    breadth = _compute_breadth_positive_ratio(dossiers)
    base_temperature = _safe_median(total_scores)
    adjusted_temperature = _adjust_temperature_by_breadth(base_temperature, breadth)

    market_state = _temperature_to_state(adjusted_temperature)
    position_range = _state_to_position(market_state)
    notes = _build_notes(
        market_state=market_state,
        breadth=breadth,
        growth_median=_safe_median(growth_scores),
        quality_median=_safe_median(quality_scores),
        momentum_median=_safe_median(momentum_scores),
    )

    panel = MarketTimingPanel(
        as_of_date=_latest_as_of_date(dossiers),
        sample_size=len(dossiers),
        risk_temperature=round(adjusted_temperature, 2),
        market_state=market_state,
        suggested_position_range=position_range,
        breadth_positive_ratio=breadth,
        median_total_score=round(_safe_median(total_scores), 2),
        median_growth_score=round(_safe_median(growth_scores), 2),
        median_quality_score=round(_safe_median(quality_scores), 2),
        median_momentum_score=round(_safe_median(momentum_scores), 2),
        notes=notes,
    )
    return panel, scores


def _safe_median(values: list[float]) -> float:
    """返回中位数；空列表返回 50。"""
    if not values:
        return 50.0
    return float(median(values))


def _compute_breadth_positive_ratio(dossiers: list[CompanyDossier]) -> float | None:
    """计算样本中区间收益为正的比例。"""
    valid_returns = [
        item.price_return_since_start
        for item in dossiers
        if item.price_return_since_start is not None
    ]
    if not valid_returns:
        return None
    positive_count = len([value for value in valid_returns if value > 0])
    return positive_count / len(valid_returns)


def _adjust_temperature_by_breadth(base: float, breadth: float | None) -> float:
    """按市场广度对温度做轻量修正。"""
    if breadth is None:
        return _clip_score(base)
    adjustment = (breadth - 0.5) * 20.0
    return _clip_score(base + adjustment)


def _clip_score(value: float) -> float:
    """将分值裁剪到 0~100 区间。"""
    return max(0.0, min(100.0, value))


def _temperature_to_state(temperature: float) -> MarketState:
    """根据风险温度映射市场状态。"""
    if temperature >= 65:
        return "偏多"
    if temperature >= 45:
        return "中性"
    return "偏谨慎"


def _state_to_position(state: MarketState) -> str:
    """根据市场状态给出仓位区间建议。"""
    if state == "偏多":
        return "60%-80%"
    if state == "中性":
        return "40%-60%"
    return "20%-40%"


def _latest_as_of_date(dossiers: list[CompanyDossier]) -> str | None:
    """获取分析卡中的最新截止日期。"""
    valid_dates = [item.as_of_date for item in dossiers if item.as_of_date is not None]
    if not valid_dates:
        return None
    return max(valid_dates)


def _build_notes(
    *,
    market_state: MarketState,
    breadth: float | None,
    growth_median: float,
    quality_median: float,
    momentum_median: float,
) -> tuple[str, ...]:
    """生成面板结论说明。"""
    notes: list[str] = []

    if market_state == "偏多":
        notes.append("风险偏好较高，可在控制回撤前提下提升仓位。")
    elif market_state == "偏谨慎":
        notes.append("风险偏好偏低，建议以防守和现金管理为主。")
    else:
        notes.append("市场处于中性区间，建议均衡配置并跟踪拐点。")

    if breadth is not None:
        notes.append(f"市场广度（区间收益为正占比）为 {breadth * 100:.2f}%。")
    else:
        notes.append("样本缺少可用价格区间收益，广度指标暂不可用。")

    if growth_median >= 65:
        notes.append("样本成长因子整体较强。")
    elif growth_median <= 45:
        notes.append("样本成长因子整体偏弱，需关注业绩兑现风险。")

    if quality_median <= 45:
        notes.append("样本经营质量偏弱，建议提高财务安全边际要求。")

    if momentum_median <= 40:
        notes.append("短期动量偏弱，择时上不宜激进追高。")

    return tuple(notes)
