"""观察池评分与排序逻辑。"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any

from fundwise.company_dossier import CompanyDossier


@dataclass(frozen=True, slots=True)
class FactorScores:
    """分项评分结果。"""

    growth: float
    quality: float
    valuation: float
    momentum: float


@dataclass(frozen=True, slots=True)
class WatchlistScore:
    """观察池单标的评分结果。"""

    symbol: str
    total_score: float
    tier: str
    factor_scores: FactorScores
    notes: tuple[str, ...]
    as_of_date: str | None

    def to_dict(self) -> dict[str, Any]:
        """转换为可序列化字典。"""
        data = asdict(self)
        data["notes"] = "；".join(self.notes)
        return data


def score_company_dossier(dossier: CompanyDossier) -> WatchlistScore:
    """对单公司分析卡打分。"""
    growth = _score_growth(dossier)
    quality = _score_quality(dossier)
    valuation = _score_valuation(dossier)
    momentum = _score_momentum(dossier)

    total = (
        growth * 0.35
        + quality * 0.35
        + valuation * 0.15
        + momentum * 0.15
    )
    notes = _build_notes(dossier, growth=growth, quality=quality, momentum=momentum)

    return WatchlistScore(
        symbol=dossier.symbol,
        total_score=round(total, 2),
        tier=_score_to_tier(total),
        factor_scores=FactorScores(
            growth=round(growth, 2),
            quality=round(quality, 2),
            valuation=round(valuation, 2),
            momentum=round(momentum, 2),
        ),
        notes=notes,
        as_of_date=dossier.as_of_date,
    )


def rank_watchlist(scores: list[WatchlistScore]) -> list[WatchlistScore]:
    """按总分降序排列观察池结果。"""
    return sorted(scores, key=lambda item: item.total_score, reverse=True)


def render_watchlist_markdown(
    scores: list[WatchlistScore],
    charts: list[tuple[str, str]] | None = None,
) -> str:
    """渲染观察池评分报告（Markdown）。"""
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ranked = rank_watchlist(scores)

    lines = [
        "# 观察池评分报告",
        "",
        f"- 生成时间：{generated_at}",
        f"- 标的数量：{len(ranked)}",
        "",
        "## 排名总览",
        "",
        "| 排名 | 股票代码 | 总分 | 分层 | 成长 | 质量 | 估值 | 动量 |",
        "| --- | --- | ---: | --- | ---: | ---: | ---: | ---: |",
    ]

    for index, item in enumerate(ranked, start=1):
        lines.append(
            "| "
            f"{index} | {item.symbol} | {item.total_score:.2f} | {item.tier} | "
            f"{item.factor_scores.growth:.2f} | {item.factor_scores.quality:.2f} | "
            f"{item.factor_scores.valuation:.2f} | {item.factor_scores.momentum:.2f} |"
        )

    lines.extend(["", "## 逐标的结论", ""])
    for item in ranked:
        lines.append(f"### {item.symbol}（{item.tier}）")
        lines.append("")
        lines.append(f"- 总分：{item.total_score:.2f}")
        lines.append(f"- 截止日期：{item.as_of_date or '未知'}")
        if item.notes:
            for note in item.notes:
                lines.append(f"- {note}")
        else:
            lines.append("- 暂无可解释结论（数据可能缺失）。")
        lines.append("")

    if charts:
        lines.extend(["## 图表", ""])
        for title, relative_path in charts:
            lines.append(f"### {title}")
            lines.append("")
            lines.append(f"![{title}]({relative_path})")
            lines.append("")

    lines.extend(
        [
            "## 说明",
            "",
            "- 该评分用于研究排序，不构成投资建议。",
            "- 当前为 MVP 规则，后续可按行业与风格做参数分层。",
            "",
        ]
    )
    return "\n".join(lines)


def _score_growth(dossier: CompanyDossier) -> float:
    """计算成长性评分。"""
    revenue_score = _map_growth_rate(dossier.revenue_yoy)
    profit_score = _map_growth_rate(dossier.net_profit_yoy)
    return (revenue_score + profit_score) / 2.0


def _score_quality(dossier: CompanyDossier) -> float:
    """计算经营质量评分。"""
    roe_score = _map_range(dossier.latest_roe, low=0.05, high=0.25)
    ocf_score = _map_range(dossier.ocf_to_profit, low=0.5, high=1.2)
    debt_score = _map_reverse_range(dossier.latest_debt_to_asset, low=0.2, high=0.8)
    return roe_score * 0.45 + ocf_score * 0.35 + debt_score * 0.2


def _score_valuation(dossier: CompanyDossier) -> float:
    """计算估值评分（MVP 先给中性分）。"""
    market_cap = (
        dossier.latest_market_cap_cny
        if dossier.latest_market_cap_cny is not None
        else dossier.latest_market_cap
    )
    net_profit = (
        dossier.latest_net_profit_cny
        if dossier.latest_net_profit_cny is not None
        else dossier.latest_net_profit
    )
    if market_cap is None or net_profit is None:
        return 50.0
    if net_profit <= 0:
        return 30.0
    pseudo_pe = market_cap / net_profit
    return _map_reverse_range(pseudo_pe, low=10.0, high=60.0)


def _score_momentum(dossier: CompanyDossier) -> float:
    """计算价格动量评分。"""
    return _map_range(dossier.price_return_since_start, low=-0.2, high=0.4)


def _map_growth_rate(value: float | None) -> float:
    """将同比增速映射为 0~100 分。"""
    return _map_range(value, low=-0.2, high=0.4)


def _map_range(value: float | None, low: float, high: float) -> float:
    """将区间值线性映射为 0~100 分。"""
    if value is None:
        return 50.0
    if high <= low:
        return 50.0
    ratio = (value - low) / (high - low)
    clipped = min(1.0, max(0.0, ratio))
    return clipped * 100.0


def _map_reverse_range(value: float | None, low: float, high: float) -> float:
    """将区间值反向映射为 0~100 分（数值越低分越高）。"""
    return 100.0 - _map_range(value, low=low, high=high)


def _score_to_tier(total_score: float) -> str:
    """按总分映射分层标签。"""
    if total_score >= 80:
        return "A"
    if total_score >= 65:
        return "B"
    if total_score >= 50:
        return "C"
    return "D"


def _build_notes(
    dossier: CompanyDossier,
    *,
    growth: float,
    quality: float,
    momentum: float,
) -> tuple[str, ...]:
    """生成可读结论说明。"""
    notes: list[str] = []

    if growth >= 70:
        notes.append("成长性较好：营收与净利润增速处于较高区间。")
    elif growth <= 40:
        notes.append("成长性偏弱：营收或净利润增速不足。")

    if quality >= 70:
        notes.append("经营质量较好：ROE、现金流质量或杠杆水平表现良好。")
    elif quality <= 40:
        notes.append("经营质量偏弱：ROE、现金流质量或杠杆有待改善。")

    if momentum >= 65:
        notes.append("股价动量偏强：样本区间内价格趋势向上。")
    elif momentum <= 35:
        notes.append("股价动量偏弱：样本区间内价格趋势偏弱。")

    if dossier.latest_ocf is not None and dossier.latest_net_profit is not None:
        if dossier.latest_net_profit > 0 and dossier.latest_ocf < 0:
            notes.append("风险提示：净利润为正但经营现金流为负，需核查利润含金量。")

    return tuple(notes)
