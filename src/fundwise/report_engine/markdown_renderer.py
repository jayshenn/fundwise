"""Markdown 报告渲染器。"""

from __future__ import annotations

from datetime import datetime

import pandas as pd

from fundwise.company_dossier import CompanyDossier
from fundwise.market_timing_panel import MarketTimingPanel
from fundwise.watchlist_screener import WatchlistScore


def render_company_dossier_markdown(
    dossier: CompanyDossier,
    dataset: dict[str, pd.DataFrame],
    charts: list[tuple[str, str]] | None = None,
) -> str:
    """渲染单公司分析卡 Markdown 文本。"""
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines = [
        f"# {dossier.symbol} 公司分析卡",
        "",
        f"- 生成时间：{generated_at}",
        f"- 数据截止：{dossier.as_of_date or '未知'}",
        f"- 市场：{dossier.market}",
        f"- 币种：{dossier.currency}",
        f"- 汇率（1 {dossier.currency} = ? CNY）：{_fmt_fx_rate(dossier.fx_to_cny)}",
        "",
        "## 核心结论（MVP）",
        "",
        (
            "- 经营质量："
            f"ROE={_fmt_percent(dossier.latest_roe)}，"
            f"资产负债率={_fmt_percent(dossier.latest_debt_to_asset)}。"
        ),
        (
            "- 成长性："
            f"营收同比={_fmt_percent(dossier.revenue_yoy)}，"
            f"净利润同比={_fmt_percent(dossier.net_profit_yoy)}。"
        ),
        (
            "- 现金流："
            f"经营现金流/净利润={_fmt_ratio(dossier.ocf_to_profit)}，"
            f"最新经营现金流={_fmt_number(dossier.latest_ocf)}。"
        ),
        (
            "- 市场表现："
            f"最新收盘价={_fmt_number(dossier.latest_close)}，"
            f"区间涨跌幅={_fmt_percent(dossier.price_return_since_start)}。"
        ),
        "",
        "## 关键指标快照",
        "",
        "| 指标 | 数值 |",
        "| --- | ---: |",
        f"| 最新市值 | {_fmt_number(dossier.latest_market_cap)} |",
        f"| 最新市值（CNY） | {_fmt_number(dossier.latest_market_cap_cny)} |",
        f"| 最新营收 | {_fmt_number(dossier.latest_revenue)} |",
        f"| 最新营收（CNY） | {_fmt_number(dossier.latest_revenue_cny)} |",
        f"| 最新净利润 | {_fmt_number(dossier.latest_net_profit)} |",
        f"| 最新净利润（CNY） | {_fmt_number(dossier.latest_net_profit_cny)} |",
        f"| 最新经营现金流 | {_fmt_number(dossier.latest_ocf)} |",
        f"| 最新经营现金流（CNY） | {_fmt_number(dossier.latest_ocf_cny)} |",
        f"| 最新 ROE | {_fmt_percent(dossier.latest_roe)} |",
        f"| 最新资产负债率 | {_fmt_percent(dossier.latest_debt_to_asset)} |",
        "",
        "## 数据覆盖",
        "",
        "| 数据集 | 行数 |",
        "| --- | ---: |",
    ]

    for dataset_type, frame in dataset.items():
        lines.append(f"| {dataset_type} | {len(frame)} |")

    if charts:
        lines.extend(["", "## 图表", ""])
        for title, relative_path in charts:
            lines.append(f"### {title}")
            lines.append("")
            lines.append(f"![{title}]({relative_path})")
            lines.append("")

    lines.extend(
        [
            "",
            "## 说明",
            "",
            "- 本报告用于投研辅助，不构成投资建议。",
            "- 口径统一规则见 `docs/架构与设计说明.md`。",
            "- 若指标缺失，通常表示上游接口无值或字段待映射。",
            "",
        ]
    )
    return "\n".join(lines)


def render_market_timing_markdown(
    panel: MarketTimingPanel,
    scores: list[WatchlistScore],
    charts: list[tuple[str, str]] | None = None,
) -> str:
    """渲染市场择时面板 Markdown 文本。"""
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ranked = sorted(scores, key=lambda item: item.total_score, reverse=True)
    top_items = ranked[:5]
    bottom_items = list(reversed(ranked[-5:]))

    lines = [
        "# 市场择时面板",
        "",
        f"- 生成时间：{generated_at}",
        f"- 数据截止：{panel.as_of_date or '未知'}",
        f"- 样本数量：{panel.sample_size}",
        "",
        "## 核心判断",
        "",
        f"- 风险偏好温度：{panel.risk_temperature:.2f} / 100",
        f"- 市场状态：{panel.market_state}",
        f"- 建议仓位区间：{panel.suggested_position_range}",
        "",
        "## 因子中位数",
        "",
        "| 因子 | 分值 |",
        "| --- | ---: |",
        f"| 总分中位数 | {panel.median_total_score:.2f} |",
        f"| 成长因子中位数 | {panel.median_growth_score:.2f} |",
        f"| 质量因子中位数 | {panel.median_quality_score:.2f} |",
        f"| 动量因子中位数 | {panel.median_momentum_score:.2f} |",
        (
            "| 市场广度（正收益占比） | "
            f"{_fmt_percent(panel.breadth_positive_ratio)} |"
        ),
        "",
        "## 评分靠前标的",
        "",
        "| 排名 | 代码 | 总分 | 分层 |",
        "| --- | --- | ---: | --- |",
    ]

    for index, item in enumerate(top_items, start=1):
        lines.append(
            f"| {index} | {item.symbol} | {item.total_score:.2f} | {item.tier} |"
        )

    lines.extend(
        [
            "",
            "## 评分靠后标的",
            "",
            "| 排名 | 代码 | 总分 | 分层 |",
            "| --- | --- | ---: | --- |",
        ]
    )
    for index, item in enumerate(bottom_items, start=1):
        lines.append(
            f"| {index} | {item.symbol} | {item.total_score:.2f} | {item.tier} |"
        )

    if charts:
        lines.extend(["", "## 图表", ""])
        for title, relative_path in charts:
            lines.append(f"### {title}")
            lines.append("")
            lines.append(f"![{title}]({relative_path})")
            lines.append("")

    lines.extend(["", "## 风险提示与说明", ""])
    for note in panel.notes:
        lines.append(f"- {note}")
    lines.extend(
        [
            "- 本面板用于投研辅助，不构成投资建议。",
            "- 建议结合行业景气与事件催化做人工复核。",
            "",
        ]
    )
    return "\n".join(lines)


def _fmt_number(value: float | None) -> str:
    """格式化数值。"""
    if value is None:
        return "N/A"
    return f"{value:,.2f}"


def _fmt_percent(value: float | None) -> str:
    """格式化百分比。"""
    if value is None:
        return "N/A"
    return f"{value * 100:.2f}%"


def _fmt_ratio(value: float | None) -> str:
    """格式化比值。"""
    if value is None:
        return "N/A"
    return f"{value:.2f}"


def _fmt_fx_rate(value: float | None) -> str:
    """格式化汇率。"""
    if value is None:
        return "N/A"
    return f"{value:.6f}"
