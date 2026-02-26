"""报告图表渲染模块。"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import cast

import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.figure import Figure

from fundwise.market_timing_panel import MarketTimingPanel
from fundwise.watchlist_screener import WatchlistScore

plt.switch_backend("Agg")
plt.rcParams["font.sans-serif"] = [
    "PingFang SC",
    "Hiragino Sans GB",
    "Microsoft YaHei",
    "SimHei",
    "Arial Unicode MS",
    "DejaVu Sans",
]
plt.rcParams["axes.unicode_minus"] = False


@dataclass(frozen=True, slots=True)
class ChartArtifact:
    """图表产物描述。"""

    title: str
    relative_path: str
    absolute_path: Path


def generate_company_dossier_charts(
    symbol: str,
    dataset: dict[str, pd.DataFrame],
    report_dir: Path,
) -> list[ChartArtifact]:
    """生成单公司报告图表（8 张）。"""
    safe_symbol = symbol.replace(".", "_")
    charts_dir = report_dir / "charts"
    charts_dir.mkdir(parents=True, exist_ok=True)

    annual_financial = dataset.get("financial_indicators", pd.DataFrame())
    trend_financial = _pick_trend_financial_frame(
        quarterly=dataset.get("financial_indicators_quarterly", pd.DataFrame()),
        annual=annual_financial,
    )

    artifacts: list[ChartArtifact] = []
    artifacts.extend(
        _plot_market_cap_vs_revenue(
            symbol=symbol,
            market_cap_frame=dataset.get("market_cap_history", pd.DataFrame()),
            financial_frame=trend_financial,
            charts_dir=charts_dir,
            file_prefix=safe_symbol,
        )
    )
    artifacts.extend(
        _plot_revenue_vs_ocf(
            symbol=symbol,
            financial_frame=trend_financial,
            charts_dir=charts_dir,
            file_prefix=safe_symbol,
        )
    )
    artifacts.extend(
        _plot_profit_vs_ocf(
            symbol=symbol,
            financial_frame=trend_financial,
            charts_dir=charts_dir,
            file_prefix=safe_symbol,
        )
    )
    artifacts.extend(
        _plot_pe_compare(
            symbol=symbol,
            pe_frame=dataset.get("pe_history", pd.DataFrame()),
            industry_pe_frame=dataset.get("industry_pe_history", pd.DataFrame()),
            hs300_pe_frame=dataset.get("hs300_pe_history", pd.DataFrame()),
            market_cap_frame=dataset.get("market_cap_history", pd.DataFrame()),
            financial_frame=annual_financial,
            charts_dir=charts_dir,
            file_prefix=safe_symbol,
        )
    )
    artifacts.extend(
        _plot_pe_sigma_band(
            symbol=symbol,
            pe_frame=dataset.get("pe_history", pd.DataFrame()),
            market_cap_frame=dataset.get("market_cap_history", pd.DataFrame()),
            financial_frame=annual_financial,
            charts_dir=charts_dir,
            file_prefix=safe_symbol,
        )
    )
    artifacts.extend(
        _plot_roe_compare(
            symbol=symbol,
            financial_frame=trend_financial,
            industry_roe_frame=dataset.get("industry_roe_history", pd.DataFrame()),
            charts_dir=charts_dir,
            file_prefix=safe_symbol,
        )
    )
    artifacts.extend(
        _plot_balance_structure(
            symbol=symbol,
            frame=dataset.get("balance_components", pd.DataFrame()),
            charts_dir=charts_dir,
            file_prefix=safe_symbol,
        )
    )
    artifacts.extend(
        _plot_balance_trend(
            symbol=symbol,
            frame=trend_financial,
            charts_dir=charts_dir,
            file_prefix=safe_symbol,
        )
    )
    return artifacts


def generate_watchlist_charts(
    scores: list[WatchlistScore],
    report_dir: Path,
) -> list[ChartArtifact]:
    """生成观察池评分报告图表。"""
    charts_dir = report_dir / "charts"
    charts_dir.mkdir(parents=True, exist_ok=True)
    ranked = sorted(scores, key=lambda item: item.total_score, reverse=True)

    artifacts: list[ChartArtifact] = []
    if ranked:
        figure, axis = plt.subplots(figsize=(10, 4.8))
        symbols = [item.symbol for item in ranked]
        values = [item.total_score for item in ranked]
        colors = [_tier_color(item.tier) for item in ranked]
        axis.bar(symbols, values, color=colors, width=0.62, edgecolor="white", linewidth=0.9)
        axis.set_title("观察池总分排序")
        axis.set_ylabel("总分")
        axis.set_ylim(0, 100)
        axis.tick_params(axis="x", rotation=25)
        axis.grid(axis="y", alpha=0.22, linestyle="--")
        artifacts.append(
            _save_figure(
                figure=figure,
                charts_dir=charts_dir,
                file_name="watchlist-total-score.png",
                title="观察池总分排序",
            )
        )

        medians = {
            "成长": _median_or_default([item.factor_scores.growth for item in ranked]),
            "质量": _median_or_default([item.factor_scores.quality for item in ranked]),
            "估值": _median_or_default([item.factor_scores.valuation for item in ranked]),
            "动量": _median_or_default([item.factor_scores.momentum for item in ranked]),
        }
        figure2, axis2 = plt.subplots(figsize=(7.5, 4.8))
        axis2.bar(
            list(medians.keys()),
            list(medians.values()),
            color="#4C72B0",
            width=0.55,
            edgecolor="white",
            linewidth=0.9,
        )
        axis2.set_title("观察池因子中位数")
        axis2.set_ylabel("分值")
        axis2.set_ylim(0, 100)
        axis2.grid(axis="y", alpha=0.22, linestyle="--")
        artifacts.append(
            _save_figure(
                figure=figure2,
                charts_dir=charts_dir,
                file_name="watchlist-factor-median.png",
                title="观察池因子中位数",
            )
        )

    return artifacts


def generate_market_timing_charts(
    panel: MarketTimingPanel,
    scores: list[WatchlistScore],
    report_dir: Path,
) -> list[ChartArtifact]:
    """生成市场择时面板图表。"""
    charts_dir = report_dir / "charts"
    charts_dir.mkdir(parents=True, exist_ok=True)

    artifacts: list[ChartArtifact] = []

    figure, axis = plt.subplots(figsize=(8, 2.4))
    axis.barh(["风险温度"], [panel.risk_temperature], color="#DD8452")
    axis.set_xlim(0, 100)
    axis.set_xlabel("0-100")
    axis.set_title("市场风险温度")
    axis.grid(axis="x", alpha=0.3)
    artifacts.append(
        _save_figure(
            figure=figure,
            charts_dir=charts_dir,
            file_name="market-risk-temperature.png",
            title="市场风险温度",
        )
    )

    tier_counts = {"A": 0, "B": 0, "C": 0, "D": 0}
    for item in scores:
        tier_counts[item.tier] = tier_counts.get(item.tier, 0) + 1
    figure2, axis2 = plt.subplots(figsize=(7, 4))
    axis2.bar(
        list(tier_counts.keys()),
        list(tier_counts.values()),
        color="#55A868",
        width=0.55,
        edgecolor="white",
        linewidth=0.9,
    )
    axis2.set_title("样本分层分布")
    axis2.set_ylabel("数量")
    axis2.grid(axis="y", alpha=0.22, linestyle="--")
    artifacts.append(
        _save_figure(
            figure=figure2,
            charts_dir=charts_dir,
            file_name="market-tier-distribution.png",
            title="样本分层分布",
        )
    )

    figure3, axis3 = plt.subplots(figsize=(7.5, 4.8))
    labels = ["成长", "质量", "动量"]
    values = [
        panel.median_growth_score,
        panel.median_quality_score,
        panel.median_momentum_score,
    ]
    axis3.bar(labels, values, color="#4C72B0", width=0.58, edgecolor="white", linewidth=0.9)
    axis3.set_title("市场面板因子中位数")
    axis3.set_ylabel("分值")
    axis3.set_ylim(0, 100)
    axis3.grid(axis="y", alpha=0.22, linestyle="--")
    artifacts.append(
        _save_figure(
            figure=figure3,
            charts_dir=charts_dir,
            file_name="market-factor-median.png",
            title="市场面板因子中位数",
        )
    )

    return artifacts


def _plot_market_cap_vs_revenue(
    *,
    symbol: str,
    market_cap_frame: pd.DataFrame,
    financial_frame: pd.DataFrame,
    charts_dir: Path,
    file_prefix: str,
) -> list[ChartArtifact]:
    """绘制总市值与营收双轴趋势。"""
    revenue_data = _prepare_multi_series(financial_frame, ["revenue"])
    if revenue_data.empty:
        return []
    revenue_data = cast(
        pd.DataFrame,
        revenue_data.loc[
            pd.to_numeric(revenue_data["revenue"], errors="coerce").notna(), ["date", "revenue"]
        ],
    )
    if revenue_data.empty:
        return []

    cap_series = _prepare_series(market_cap_frame, value_column="market_cap")
    if cap_series.empty:
        return []
    cap_data = cap_series.rename(columns={"value": "market_cap"})

    merged = pd.merge_asof(
        revenue_data.sort_values(by="date"),
        cap_data.sort_values(by="date"),
        on="date",
        direction="backward",
    )
    merged = cast(
        pd.DataFrame,
        merged.dropna(subset=["revenue", "market_cap"])
        .sort_values(by="date")
        .reset_index(drop=True),
    )
    if merged.empty:
        return []

    revenue_factor, revenue_unit = _guess_scale(cast(pd.Series, merged["revenue"]))
    cap_factor, cap_unit = _guess_scale(cast(pd.Series, merged["market_cap"]))

    figure, axis = plt.subplots(figsize=(10, 4.8))
    x_positions = list(range(len(merged)))
    axis.bar(
        x_positions,
        merged["revenue"] / revenue_factor,
        color="#4C72B0",
        width=0.58,
        alpha=0.9,
        label="总营收",
        edgecolor="white",
        linewidth=0.7,
    )
    axis.set_ylabel(f"总营收（{revenue_unit}）")
    axis.grid(axis="y", alpha=0.2, linestyle="--")

    axis2 = axis.twinx()
    axis2.plot(
        x_positions,
        merged["market_cap"] / cap_factor,
        color="#C44E52",
        linewidth=2.0,
        label="总市值",
    )
    axis2.set_ylabel(f"总市值（{cap_unit}）")
    axis.set_title(f"{symbol} 市值与营收增长趋势")

    date_labels = _format_date_labels(cast(pd.Series, merged["date"]))
    tick_step = _get_tick_step(len(date_labels))
    axis.set_xticks(x_positions[::tick_step])
    axis.set_xticklabels(date_labels[::tick_step], rotation=30, ha="right")

    handles1, labels1 = axis.get_legend_handles_labels()
    handles2, labels2 = axis2.get_legend_handles_labels()
    axis.legend(handles1 + handles2, labels1 + labels2, loc="upper left")

    return [
        _save_figure(
            figure=figure,
            charts_dir=charts_dir,
            file_name=f"{file_prefix}-market-cap-revenue.png",
            title="市值与营收增长趋势",
        )
    ]


def _plot_revenue_vs_ocf(
    *,
    symbol: str,
    financial_frame: pd.DataFrame,
    charts_dir: Path,
    file_prefix: str,
) -> list[ChartArtifact]:
    """绘制主营收与经营现金流趋势。"""
    data = _prepare_multi_series(financial_frame, ["revenue", "ocf"])
    if data.empty:
        return []
    if pd.to_numeric(data["revenue"], errors="coerce").dropna().empty:
        return []
    if pd.to_numeric(data["ocf"], errors="coerce").dropna().empty:
        return []

    metric_frame = cast(pd.DataFrame, data.loc[:, ["revenue", "ocf"]])
    factor, unit = _guess_scale_from_frame(metric_frame)

    figure, axis = plt.subplots(figsize=(10, 4.8))
    axis.plot(
        data["date"], data["revenue"] / factor, color="#4C72B0", linewidth=1.9, label="主营收"
    )
    axis.plot(
        data["date"], data["ocf"] / factor, color="#C44E52", linewidth=1.9, label="主业现金流"
    )
    axis.set_title(f"{symbol} 主营收与主业现金流趋势")
    axis.set_ylabel(f"金额（{unit}）")
    axis.grid(alpha=0.26)
    axis.legend(loc="upper left")

    return [
        _save_figure(
            figure=figure,
            charts_dir=charts_dir,
            file_name=f"{file_prefix}-revenue-ocf.png",
            title="主营收与主业现金流趋势",
        )
    ]


def _plot_profit_vs_ocf(
    *,
    symbol: str,
    financial_frame: pd.DataFrame,
    charts_dir: Path,
    file_prefix: str,
) -> list[ChartArtifact]:
    """绘制净利润与经营现金流趋势。"""
    data = _prepare_multi_series(financial_frame, ["net_profit", "ocf"])
    if data.empty:
        return []
    if pd.to_numeric(data["net_profit"], errors="coerce").dropna().empty:
        return []
    if pd.to_numeric(data["ocf"], errors="coerce").dropna().empty:
        return []

    metric_frame = cast(pd.DataFrame, data.loc[:, ["net_profit", "ocf"]])
    factor, unit = _guess_scale_from_frame(metric_frame)

    figure, axis = plt.subplots(figsize=(10, 4.8))
    axis.plot(
        data["date"], data["net_profit"] / factor, color="#4C72B0", linewidth=1.9, label="净利润"
    )
    axis.plot(
        data["date"], data["ocf"] / factor, color="#C44E52", linewidth=1.9, label="现金流净额"
    )
    axis.set_title(f"{symbol} 净利润与主业现金流净额趋势")
    axis.set_ylabel(f"金额（{unit}）")
    axis.grid(alpha=0.26)
    axis.legend(loc="upper left")

    return [
        _save_figure(
            figure=figure,
            charts_dir=charts_dir,
            file_name=f"{file_prefix}-profit-ocf.png",
            title="净利润与主业现金流净额趋势",
        )
    ]


def _plot_pe_compare(
    *,
    symbol: str,
    pe_frame: pd.DataFrame,
    industry_pe_frame: pd.DataFrame,
    hs300_pe_frame: pd.DataFrame,
    market_cap_frame: pd.DataFrame,
    financial_frame: pd.DataFrame,
    charts_dir: Path,
    file_prefix: str,
) -> list[ChartArtifact]:
    """绘制公司 PE、行业 PE 与沪深300 PE 对比图。"""
    company_pe = _prepare_series(pe_frame, value_column="pe")
    if company_pe.empty:
        fallback = _prepare_pe_series(
            market_cap_frame=market_cap_frame, financial_frame=financial_frame
        )
        company_pe = fallback.rename(columns={"pe": "value"})
    if company_pe.empty:
        return []

    figure, axis = plt.subplots(figsize=(10, 4.8))
    axis.plot(
        company_pe["date"],
        company_pe["value"],
        color="#4C72B0",
        linewidth=1.8,
        label="公司市盈率",
    )

    company_dates = cast(pd.Series, company_pe["date"])

    industry = _prepare_series(industry_pe_frame, value_column="industry_pe")
    if not industry.empty:
        aligned_industry = _align_series_to_dates(industry, company_dates)
        if not aligned_industry.empty:
            axis.plot(
                company_dates,
                aligned_industry,
                color="#C44E52",
                linewidth=1.7,
                label="行业市盈率",
            )

    hs300 = _prepare_series(hs300_pe_frame, value_column="hs300_pe")
    if not hs300.empty:
        aligned_hs300 = _align_series_to_dates(hs300, company_dates)
        if not aligned_hs300.empty:
            axis.plot(
                company_dates,
                aligned_hs300,
                color="#55A868",
                linewidth=1.6,
                label="沪深300市盈率",
            )

    axis.set_title(f"{symbol} 市盈率趋势")
    axis.set_ylabel("PE")
    axis.ticklabel_format(style="plain", axis="y", useOffset=False)
    axis.grid(axis="y", alpha=0.24, linestyle="--")
    axis.legend(loc="upper left")

    return [
        _save_figure(
            figure=figure,
            charts_dir=charts_dir,
            file_name=f"{file_prefix}-pe-compare.png",
            title="市盈率对比趋势",
        )
    ]


def _plot_pe_sigma_band(
    *,
    symbol: str,
    pe_frame: pd.DataFrame,
    market_cap_frame: pd.DataFrame,
    financial_frame: pd.DataFrame,
    charts_dir: Path,
    file_prefix: str,
) -> list[ChartArtifact]:
    """绘制 PE 均值与标准差区间图。"""
    company_pe = _prepare_series(pe_frame, value_column="pe")
    if company_pe.empty:
        fallback = _prepare_pe_series(
            market_cap_frame=market_cap_frame, financial_frame=financial_frame
        )
        company_pe = fallback.rename(columns={"pe": "value"})
    if company_pe.empty:
        return []

    latest_date = cast(pd.Timestamp, company_pe["date"].max())
    window_start = latest_date - pd.DateOffset(years=8)
    windowed = cast(pd.DataFrame, company_pe.loc[company_pe["date"] >= window_start, :])
    stats_source = windowed if len(windowed) >= 20 else company_pe

    pe_series = cast(pd.Series, pd.to_numeric(stats_source["value"], errors="coerce").dropna())
    if pe_series.empty:
        return []
    mean_pe = float(pe_series.mean())
    std_pe = float(pe_series.std(ddof=0))
    high_line = mean_pe + std_pe
    low_line = max(mean_pe - std_pe, 0.0)

    figure, axis = plt.subplots(figsize=(10, 4.8))
    axis.plot(
        company_pe["date"], company_pe["value"], color="#4C72B0", linewidth=1.7, label="公司市盈率"
    )
    axis.axhline(
        mean_pe, color="#C9A227", linestyle="--", linewidth=1.5, label=f"均值({mean_pe:.2f})"
    )
    axis.axhline(
        high_line, color="#C44E52", linestyle="--", linewidth=1.5, label=f"高估线({high_line:.2f})"
    )
    axis.axhline(
        low_line, color="#55A868", linestyle="--", linewidth=1.5, label=f"低估线({low_line:.2f})"
    )
    axis.set_title(f"{symbol} 市盈率均值与标准差区间")
    axis.set_ylabel("PE")
    axis.ticklabel_format(style="plain", axis="y", useOffset=False)
    axis.grid(axis="y", alpha=0.24, linestyle="--")
    axis.legend(loc="upper left", fontsize=9)

    return [
        _save_figure(
            figure=figure,
            charts_dir=charts_dir,
            file_name=f"{file_prefix}-pe-normal-band.png",
            title="PE均值与标准差区间",
        )
    ]


def _plot_roe_compare(
    *,
    symbol: str,
    financial_frame: pd.DataFrame,
    industry_roe_frame: pd.DataFrame,
    charts_dir: Path,
    file_prefix: str,
) -> list[ChartArtifact]:
    """绘制公司 ROE 与行业 ROE 趋势。"""
    company = _prepare_series(financial_frame, value_column="roe")
    if company.empty:
        return []
    company_values = _normalize_ratio_to_percent(cast(pd.Series, company["value"]))
    company = cast(pd.DataFrame, company.assign(value=company_values))

    figure, axis = plt.subplots(figsize=(10, 4.8))
    axis.plot(company["date"], company["value"], color="#4C72B0", linewidth=1.8, label="公司ROE")

    industry = _prepare_series(industry_roe_frame, value_column="industry_roe")
    if not industry.empty:
        industry_values = _normalize_ratio_to_percent(cast(pd.Series, industry["value"]))
        industry = cast(pd.DataFrame, industry.assign(value=industry_values))
        aligned_industry = _align_series_to_dates(industry, cast(pd.Series, company["date"]))
        if not aligned_industry.empty:
            axis.plot(
                company["date"],
                aligned_industry,
                color="#C44E52",
                linewidth=1.8,
                label="行业ROE",
            )

    axis.set_title(f"{symbol} 净资产收益率趋势")
    axis.set_ylabel("ROE(%)")
    axis.grid(axis="y", alpha=0.24, linestyle="--")
    axis.legend(loc="upper left")

    return [
        _save_figure(
            figure=figure,
            charts_dir=charts_dir,
            file_name=f"{file_prefix}-roe-compare.png",
            title="ROE对比趋势",
        )
    ]


def _plot_balance_structure(
    *,
    symbol: str,
    frame: pd.DataFrame,
    charts_dir: Path,
    file_prefix: str,
) -> list[ChartArtifact]:
    """绘制单期资产负债结构图。"""
    if frame.empty:
        return []
    required = {"date", "category", "side", "value"}
    if not required.issubset(set(frame.columns)):
        return []

    working = frame.copy()
    working["date"] = pd.to_datetime(working["date"], errors="coerce")
    working["value"] = pd.to_numeric(working["value"], errors="coerce")
    working = cast(pd.DataFrame, working.dropna(subset=["date", "value", "category"]))
    if working.empty:
        return []

    latest_date = cast(pd.Timestamp, working["date"].max())
    snapshot = cast(pd.DataFrame, working.loc[working["date"] == latest_date, :])
    if snapshot.empty:
        return []

    category_order = [
        "现金",
        "应收款",
        "预付款",
        "存货",
        "长期投资",
        "固定资产",
        "无形资产/商誉",
        "其他资产",
        "应付账款",
        "合同负债/预收款",
        "应付职工薪酬",
        "应交税费",
        "有息负债",
        "其他负债",
    ]
    snapshot["category"] = pd.Categorical(
        snapshot["category"], categories=category_order, ordered=True
    )
    snapshot = cast(pd.DataFrame, snapshot.sort_values(by="category"))

    factor, unit = _guess_scale(cast(pd.Series, snapshot["value"]))
    colors = ["#4C72B0" if side == "asset" else "#C44E52" for side in snapshot["side"]]

    figure, axis = plt.subplots(figsize=(11.2, 5.0))
    axis.bar(
        snapshot["category"].astype(str),
        snapshot["value"] / factor,
        color=colors,
        width=0.62,
        edgecolor="white",
        linewidth=0.8,
    )
    axis.set_title(f"{symbol} 资产负债表结构（{latest_date.strftime('%Y-%m-%d')}）")
    axis.set_ylabel(f"金额（{unit}）")
    axis.grid(axis="y", alpha=0.22, linestyle="--")
    axis.tick_params(axis="x", rotation=30)

    return [
        _save_figure(
            figure=figure,
            charts_dir=charts_dir,
            file_name=f"{file_prefix}-balance-structure.png",
            title="资产负债结构（单期）",
        )
    ]


def _plot_balance_trend(
    *,
    symbol: str,
    frame: pd.DataFrame,
    charts_dir: Path,
    file_prefix: str,
) -> list[ChartArtifact]:
    """绘制总资产、总负债与资产负债率趋势图。"""
    data = _prepare_multi_series(frame, ["total_assets", "total_liabilities", "debt_to_asset"])
    if data.empty:
        return []

    balance_frame = cast(pd.DataFrame, data.loc[:, ["total_assets", "total_liabilities"]])
    if pd.to_numeric(balance_frame["total_assets"], errors="coerce").dropna().empty:
        return []
    if pd.to_numeric(balance_frame["total_liabilities"], errors="coerce").dropna().empty:
        return []

    debt_ratio = _normalize_ratio_to_percent(cast(pd.Series, data["debt_to_asset"]))
    data = cast(pd.DataFrame, data.assign(debt_to_asset=debt_ratio / 100.0))

    factor, unit = _guess_scale_from_frame(balance_frame)
    figure, axis = plt.subplots(figsize=(10, 4.8))
    x_positions = list(range(len(data)))
    bar_width = 0.40
    date_labels = _format_date_labels(cast(pd.Series, data["date"]))
    axis.bar(
        [value - bar_width / 2 for value in x_positions],
        data["total_assets"] / factor,
        width=bar_width,
        label="总资产",
        alpha=0.84,
        color="#4C72B0",
        edgecolor="white",
        linewidth=0.7,
    )
    axis.bar(
        [value + bar_width / 2 for value in x_positions],
        data["total_liabilities"] / factor,
        width=bar_width,
        label="总负债",
        alpha=0.88,
        color="#E41A1C",
        edgecolor="white",
        linewidth=0.7,
    )
    axis.set_ylabel(f"金额（{unit}）")
    axis.set_title(f"{symbol} 资产负债率趋势")
    axis.grid(axis="y", alpha=0.22, linestyle="--")
    tick_step = _get_tick_step(len(date_labels))
    tick_positions = x_positions[::tick_step]
    tick_labels = date_labels[::tick_step]
    axis.set_xticks(tick_positions)
    axis.set_xticklabels(tick_labels, rotation=30, ha="right")

    axis2 = axis.twinx()
    axis2.plot(
        x_positions,
        data["debt_to_asset"] * 100,
        color="#1B9E77",
        linewidth=1.8,
        label="资产负债率",
    )
    axis2.set_ylabel("资产负债率(%)")
    axis2.set_xlim(-0.8, len(x_positions) - 0.2)

    handles1, labels1 = axis.get_legend_handles_labels()
    handles2, labels2 = axis2.get_legend_handles_labels()
    axis.legend(handles1 + handles2, labels1 + labels2, loc="upper left")

    return [
        _save_figure(
            figure=figure,
            charts_dir=charts_dir,
            file_name=f"{file_prefix}-balance-trend.png",
            title="资产负债率趋势",
        )
    ]


def _pick_trend_financial_frame(quarterly: pd.DataFrame, annual: pd.DataFrame) -> pd.DataFrame:
    """趋势图优先使用季度财务，缺失则回退年度。"""
    quarterly_prepared = _prepare_multi_series(
        quarterly,
        [
            "revenue",
            "net_profit",
            "ocf",
            "roe",
            "total_assets",
            "total_liabilities",
            "debt_to_asset",
        ],
    )
    if not quarterly_prepared.empty:
        return quarterly
    return annual


def _align_series_to_dates(series: pd.DataFrame, target_dates: pd.Series) -> pd.Series:
    """将序列按目标日期对齐并前向填充。"""
    if series.empty:
        return pd.Series(dtype="float64")
    indexed = series.copy().set_index("date")["value"]
    aligned = indexed.reindex(target_dates, method="ffill")
    return cast(pd.Series, aligned)


def _normalize_ratio_to_percent(series: pd.Series) -> pd.Series:
    """将比率序列统一转换为百分比值。"""
    numeric = pd.to_numeric(series, errors="coerce")
    clean = cast(pd.Series, numeric.dropna())
    if clean.empty:
        return pd.Series(numeric, index=series.index)
    median_abs = float(clean.abs().median())
    if median_abs <= 1.2:
        numeric = numeric * 100
    return pd.Series(numeric, index=series.index)


def _prepare_series(frame: pd.DataFrame, value_column: str) -> pd.DataFrame:
    """准备单序列时间数据。"""
    if frame.empty or "date" not in frame.columns or value_column not in frame.columns:
        return pd.DataFrame(columns=["date", "value"])

    data = pd.DataFrame(
        {
            "date": pd.to_datetime(frame["date"], errors="coerce"),
            "value": pd.to_numeric(frame[value_column], errors="coerce"),
        }
    )
    data = data.dropna(subset=["date", "value"]).sort_values(by="date")
    data = cast(pd.DataFrame, data.loc[data["value"] > 0, :])
    return data


def _prepare_multi_series(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """准备多序列时间数据。"""
    if frame.empty or "date" not in frame.columns:
        return pd.DataFrame(columns=["date", *columns])

    working = pd.DataFrame({"date": pd.to_datetime(frame["date"], errors="coerce")})
    for column in columns:
        if column in frame.columns:
            working[column] = pd.to_numeric(frame[column], errors="coerce")
        else:
            working[column] = pd.NA

    cleaned = working.dropna(subset=["date"]).sort_values(by="date")
    has_any_value = False
    for column in columns:
        if pd.Series(cleaned[column]).notna().any():
            has_any_value = True
            break
    if not has_any_value:
        return pd.DataFrame(columns=["date", *columns])
    return cleaned


def _prepare_pe_series(
    market_cap_frame: pd.DataFrame,
    financial_frame: pd.DataFrame,
) -> pd.DataFrame:
    """准备 PE 时间序列，优先真实 pe_ttm，缺失时退化为市值/净利润估算。"""
    if not financial_frame.empty and {"date", "pe_ttm"}.issubset(set(financial_frame.columns)):
        pe_data = pd.DataFrame(
            {
                "date": pd.to_datetime(financial_frame["date"], errors="coerce"),
                "pe": pd.to_numeric(financial_frame["pe_ttm"], errors="coerce"),
            }
        )
        pe_data = pe_data.dropna(subset=["date", "pe"]).sort_values(by="date")
        pe_data = cast(pd.DataFrame, pe_data.loc[pe_data["pe"] > 0, :])
        if len(pe_data) >= 3:
            return pe_data

    if market_cap_frame.empty or "date" not in market_cap_frame.columns:
        return pd.DataFrame(columns=["date", "pe"])
    if "net_profit" not in financial_frame.columns:
        return pd.DataFrame(columns=["date", "pe"])

    net_profit_source = pd.Series(financial_frame["net_profit"], index=financial_frame.index)
    net_profit_numeric = pd.Series(
        pd.to_numeric(net_profit_source, errors="coerce"),
        index=net_profit_source.index,
    )
    net_profit_series = cast(pd.Series, net_profit_numeric.dropna())
    if net_profit_series.empty:
        return pd.DataFrame(columns=["date", "pe"])
    latest_net_profit = float(net_profit_series.iloc[-1])
    if latest_net_profit <= 0:
        return pd.DataFrame(columns=["date", "pe"])

    market_cap_numeric = pd.Series(
        pd.to_numeric(market_cap_frame["market_cap"], errors="coerce"),
        index=market_cap_frame.index,
    )
    market_cap_normalized = _normalize_market_cap_for_pe(
        market_cap=market_cap_numeric,
        latest_net_profit=latest_net_profit,
    )
    result = pd.DataFrame(
        {
            "date": pd.to_datetime(market_cap_frame["date"], errors="coerce"),
            "pe": market_cap_normalized / latest_net_profit,
        }
    )
    result = result.dropna(subset=["date", "pe"]).sort_values(by="date")
    result = cast(pd.DataFrame, result.loc[result["pe"] > 0, :])
    return result


def _normalize_market_cap_for_pe(
    market_cap: pd.Series,
    latest_net_profit: float,
) -> pd.Series:
    """对市值序列做单位归一，尽量让估算 PE 回到合理量级。"""
    raw_cap = pd.Series(pd.to_numeric(market_cap, errors="coerce"), index=market_cap.index)
    raw_pe = pd.Series(raw_cap / latest_net_profit, index=raw_cap.index)
    clean_pe = cast(pd.Series, raw_pe.dropna())
    if clean_pe.empty:
        return raw_cap

    median_pe = float(clean_pe.median())
    if median_pe < 0.01:
        return raw_cap * 100_000_000.0
    if median_pe > 1_000_000:
        return raw_cap / 100_000_000.0
    return raw_cap


def _save_figure(
    *,
    figure: Figure,
    charts_dir: Path,
    file_name: str,
    title: str,
) -> ChartArtifact:
    """保存图像文件并返回图表描述。"""
    output_path = charts_dir / file_name
    figure.tight_layout()
    figure.savefig(output_path, dpi=170)
    plt.close(figure)
    return ChartArtifact(
        title=title,
        relative_path=f"charts/{file_name}",
        absolute_path=output_path,
    )


def _tier_color(tier: str) -> str:
    """按分层返回柱状图颜色。"""
    color_map = {
        "A": "#2ca02c",
        "B": "#1f77b4",
        "C": "#ff7f0e",
        "D": "#d62728",
    }
    return color_map.get(tier, "#7f7f7f")


def _median_or_default(values: list[float], default: float = 50.0) -> float:
    """返回中位数；空列表返回默认值。"""
    if not values:
        return default
    return float(pd.Series(values).median())


def _guess_scale(series: pd.Series) -> tuple[float, str]:
    """根据数值量级推断展示单位。"""
    source = pd.Series(series, index=series.index)
    numeric = pd.Series(pd.to_numeric(source, errors="coerce"), index=source.index)
    clean = cast(pd.Series, numeric.dropna())
    if clean.empty:
        return 1.0, "元"
    max_value = float(clean.max())
    if max_value >= 100_000_000:
        return 100_000_000.0, "亿元"
    if max_value >= 10_000:
        return 10_000.0, "万元"
    return 1.0, "元"


def _guess_scale_from_frame(frame: pd.DataFrame) -> tuple[float, str]:
    """根据多列数据量级推断展示单位。"""
    max_value = 0.0
    for column in frame.columns:
        source = pd.Series(frame[column], index=frame.index)
        numeric = pd.Series(pd.to_numeric(source, errors="coerce"), index=source.index)
        clean = cast(pd.Series, numeric.dropna())
        if clean.empty:
            continue
        max_value = max(max_value, float(clean.max()))
    if max_value >= 100_000_000:
        return 100_000_000.0, "亿元"
    if max_value >= 10_000:
        return 10_000.0, "万元"
    return 1.0, "元"


def _get_tick_step(length: int) -> int:
    """根据点数返回横轴标签抽样步长。"""
    if length <= 8:
        return 1
    if length <= 16:
        return 2
    if length <= 28:
        return 3
    return 4


def _format_date_labels(values: pd.Series) -> list[str]:
    """将时间序列格式化为横轴标签。"""
    result: list[str] = []
    converted = pd.to_datetime(values, errors="coerce")
    for value in converted:
        if pd.isna(value):
            result.append("")
        else:
            result.append(value.strftime("%Y-%m"))
    return result
