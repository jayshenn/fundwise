"""图表渲染模块测试。"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from fundwise.market_timing_panel import MarketTimingPanel
from fundwise.report_engine import (
    generate_company_dossier_charts,
    generate_market_timing_charts,
    generate_watchlist_charts,
)
from fundwise.report_engine.chart_renderer import _prepare_pe_series
from fundwise.watchlist_screener import FactorScores, WatchlistScore


def test_generate_company_dossier_charts(tmp_path: Path) -> None:
    """单公司图表生成应产出 PNG 文件。"""
    dataset = {
        "market_cap_history": pd.DataFrame(
            {
                "date": [
                    "2022-03-31",
                    "2022-06-30",
                    "2022-09-30",
                    "2022-12-31",
                    "2023-03-31",
                    "2023-06-30",
                    "2023-09-30",
                    "2023-12-31",
                    "2024-03-31",
                    "2024-06-30",
                    "2024-09-30",
                    "2024-12-31",
                ],
                "market_cap": [
                    1800.0,
                    1850.0,
                    1880.0,
                    1920.0,
                    1980.0,
                    2050.0,
                    2120.0,
                    2200.0,
                    2280.0,
                    2360.0,
                    2420.0,
                    2500.0,
                ],
            }
        ),
        "financial_indicators": pd.DataFrame(
            {
                "date": ["2022-12-31", "2023-12-31", "2024-12-31"],
                "revenue": [950.0, 1100.0, 1260.0],
                "net_profit": [320.0, 365.0, 410.0],
                "ocf": [330.0, 390.0, 450.0],
                "total_assets": [1400.0, 1600.0, 1850.0],
                "total_liabilities": [420.0, 450.0, 510.0],
                "debt_to_asset": [0.30, 0.28125, 0.275676],
                "roe": [22.0, 24.0, 25.0],
            }
        ),
        "financial_indicators_quarterly": pd.DataFrame(
            {
                "date": [
                    "2023-03-31",
                    "2023-06-30",
                    "2023-09-30",
                    "2023-12-31",
                    "2024-03-31",
                    "2024-06-30",
                    "2024-09-30",
                    "2024-12-31",
                ],
                "revenue": [250.0, 510.0, 780.0, 1100.0, 285.0, 575.0, 900.0, 1260.0],
                "net_profit": [80.0, 165.0, 255.0, 365.0, 92.0, 188.0, 295.0, 410.0],
                "ocf": [75.0, 180.0, 285.0, 390.0, 86.0, 205.0, 328.0, 450.0],
                "total_assets": [1500.0, 1520.0, 1550.0, 1600.0, 1680.0, 1740.0, 1800.0, 1850.0],
                "total_liabilities": [430.0, 435.0, 442.0, 450.0, 468.0, 482.0, 496.0, 510.0],
                "debt_to_asset": [0.2867, 0.2862, 0.2851, 0.2813, 0.2786, 0.2770, 0.2756, 0.2757],
                "roe": [23.0, 24.0, 24.5, 25.0, 24.0, 24.5, 24.8, 25.0],
            }
        ),
        "pe_history": pd.DataFrame(
            {
                "date": [
                    "2019-12-31",
                    "2020-12-31",
                    "2021-12-31",
                    "2022-12-31",
                    "2023-12-31",
                    "2024-12-31",
                ],
                "pe": [28.0, 34.0, 49.0, 41.0, 37.0, 39.0],
            }
        ),
        "industry_pe_history": pd.DataFrame(
            {
                "date": ["2019-12-31", "2020-12-31", "2021-12-31", "2022-12-31", "2023-12-31"],
                "industry_pe": [30.0, 33.0, 45.0, 38.0, 35.0],
            }
        ),
        "hs300_pe_history": pd.DataFrame(
            {
                "date": ["2019-12-31", "2020-12-31", "2021-12-31", "2022-12-31", "2023-12-31"],
                "hs300_pe": [12.0, 14.0, 16.0, 13.5, 12.8],
            }
        ),
        "industry_roe_history": pd.DataFrame(
            {
                "date": ["2023-03-31", "2023-06-30", "2023-09-30", "2023-12-31", "2024-12-31"],
                "industry_roe": [15.0, 15.8, 16.2, 16.5, 16.8],
            }
        ),
        "balance_components": pd.DataFrame(
            {
                "date": ["2024-12-31"] * 14,
                "category": [
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
                ],
                "side": ["asset"] * 8 + ["liability"] * 6,
                "value": [
                    300.0,
                    5.0,
                    22.0,
                    46.0,
                    60.0,
                    180.0,
                    24.0,
                    1213.0,
                    75.0,
                    240.0,
                    33.0,
                    40.0,
                    25.0,
                    97.0,
                ],
            }
        ),
    }

    artifacts = generate_company_dossier_charts(
        symbol="600519.SH",
        dataset=dataset,
        report_dir=tmp_path,
    )

    expected_suffixes = {
        "market-cap-revenue.png",
        "revenue-ocf.png",
        "profit-ocf.png",
        "pe-compare.png",
        "pe-normal-band.png",
        "roe-compare.png",
        "balance-structure.png",
        "balance-trend.png",
    }
    assert len(artifacts) == 8
    for suffix in expected_suffixes:
        assert any(item.relative_path.endswith(suffix) for item in artifacts)
    for item in artifacts:
        assert item.absolute_path.exists()


def test_generate_watchlist_charts(tmp_path: Path) -> None:
    """观察池图表生成应产出总分与因子图。"""
    scores = [
        WatchlistScore(
            symbol="600519.SH",
            total_score=78.0,
            tier="B",
            factor_scores=FactorScores(growth=75.0, quality=80.0, valuation=60.0, momentum=55.0),
            notes=("样例",),
            as_of_date="2025-12-31",
        ),
        WatchlistScore(
            symbol="00700.HK",
            total_score=66.0,
            tier="B",
            factor_scores=FactorScores(growth=70.0, quality=62.0, valuation=58.0, momentum=50.0),
            notes=("样例",),
            as_of_date="2025-12-31",
        ),
    ]

    artifacts = generate_watchlist_charts(scores=scores, report_dir=tmp_path)

    assert len(artifacts) == 2
    for item in artifacts:
        assert item.absolute_path.exists()


def test_generate_market_timing_charts(tmp_path: Path) -> None:
    """市场择时图表生成应产出温度与分布图。"""
    panel = MarketTimingPanel(
        as_of_date="2025-12-31",
        sample_size=2,
        risk_temperature=58.0,
        market_state="中性",
        suggested_position_range="40%-60%",
        breadth_positive_ratio=0.5,
        median_total_score=60.0,
        median_growth_score=62.0,
        median_quality_score=58.0,
        median_momentum_score=45.0,
        notes=("样例",),
    )
    scores = [
        WatchlistScore(
            symbol="600519.SH",
            total_score=64.0,
            tier="C",
            factor_scores=FactorScores(growth=65.0, quality=60.0, valuation=55.0, momentum=45.0),
            notes=("样例",),
            as_of_date="2025-12-31",
        ),
        WatchlistScore(
            symbol="00700.HK",
            total_score=56.0,
            tier="C",
            factor_scores=FactorScores(growth=58.0, quality=57.0, valuation=50.0, momentum=40.0),
            notes=("样例",),
            as_of_date="2025-12-31",
        ),
    ]

    artifacts = generate_market_timing_charts(panel=panel, scores=scores, report_dir=tmp_path)

    assert len(artifacts) == 3
    for item in artifacts:
        assert item.absolute_path.exists()


def test_prepare_pe_series_normalizes_market_cap_unit() -> None:
    """当市值明显是亿元口径时应自动换算出合理 PE。"""
    market_cap = pd.DataFrame(
        {
            "date": ["2026-01-01", "2026-01-02", "2026-01-03"],
            "market_cap": [1200.0, 1300.0, 1400.0],
        }
    )
    financial = pd.DataFrame(
        {
            "date": ["2025-12-31"],
            "net_profit": [2_000_000_000.0],
            "pe_ttm": [pd.NA],
        }
    )

    pe_data = _prepare_pe_series(
        market_cap_frame=market_cap,
        financial_frame=financial,
    )
    assert not pe_data.empty
    latest_pe = float(pe_data["pe"].iloc[-1])
    assert 60.0 <= latest_pe <= 80.0
