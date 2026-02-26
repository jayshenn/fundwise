"""单公司分析卡构建测试。"""

from __future__ import annotations

import pandas as pd

from fundwise.company_dossier import build_company_dossier


def test_build_company_dossier_basic_metrics() -> None:
    dataset = {
        "price_history": pd.DataFrame(
            {
                "symbol": ["600519.SH", "600519.SH"],
                "date": ["2024-01-02", "2024-01-03"],
                "close": [100.0, 110.0],
                "market": ["CN", "CN"],
                "currency": ["CNY", "CNY"],
            }
        ),
        "market_cap_history": pd.DataFrame(
            {
                "symbol": ["600519.SH"],
                "date": ["2024-01-03"],
                "market_cap": [2_000_000_000.0],
                "market": ["CN"],
                "currency": ["CNY"],
            }
        ),
        "financial_indicators": pd.DataFrame(
            {
                "symbol": ["600519.SH", "600519.SH"],
                "date": ["2023-12-31", "2024-12-31"],
                "revenue": [1000.0, 1200.0],
                "net_profit": [200.0, 250.0],
                "ocf": [180.0, 260.0],
                "roe": [0.18, 0.20],
                "debt_to_asset": [0.30, 0.32],
                "market": ["CN", "CN"],
                "currency": ["CNY", "CNY"],
            }
        ),
    }

    dossier = build_company_dossier(symbol="600519.SH", dataset=dataset)

    assert dossier.symbol == "600519.SH"
    assert dossier.as_of_date == "2024-12-31"
    assert dossier.fx_to_cny == 1.0
    assert dossier.latest_close == 110.0
    assert dossier.latest_market_cap == 2_000_000_000.0
    assert dossier.latest_market_cap_cny == 2_000_000_000.0
    assert dossier.latest_revenue_cny == 1200.0
    assert dossier.latest_net_profit_cny == 250.0
    assert dossier.latest_ocf_cny == 260.0
    assert round(dossier.revenue_yoy or 0, 6) == 0.2
    assert round(dossier.net_profit_yoy or 0, 6) == 0.25
    assert round(dossier.ocf_to_profit or 0, 6) == 1.04
    assert round(dossier.price_return_since_start or 0, 6) == 0.1


def test_build_company_dossier_fx_conversion() -> None:
    """非 CNY 币种应按传入汇率计算 CNY 折算值。"""
    dataset = {
        "price_history": pd.DataFrame(
            {
                "symbol": ["00700.HK"],
                "date": ["2024-01-03"],
                "close": [300.0],
                "market": ["HK"],
                "currency": ["HKD"],
            }
        ),
        "market_cap_history": pd.DataFrame(
            {
                "symbol": ["00700.HK"],
                "date": ["2024-01-03"],
                "market_cap": [3_000_000_000.0],
                "market": ["HK"],
                "currency": ["HKD"],
            }
        ),
        "financial_indicators": pd.DataFrame(
            {
                "symbol": ["00700.HK", "00700.HK"],
                "date": ["2023-12-31", "2024-12-31"],
                "revenue": [900.0, 1000.0],
                "net_profit": [180.0, 200.0],
                "ocf": [190.0, 220.0],
                "roe": [0.16, 0.17],
                "debt_to_asset": [0.45, 0.43],
                "market": ["HK", "HK"],
                "currency": ["HKD", "HKD"],
            }
        ),
    }

    dossier = build_company_dossier(symbol="00700.HK", dataset=dataset, fx_to_cny=0.92)

    assert dossier.currency == "HKD"
    assert dossier.fx_to_cny == 0.92
    assert dossier.latest_market_cap_cny == 2_760_000_000.0
    assert dossier.latest_revenue_cny == 920.0
    assert dossier.latest_net_profit_cny == 184.0
    assert dossier.latest_ocf_cny == 202.4
