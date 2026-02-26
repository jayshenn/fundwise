"""Markdown 报告渲染测试。"""

from __future__ import annotations

import pandas as pd

from fundwise.company_dossier import CompanyDossier
from fundwise.report_engine import render_company_dossier_markdown


def test_render_company_dossier_markdown_contains_key_sections() -> None:
    dossier = CompanyDossier(
        symbol="00700.HK",
        market="HK",
        currency="HKD",
        fx_to_cny=0.91,
        as_of_date="2025-01-31",
        latest_close=320.0,
        latest_market_cap=3_000_000_000.0,
        latest_market_cap_cny=2_730_000_000.0,
        latest_revenue=800.0,
        latest_revenue_cny=728.0,
        latest_net_profit=120.0,
        latest_net_profit_cny=109.2,
        latest_ocf=150.0,
        latest_ocf_cny=136.5,
        latest_roe=0.155,
        latest_debt_to_asset=0.45,
        revenue_yoy=0.1,
        net_profit_yoy=0.08,
        ocf_to_profit=1.25,
        price_return_since_start=0.2,
    )
    dataset = {
        "price_history": pd.DataFrame({"date": ["2025-01-31"]}),
        "market_cap_history": pd.DataFrame({"date": ["2025-01-31"]}),
        "financial_indicators": pd.DataFrame({"date": ["2024-12-31"]}),
    }

    content = render_company_dossier_markdown(
        dossier=dossier,
        dataset=dataset,
        charts=[("收盘价趋势", "charts/demo.png")],
    )

    assert "# 00700.HK 公司分析卡" in content
    assert "## 核心结论（MVP）" in content
    assert "## 关键指标快照" in content
    assert "## 数据覆盖" in content
    assert "## 图表" in content
    assert "![收盘价趋势](charts/demo.png)" in content
    assert "| price_history | 1 |" in content
    assert "ROE=15.50%" in content
    assert "汇率（1 HKD = ? CNY）：0.910000" in content
    assert "| 最新市值（CNY） | 2,730,000,000.00 |" in content
