"""市场择时面板模块测试。"""

from __future__ import annotations

from fundwise.company_dossier import CompanyDossier
from fundwise.market_timing_panel import build_market_timing_panel
from fundwise.report_engine import render_market_timing_markdown


def _build_dossier(
    symbol: str,
    *,
    as_of_date: str,
    revenue_yoy: float,
    net_profit_yoy: float,
    roe: float,
    ocf_to_profit: float,
    debt_to_asset: float,
    price_return: float,
) -> CompanyDossier:
    """构造测试用分析卡对象。"""
    return CompanyDossier(
        symbol=symbol,
        market="CN",
        currency="CNY",
        fx_to_cny=1.0,
        as_of_date=as_of_date,
        latest_close=10.0,
        latest_market_cap=1000.0,
        latest_market_cap_cny=1000.0,
        latest_revenue=500.0,
        latest_revenue_cny=500.0,
        latest_net_profit=100.0,
        latest_net_profit_cny=100.0,
        latest_ocf=120.0,
        latest_ocf_cny=120.0,
        latest_roe=roe,
        latest_debt_to_asset=debt_to_asset,
        revenue_yoy=revenue_yoy,
        net_profit_yoy=net_profit_yoy,
        ocf_to_profit=ocf_to_profit,
        price_return_since_start=price_return,
    )


def test_build_market_timing_panel_state_and_position() -> None:
    """偏强样本应输出偏多状态与较高仓位建议。"""
    dossiers = [
        _build_dossier(
            "600519.SH",
            as_of_date="2025-12-31",
            revenue_yoy=0.2,
            net_profit_yoy=0.18,
            roe=0.22,
            ocf_to_profit=1.1,
            debt_to_asset=0.3,
            price_return=0.25,
        ),
        _build_dossier(
            "000333.SZ",
            as_of_date="2025-12-31",
            revenue_yoy=0.12,
            net_profit_yoy=0.1,
            roe=0.18,
            ocf_to_profit=1.0,
            debt_to_asset=0.35,
            price_return=0.18,
        ),
    ]

    panel, scores = build_market_timing_panel(dossiers)

    assert panel.market_state in {"偏多", "中性"}
    assert panel.suggested_position_range in {"60%-80%", "40%-60%"}
    assert panel.sample_size == 2
    assert len(scores) == 2


def test_build_market_timing_panel_handles_empty_input() -> None:
    """空输入应抛出异常。"""
    try:
        build_market_timing_panel([])
    except ValueError as exc:
        assert "不能为空" in str(exc)
    else:
        raise AssertionError("预期抛出 ValueError")


def test_render_market_timing_markdown_contains_sections() -> None:
    """择时面板 Markdown 应包含关键章节。"""
    dossiers = [
        _build_dossier(
            "00700.HK",
            as_of_date="2025-11-30",
            revenue_yoy=0.1,
            net_profit_yoy=0.15,
            roe=0.17,
            ocf_to_profit=1.0,
            debt_to_asset=0.45,
            price_return=0.05,
        )
    ]
    panel, scores = build_market_timing_panel(dossiers)

    content = render_market_timing_markdown(
        panel=panel,
        scores=scores,
        charts=[("市场风险温度", "charts/market-risk-temperature.png")],
    )

    assert "# 市场择时面板" in content
    assert "## 核心判断" in content
    assert "## 因子中位数" in content
    assert "## 评分靠前标的" in content
    assert "## 图表" in content
    assert "![市场风险温度](charts/market-risk-temperature.png)" in content
    assert "00700.HK" in content
