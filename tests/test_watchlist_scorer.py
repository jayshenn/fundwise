"""观察池评分模块测试。"""

from __future__ import annotations

from fundwise.company_dossier import CompanyDossier
from fundwise.watchlist_screener import (
    rank_watchlist,
    render_watchlist_markdown,
    score_company_dossier,
)


def _build_dossier(
    symbol: str,
    *,
    revenue_yoy: float | None,
    net_profit_yoy: float | None,
    roe: float | None,
    ocf_to_profit: float | None,
    debt_to_asset: float | None,
    price_return: float | None,
) -> CompanyDossier:
    """构造测试用分析卡对象。"""
    return CompanyDossier(
        symbol=symbol,
        market="CN",
        currency="CNY",
        fx_to_cny=1.0,
        as_of_date="2025-12-31",
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


def test_score_company_dossier_high_quality() -> None:
    """高质量样本应得到较高评分。"""
    dossier = _build_dossier(
        "600519.SH",
        revenue_yoy=0.22,
        net_profit_yoy=0.25,
        roe=0.24,
        ocf_to_profit=1.2,
        debt_to_asset=0.25,
        price_return=0.15,
    )

    score = score_company_dossier(dossier)

    assert score.total_score >= 70
    assert score.tier in {"A", "B"}
    assert score.factor_scores.growth > 60
    assert score.factor_scores.quality > 70


def test_rank_watchlist_descending() -> None:
    """排序结果应按总分降序。"""
    low = score_company_dossier(
        _build_dossier(
            "000001.SZ",
            revenue_yoy=-0.1,
            net_profit_yoy=-0.15,
            roe=0.06,
            ocf_to_profit=0.5,
            debt_to_asset=0.75,
            price_return=-0.1,
        )
    )
    high = score_company_dossier(
        _build_dossier(
            "00700.HK",
            revenue_yoy=0.18,
            net_profit_yoy=0.2,
            roe=0.2,
            ocf_to_profit=1.1,
            debt_to_asset=0.35,
            price_return=0.2,
        )
    )

    ranked = rank_watchlist([low, high])

    assert ranked[0].symbol == "00700.HK"
    assert ranked[1].symbol == "000001.SZ"


def test_render_watchlist_markdown_sections() -> None:
    """报告渲染应包含核心结构。"""
    score = score_company_dossier(
        _build_dossier(
            "600519.SH",
            revenue_yoy=0.1,
            net_profit_yoy=0.12,
            roe=0.18,
            ocf_to_profit=1.05,
            debt_to_asset=0.3,
            price_return=0.08,
        )
    )

    content = render_watchlist_markdown(
        [score],
        charts=[("观察池总分排序", "charts/watchlist-total-score.png")],
    )

    assert "# 观察池评分报告" in content
    assert "## 排名总览" in content
    assert "## 逐标的结论" in content
    assert "## 图表" in content
    assert "![观察池总分排序](charts/watchlist-total-score.png)" in content
    assert "600519.SH" in content
