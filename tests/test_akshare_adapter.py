"""AkShare 适配器标准化逻辑测试（离线）。"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

import fundwise.data_adapter.akshare_adapter as adapter_module
from fundwise.data_adapter.akshare_adapter import AkshareDataAdapter


def test_get_price_history_cn_standardization(monkeypatch) -> None:
    def _fake_hist_tx(*args, **kwargs) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "date": ["2024-01-02", "2024-01-03"],
                "open": [1.0, 1.1],
                "close": [1.2, 1.3],
                "high": [1.25, 1.35],
                "low": [0.95, 1.05],
                "amount": [1000, 1200],
            }
        )

    monkeypatch.setattr(adapter_module.ak, "stock_zh_a_hist_tx", _fake_hist_tx)
    adapter = AkshareDataAdapter(strict=True)
    data = adapter.get_price_history("600519.SH", start_date="2024-01-01", end_date="2024-12-31")

    assert list(data.columns) == adapter_module.PRICE_COLUMNS
    assert data["symbol"].unique().tolist() == ["600519.SH"]
    assert data["market"].unique().tolist() == ["CN"]
    assert data["currency"].unique().tolist() == ["CNY"]
    assert data["turnover"].tolist() == [1000, 1200]


def test_get_price_history_hk_filtering(monkeypatch) -> None:
    def _fake_hk_daily(*args, **kwargs) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "date": ["2024-01-01", "2024-03-01", "2024-05-01"],
                "open": [10, 11, 12],
                "high": [11, 12, 13],
                "low": [9, 10, 11],
                "close": [10.5, 11.5, 12.5],
                "volume": [100, 200, 300],
            }
        )

    monkeypatch.setattr(adapter_module.ak, "stock_hk_daily", _fake_hk_daily)
    adapter = AkshareDataAdapter(strict=True)
    data = adapter.get_price_history("00700.HK", start_date="2024-02-01", end_date="2024-04-01")

    assert data["date"].tolist() == ["2024-03-01"]
    assert data["symbol"].unique().tolist() == ["00700.HK"]
    assert data["market"].unique().tolist() == ["HK"]


def test_get_market_cap_history_dispatch(monkeypatch) -> None:
    def _fake_cn_valuation(*args, **kwargs) -> pd.DataFrame:
        return pd.DataFrame({"date": ["2024-01-01"], "value": [2_000_000]})

    def _fake_hk_valuation(*args, **kwargs) -> pd.DataFrame:
        return pd.DataFrame({"date": ["2024-01-02"], "value": [3_000_000]})

    monkeypatch.setattr(adapter_module.ak, "stock_zh_valuation_baidu", _fake_cn_valuation)
    monkeypatch.setattr(adapter_module.ak, "stock_hk_valuation_baidu", _fake_hk_valuation)
    adapter = AkshareDataAdapter(strict=True)

    cn_data = adapter.get_market_cap_history("600519.SH")
    hk_data = adapter.get_market_cap_history("00700.HK")

    assert cn_data["market_cap"].tolist() == [2_000_000]
    assert hk_data["market_cap"].tolist() == [3_000_000]
    assert cn_data["currency"].iloc[0] == "CNY"
    assert hk_data["currency"].iloc[0] == "HKD"


def test_get_pe_history_dispatch(monkeypatch) -> None:
    def _fake_cn_valuation(*args, **kwargs) -> pd.DataFrame:
        return pd.DataFrame({"date": ["2024-01-01"], "value": [22.5]})

    def _fake_hk_valuation(*args, **kwargs) -> pd.DataFrame:
        return pd.DataFrame({"date": ["2024-01-02"], "value": [18.7]})

    monkeypatch.setattr(adapter_module.ak, "stock_zh_valuation_baidu", _fake_cn_valuation)
    monkeypatch.setattr(adapter_module.ak, "stock_hk_valuation_baidu", _fake_hk_valuation)
    adapter = AkshareDataAdapter(strict=True)

    cn_data = adapter.get_pe_history("600519.SH")
    hk_data = adapter.get_pe_history("00700.HK")

    assert cn_data["pe"].tolist() == [22.5]
    assert hk_data["pe"].tolist() == [18.7]
    assert cn_data["currency"].iloc[0] == "CNY"
    assert hk_data["currency"].iloc[0] == "HKD"


def test_get_hs300_pe_history_standardization(monkeypatch) -> None:
    def _fake_index_pe(*args, **kwargs) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "日期": ["2024-01-01", "2024-01-02"],
                "滚动市盈率": [11.2, 11.4],
            }
        )

    monkeypatch.setattr(adapter_module.ak, "stock_index_pe_lg", _fake_index_pe)
    adapter = AkshareDataAdapter(strict=True)
    data = adapter.get_hs300_pe_history()

    assert data["date"].tolist() == ["2024-01-01", "2024-01-02"]
    assert data["hs300_pe"].tolist() == [11.2, 11.4]


def test_get_balance_components_cn(monkeypatch) -> None:
    def _fake_balance_report(*args, **kwargs) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "REPORT_DATE": ["2024-12-31"],
                "MONETARYFUNDS": [120.0],
                "ACCOUNTS_RECE": [15.0],
                "PREPAYMENT": [5.0],
                "INVENTORY": [30.0],
                "LONG_TERM_EQUITY_INVEST": [10.0],
                "FIXED_ASSET": [200.0],
                "INTANGIBLE_ASSET": [18.0],
                "GOODWILL": [2.0],
                "TOTAL_ASSETS": [800.0],
                "ACCOUNTS_PAYABLE": [45.0],
                "CONTRACT_LIAB": [220.0],
                "STAFF_SALARY_PAYABLE": [12.0],
                "TAX_PAYABLE": [20.0],
                "SHORT_LOAN": [8.0],
                "LONG_LOAN": [12.0],
                "TOTAL_LIABILITIES": [380.0],
            }
        )

    monkeypatch.setattr(adapter_module.ak, "stock_balance_sheet_by_report_em", _fake_balance_report)
    monkeypatch.setattr(
        adapter_module.ak,
        "stock_balance_sheet_by_yearly_em",
        lambda *args, **kwargs: pd.DataFrame(),
    )
    adapter = AkshareDataAdapter(strict=True)
    data = adapter.get_balance_components("600519.SH")

    assert set(data["side"]) == {"asset", "liability"}
    cash_row = data.loc[data["category"] == "现金"].iloc[0]
    contract_row = data.loc[data["category"] == "合同负债/预收款"].iloc[0]
    assert float(cash_row["value"]) == 120.0
    assert float(contract_row["value"]) == 220.0


def test_get_industry_pe_history_fallback_to_cache(monkeypatch, tmp_path: Path) -> None:
    cache_dir = tmp_path / "industry-cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_file = cache_dir / "600519_SH-industry_pe.csv"
    pd.DataFrame(
        {
            "symbol": ["600519.SH"],
            "date": ["2024-12-31"],
            "industry": ["酿酒行业"],
            "industry_pe": [28.5],
            "market": ["CN"],
        }
    ).to_csv(cache_file, index=False)

    monkeypatch.setattr(
        adapter_module.ak,
        "stock_individual_info_em",
        lambda *args, **kwargs: pd.DataFrame({"item": ["行业"], "value": ["酿酒行业"]}),
    )
    monkeypatch.setattr(
        adapter_module.ak,
        "stock_industry_pe_ratio_cninfo",
        lambda *args, **kwargs: pd.DataFrame(),
    )

    adapter = AkshareDataAdapter(strict=True, industry_cache_dir=cache_dir)
    data = adapter.get_industry_pe_history("600519.SH")

    assert not data.empty
    assert data["industry_pe"].tolist() == [28.5]


def test_get_industry_roe_history_fallback_to_cache(monkeypatch, tmp_path: Path) -> None:
    cache_dir = tmp_path / "industry-cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_file = cache_dir / "600519_SH-industry_roe.csv"
    pd.DataFrame(
        {
            "symbol": ["600519.SH"],
            "date": ["2024-12-31"],
            "industry": ["酿酒行业"],
            "industry_roe": [17.2],
            "sample_size": [8],
            "market": ["CN"],
        }
    ).to_csv(cache_file, index=False)

    monkeypatch.setattr(
        adapter_module.ak,
        "stock_individual_info_em",
        lambda *args, **kwargs: pd.DataFrame({"item": ["行业"], "value": ["酿酒行业"]}),
    )
    monkeypatch.setattr(
        adapter_module.ak,
        "stock_board_industry_cons_em",
        lambda *args, **kwargs: pd.DataFrame(),
    )

    adapter = AkshareDataAdapter(strict=True, industry_cache_dir=cache_dir)
    data = adapter.get_industry_roe_history("600519.SH")

    assert not data.empty
    assert data["industry_roe"].tolist() == [17.2]
    assert data["sample_size"].tolist() == [8]


def test_get_financial_indicators_cn_merge(monkeypatch) -> None:
    def _fake_profit(*args, **kwargs) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "REPORT_DATE": ["2023-12-31", "2024-12-31"],
                "TOTAL_OPERATE_INCOME": [1000, 1200],
                "PARENT_NETPROFIT": [200, 250],
            }
        )

    def _fake_cashflow(*args, **kwargs) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "REPORT_DATE": ["2023-12-31", "2024-12-31"],
                "NETCASH_OPERATE": [180, 230],
            }
        )

    def _fake_balance(*args, **kwargs) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "REPORT_DATE": ["2023-12-31", "2024-12-31"],
                "TOTAL_ASSETS": [5000, 6000],
                "TOTAL_LIABILITIES": [2000, 2500],
            }
        )

    def _fake_roe(*args, **kwargs) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "日期": ["2023-12-31", "2024-12-31"],
                "净资产收益率(%)": [10.0, 11.0],
            }
        )

    monkeypatch.setattr(adapter_module.ak, "stock_profit_sheet_by_yearly_em", _fake_profit)
    monkeypatch.setattr(adapter_module.ak, "stock_cash_flow_sheet_by_yearly_em", _fake_cashflow)
    monkeypatch.setattr(adapter_module.ak, "stock_balance_sheet_by_yearly_em", _fake_balance)
    monkeypatch.setattr(adapter_module.ak, "stock_financial_analysis_indicator", _fake_roe)

    adapter = AkshareDataAdapter(strict=True)
    data = adapter.get_financial_indicators("600519.SH")

    assert list(data.columns) == adapter_module.FINANCIAL_COLUMNS
    assert data["revenue"].tolist() == [1000, 1200]
    assert data["debt_to_asset"].round(6).tolist() == [0.4, 0.416667]


def test_get_financial_indicators_hk_ratio_normalization(monkeypatch) -> None:
    def _fake_hk_indicator(*args, **kwargs) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "REPORT_DATE": ["2024-12-31"],
                "OPERATE_INCOME": [800],
                "HOLDER_PROFIT": [120],
                "PER_NETCASH_OPERATE": [1.23],
                "ROE_AVG": [15.5],
                "DEBT_ASSET_RATIO": [45.0],
            }
        )

    monkeypatch.setattr(
        adapter_module.ak,
        "stock_financial_hk_analysis_indicator_em",
        _fake_hk_indicator,
    )
    adapter = AkshareDataAdapter(strict=True)
    data = adapter.get_financial_indicators("00700.HK")

    assert data["symbol"].iloc[0] == "00700.HK"
    assert float(data["debt_to_asset"].iloc[0]) == 0.45
