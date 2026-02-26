"""A 股与港股核心 AkShare 接口的实时契约测试。"""

import os
import ssl
import time
from collections.abc import Callable
from datetime import datetime, timedelta

import akshare as ak
import pandas as pd
import pytest
import requests

RUN_AKSHARE_TESTS = os.getenv("RUN_AKSHARE_TESTS") == "1"
AKSHARE_STRICT = os.getenv("AKSHARE_STRICT") == "1"
pytestmark = pytest.mark.akshare_live
DataFrameFetcher = Callable[[], pd.DataFrame]


def _require_live_mode() -> None:
    """仅在 RUN_AKSHARE_TESTS=1 时运行实时测试。"""
    if not RUN_AKSHARE_TESTS:
        pytest.skip("请设置 RUN_AKSHARE_TESTS=1 后再运行 AkShare 实时契约测试")


def _assert_dataframe_non_empty(data: pd.DataFrame) -> None:
    """断言响应为非空 DataFrame。"""
    assert isinstance(data, pd.DataFrame), f"预期 DataFrame，实际为 {type(data)!r}"
    assert not data.empty, "AkShare 返回了空 DataFrame"


def _assert_required_columns(data: pd.DataFrame, required_columns: list[str]) -> None:
    """断言 DataFrame 中包含必需列。"""
    missing = [name for name in required_columns if name not in data.columns]
    assert not missing, f"缺少列: {missing}; 当前列: {list(data.columns)}"


def _assert_date_not_stale(
    data: pd.DataFrame,
    column_name: str,
    max_age_days: int,
) -> None:
    """断言最新数据点未超过允许的滞后阈值。"""
    series = pd.to_datetime(data[column_name], errors="coerce").dropna()
    assert not series.empty, f"列 {column_name!r} 不包含有效日期"
    latest = series.max().date()
    threshold = datetime.now().date() - timedelta(days=max_age_days)
    assert latest >= threshold, f"最新日期 {latest} 已超过 {max_age_days} 天阈值"


def _fetch_live_dataframe(
    endpoint_name: str,
    fetcher: DataFrameFetcher,
    retries: int = 3,
    base_sleep_seconds: float = 1.0,
) -> pd.DataFrame:
    """带重试拉取 DataFrame，并按严格/非严格模式处理失败。"""
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            data = fetcher()
            _assert_dataframe_non_empty(data)
            return data
        except (
            requests.exceptions.RequestException,
            ConnectionError,
            TimeoutError,
            ssl.SSLError,
        ) as exc:
            last_error = exc
            if attempt < retries:
                time.sleep(base_sleep_seconds * attempt)

    assert last_error is not None
    message = (
        f"{endpoint_name} 在重试 {retries} 次后仍失败，原因可能是网络/上游异常："
        f"{type(last_error).__name__}: {last_error}"
    )
    if AKSHARE_STRICT:
        pytest.fail(message)
    pytest.skip(message)


def test_stock_zh_a_hist_tx_contract() -> None:
    _require_live_mode()
    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=120)).strftime("%Y%m%d")

    data = _fetch_live_dataframe(
        "stock_zh_a_hist_tx",
        lambda: ak.stock_zh_a_hist_tx(
            symbol="sh600519",
            start_date=start_date,
            end_date=end_date,
            adjust="",
        ),
    )

    _assert_required_columns(
        data,
        ["date", "open", "close", "high", "low", "amount"],
    )
    _assert_date_not_stale(data, "date", max_age_days=45)


def test_stock_hk_daily_contract() -> None:
    _require_live_mode()
    data = _fetch_live_dataframe(
        "stock_hk_daily",
        lambda: ak.stock_hk_daily(symbol="00700"),
    )

    _assert_required_columns(
        data,
        ["date", "open", "high", "low", "close", "volume"],
    )
    _assert_date_not_stale(data, "date", max_age_days=45)


def test_a_share_valuation_contract() -> None:
    _require_live_mode()
    data = _fetch_live_dataframe(
        "stock_zh_valuation_baidu",
        lambda: ak.stock_zh_valuation_baidu(
            symbol="600519",
            indicator="总市值",
            period="近一年",
        ),
    )

    _assert_required_columns(data, ["date", "value"])
    _assert_date_not_stale(data, "date", max_age_days=45)


def test_hk_valuation_contract() -> None:
    _require_live_mode()
    data = _fetch_live_dataframe(
        "stock_hk_valuation_baidu",
        lambda: ak.stock_hk_valuation_baidu(
            symbol="00700",
            indicator="总市值",
            period="近一年",
        ),
    )

    _assert_required_columns(data, ["date", "value"])
    _assert_date_not_stale(data, "date", max_age_days=45)


def test_a_share_financial_sheet_contracts() -> None:
    _require_live_mode()
    required_columns = ["SECUCODE", "SECURITY_CODE", "REPORT_DATE"]

    balance_sheet = _fetch_live_dataframe(
        "stock_balance_sheet_by_yearly_em",
        lambda: ak.stock_balance_sheet_by_yearly_em(symbol="SH600519"),
    )
    _assert_required_columns(balance_sheet, required_columns)

    profit_sheet = _fetch_live_dataframe(
        "stock_profit_sheet_by_yearly_em",
        lambda: ak.stock_profit_sheet_by_yearly_em(symbol="SH600519"),
    )
    _assert_required_columns(profit_sheet, required_columns)

    cash_flow_sheet = _fetch_live_dataframe(
        "stock_cash_flow_sheet_by_yearly_em",
        lambda: ak.stock_cash_flow_sheet_by_yearly_em(symbol="SH600519"),
    )
    _assert_required_columns(cash_flow_sheet, required_columns)


def test_hk_financial_indicator_contract() -> None:
    _require_live_mode()
    data = _fetch_live_dataframe(
        "stock_financial_hk_analysis_indicator_em",
        lambda: ak.stock_financial_hk_analysis_indicator_em(symbol="00700", indicator="年度"),
    )

    _assert_required_columns(
        data,
        [
            "SECUCODE",
            "REPORT_DATE",
            "OPERATE_INCOME",
            "HOLDER_PROFIT",
            "ROE_AVG",
            "PER_NETCASH_OPERATE",
            "DEBT_ASSET_RATIO",
        ],
    )


def test_hk_financial_report_contract() -> None:
    _require_live_mode()
    data = _fetch_live_dataframe(
        "stock_financial_hk_report_em",
        lambda: ak.stock_financial_hk_report_em(
            stock="00700",
            symbol="资产负债表",
            indicator="年度",
        ),
    )

    _assert_required_columns(
        data,
        ["SECUCODE", "SECURITY_CODE", "STD_ITEM_NAME", "AMOUNT", "STD_REPORT_DATE"],
    )
