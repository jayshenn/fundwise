"""行业相关 AkShare 接口的实时契约测试。"""

import os
import ssl
import time
from collections.abc import Callable
from datetime import date, datetime, timedelta

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


def _quarter_end_candidates(max_years_back: int = 4) -> list[str]:
    """按时间倒序生成最近若干年的季末日期候选。"""
    today = date.today()
    candidates: list[str] = []
    for year in range(today.year, today.year - max_years_back - 1, -1):
        for month, day_of_month in ((12, 31), (9, 30), (6, 30), (3, 31)):
            q_end = date(year, month, day_of_month)
            if q_end <= today:
                candidates.append(q_end.strftime("%Y%m%d"))
    return candidates


def _load_industry_pe_with_fallback() -> tuple[str, pd.DataFrame]:
    """尝试最近季末日期，回退加载行业市盈率数据。"""
    network_errors: list[str] = []
    for q_end in _quarter_end_candidates():
        try:
            data = ak.stock_industry_pe_ratio_cninfo(
                symbol="证监会行业分类",
                date=q_end,
            )
        except (
            requests.exceptions.RequestException,
            ConnectionError,
            TimeoutError,
            ssl.SSLError,
        ) as exc:
            network_errors.append(f"{q_end}: {type(exc).__name__}")
            continue

        if isinstance(data, pd.DataFrame) and not data.empty:
            return q_end, data

    if network_errors and not AKSHARE_STRICT:
        pytest.skip(
            "stock_industry_pe_ratio_cninfo 因网络/上游问题不可用："
            + "; ".join(network_errors[:5])
        )

    pytest.fail("stock_industry_pe_ratio_cninfo 在最近季末日期均未返回有效数据")


def test_industry_name_ths_contract() -> None:
    _require_live_mode()
    data = _fetch_live_dataframe(
        "stock_board_industry_name_ths",
        lambda: ak.stock_board_industry_name_ths(),
    )

    _assert_required_columns(data, ["name", "code"])
    assert len(data) >= 20, f"预期至少 20 个行业，实际 {len(data)}"


def test_industry_index_ths_contract() -> None:
    _require_live_mode()
    name_df = _fetch_live_dataframe(
        "stock_board_industry_name_ths",
        lambda: ak.stock_board_industry_name_ths(),
    )
    symbol = "半导体" if "半导体" in set(name_df["name"]) else name_df.iloc[0]["name"]
    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=120)).strftime("%Y%m%d")

    data = _fetch_live_dataframe(
        "stock_board_industry_index_ths",
        lambda: ak.stock_board_industry_index_ths(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
        ),
    )

    _assert_required_columns(
        data,
        ["日期", "开盘价", "最高价", "最低价", "收盘价", "成交量", "成交额"],
    )
    _assert_date_not_stale(data, "日期", max_age_days=45)


def test_industry_fund_flow_contract() -> None:
    _require_live_mode()
    data = _fetch_live_dataframe(
        "stock_fund_flow_industry",
        lambda: ak.stock_fund_flow_industry(symbol="即时"),
    )

    _assert_required_columns(
        data,
        ["行业", "行业指数", "行业-涨跌幅", "流入资金", "流出资金", "净额", "公司家数", "领涨股"],
    )
    assert len(data) >= 20, f"预期至少 20 个行业，实际 {len(data)}"


def test_industry_pe_ratio_cninfo_contract() -> None:
    _require_live_mode()
    selected_date, data = _load_industry_pe_with_fallback()

    _assert_required_columns(
        data,
        ["变动日期", "行业分类", "行业编码", "行业名称", "公司数量", "静态市盈率-加权平均"],
    )
    # 行业 PE 通常按季度/阶段披露，放宽 freshness 阈值。
    _assert_date_not_stale(data, "变动日期", max_age_days=550)
    assert len(data) >= 20, (
        f"预期 {selected_date} 至少 20 行行业数据，实际 {len(data)}"
    )
