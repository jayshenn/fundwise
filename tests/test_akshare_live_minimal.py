"""最小 AkShare live 测试：面向公司分析、选股、择时。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
import csv
from pathlib import Path
import time
from typing import Any

import pandas as pd
import pytest

ak = pytest.importorskip("akshare")


@dataclass(frozen=True)
class InterfaceCase:
    case_id: str
    title: str
    full_url: str
    function_name: str
    kwargs: dict[str, Any]
    expected_columns: tuple[str, ...]
    freshness_column: str | None = None
    freshness_days: int | None = None


# 按 docs/AKShare数据字典.csv 的 title + full_url 召回后，映射到可调用函数。
CASES: tuple[InterfaceCase, ...] = (
    InterfaceCase(
        case_id="a_hist_tx",
        title="历史行情数据-腾讯",
        full_url="https://akshare.akfamily.xyz/data/stock/stock.html#id26",
        function_name="stock_zh_a_hist_tx",
        kwargs={"symbol": "sz000001", "start_date": "20250101", "end_date": "20260227"},
        expected_columns=("date", "open", "close", "high", "low", "amount"),
        freshness_column="date",
        freshness_days=45,
    ),
    InterfaceCase(
        case_id="a_valuation",
        title="A 股估值指标",
        full_url="https://akshare.akfamily.xyz/data/stock/stock.html#id295",
        function_name="stock_zh_valuation_baidu",
        kwargs={"symbol": "600519", "indicator": "市盈率(TTM)", "period": "近一年"},
        expected_columns=("date", "value"),
        freshness_column="date",
        freshness_days=45,
    ),
    InterfaceCase(
        case_id="hk_valuation",
        title="港股估值指标",
        full_url="https://akshare.akfamily.xyz/data/stock/stock.html#id299",
        function_name="stock_hk_valuation_baidu",
        kwargs={"symbol": "00700", "indicator": "市盈率(TTM)", "period": "近一年"},
        expected_columns=("date", "value"),
        freshness_column="date",
        freshness_days=45,
    ),
    InterfaceCase(
        case_id="balance_yearly",
        title="资产负债表-沪深",
        full_url="https://akshare.akfamily.xyz/data/stock/stock.html#id158",
        function_name="stock_balance_sheet_by_yearly_em",
        kwargs={"symbol": "SH600519"},
        expected_columns=("SECUCODE", "SECURITY_CODE", "REPORT_DATE"),
        freshness_column="REPORT_DATE",
        freshness_days=460,
    ),
    InterfaceCase(
        case_id="profit_yearly",
        title="利润表",
        full_url="https://akshare.akfamily.xyz/data/stock/stock.html#id160",
        function_name="stock_profit_sheet_by_yearly_em",
        kwargs={"symbol": "SH600519"},
        expected_columns=("SECUCODE", "SECURITY_CODE", "REPORT_DATE"),
        freshness_column="REPORT_DATE",
        freshness_days=460,
    ),
    InterfaceCase(
        case_id="cashflow_yearly",
        title="现金流量表",
        full_url="https://akshare.akfamily.xyz/data/stock/stock.html#id161",
        function_name="stock_cash_flow_sheet_by_yearly_em",
        kwargs={"symbol": "SH600519"},
        expected_columns=("SECUCODE", "SECURITY_CODE", "REPORT_DATE"),
        freshness_column="REPORT_DATE",
        freshness_days=460,
    ),
    InterfaceCase(
        case_id="hk_fin_indicator",
        title="港股财务指标",
        full_url="https://akshare.akfamily.xyz/data/stock/stock.html#id214",
        function_name="stock_financial_hk_analysis_indicator_em",
        kwargs={"symbol": "00700", "indicator": "年度"},
        expected_columns=(
            "SECUCODE",
            "REPORT_DATE",
            "OPERATE_INCOME",
            "HOLDER_PROFIT",
            "ROE_AVG",
            "PER_NETCASH_OPERATE",
        ),
        freshness_column="REPORT_DATE",
        freshness_days=460,
    ),
    InterfaceCase(
        case_id="industry_pe",
        title="行业市盈率",
        full_url="https://akshare.akfamily.xyz/data/stock/stock.html#id267",
        function_name="stock_industry_pe_ratio_cninfo",
        kwargs={"symbol": "证监会行业分类", "date": "20241231"},
        expected_columns=("变动日期", "行业名称", "静态市盈率-加权平均"),
        freshness_column="变动日期",
        freshness_days=460,
    ),
    InterfaceCase(
        case_id="industry_names",
        title="同花顺-同花顺行业一览表",
        full_url="https://akshare.akfamily.xyz/data/stock/stock.html#id369",
        function_name="stock_board_industry_name_ths",
        kwargs={},
        expected_columns=("name", "code"),
    ),
    InterfaceCase(
        case_id="industry_index",
        title="同花顺-指数",
        full_url="https://akshare.akfamily.xyz/data/stock/stock.html#id370",
        function_name="stock_board_industry_index_ths",
        kwargs={"symbol": "半导体", "start_date": "20250101", "end_date": "20260227"},
        expected_columns=("日期", "开盘价", "最高价", "最低价", "收盘价", "成交量", "成交额"),
        freshness_column="日期",
        freshness_days=45,
    ),
    InterfaceCase(
        case_id="industry_flow",
        title="行业资金流",
        full_url="https://akshare.akfamily.xyz/data/stock/stock.html#id173",
        function_name="stock_fund_flow_industry",
        kwargs={"symbol": "即时"},
        expected_columns=("行业", "行业指数", "净额"),
    ),
)


def _read_dictionary_rows() -> list[dict[str, str]]:
    csv_path = Path("docs/AKShare数据字典.csv")
    assert csv_path.exists(), f"字典文件缺失: {csv_path}"
    with csv_path.open("r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _to_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, pd.Timestamp):
        return value.date()
    text = str(value).strip()
    if not text:
        return None
    text = text.split(" ")[0]
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y%m%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def _call_with_retry(function_name: str, kwargs: dict[str, Any], retries: int = 2) -> pd.DataFrame:
    fn = getattr(ak, function_name)
    last_error: Exception | None = None
    for i in range(retries + 1):
        try:
            return fn(**kwargs)
        except Exception as err:  # pragma: no cover - 仅在 live 失败时执行
            last_error = err
            if i < retries:
                time.sleep(1.5 * (i + 1))
    raise AssertionError(f"接口调用失败: {function_name}, kwargs={kwargs}, err={last_error}")


@pytest.mark.parametrize("case", CASES, ids=[c.case_id for c in CASES])
def test_cases_are_traceable_to_dictionary(case: InterfaceCase) -> None:
    """确保测试接口都能在 AKShare 数据字典中追溯到对应 full_url。"""
    rows = _read_dictionary_rows()
    matched = [
        row for row in rows if row.get("full_url") == case.full_url and case.title in (row.get("title") or "")
    ]
    assert matched, f"未在数据字典中找到接口: {case.title} | {case.full_url}"


@pytest.mark.parametrize("case", CASES, ids=[c.case_id for c in CASES])
def test_live_interface_returns_valid_dataframe(case: InterfaceCase) -> None:
    """真实网络调用：验证数据非空、关键列存在、日期新鲜度合理。"""
    df = _call_with_retry(case.function_name, case.kwargs)

    assert isinstance(df, pd.DataFrame), f"返回值不是 DataFrame: {case.function_name}"
    assert not df.empty, f"接口返回空数据: {case.function_name}"

    missing = [col for col in case.expected_columns if col not in df.columns]
    assert not missing, f"缺少关键列 {missing}: {case.function_name}"

    if case.freshness_column and case.freshness_days is not None:
        assert case.freshness_column in df.columns, (
            f"新鲜度列不存在: {case.freshness_column} in {case.function_name}"
        )
        parsed = [_to_date(v) for v in df[case.freshness_column].tolist()]
        valid_dates = [d for d in parsed if d is not None]
        assert valid_dates, f"无法解析日期列: {case.freshness_column} in {case.function_name}"
        latest = max(valid_dates)
        days = (date.today() - latest).days
        assert days <= case.freshness_days, (
            f"数据可能过期: {case.function_name} 最新日期 {latest}, 距今 {days} 天"
        )
