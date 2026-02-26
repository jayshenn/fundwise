"""单公司分析卡构建逻辑。"""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from typing import Any, cast

import pandas as pd


@dataclass(frozen=True, slots=True)
class CompanyDossier:
    """单公司核心分析结果。

    字段均为标准化口径，供 Markdown 报告和后续评分模块复用。
    """

    symbol: str
    market: str
    currency: str
    fx_to_cny: float | None
    as_of_date: str | None
    latest_close: float | None
    latest_market_cap: float | None
    latest_market_cap_cny: float | None
    latest_revenue: float | None
    latest_revenue_cny: float | None
    latest_net_profit: float | None
    latest_net_profit_cny: float | None
    latest_ocf: float | None
    latest_ocf_cny: float | None
    latest_roe: float | None
    latest_debt_to_asset: float | None
    revenue_yoy: float | None
    net_profit_yoy: float | None
    ocf_to_profit: float | None
    price_return_since_start: float | None

    def to_dict(self) -> dict[str, Any]:
        """将数据类对象转为字典，便于序列化。"""
        return asdict(self)


def build_company_dossier(
    symbol: str,
    dataset: dict[str, pd.DataFrame],
    *,
    fx_to_cny: float | None = None,
) -> CompanyDossier:
    """根据标准化数据集生成单公司分析卡。"""
    price_history = dataset.get("price_history", pd.DataFrame())
    market_cap_history = dataset.get("market_cap_history", pd.DataFrame())
    financial_indicators = dataset.get("financial_indicators", pd.DataFrame())

    latest_price = _latest_row(price_history)
    latest_market_cap = _latest_row(market_cap_history)
    latest_financial = _latest_row(financial_indicators)

    market = _safe_str(_pick_field(latest_price, "market"))
    currency = _safe_str(_pick_field(latest_price, "currency"))
    if market is None:
        market = _safe_str(_pick_field(latest_financial, "market"))
    if currency is None:
        currency = _safe_str(_pick_field(latest_financial, "currency"))

    revenue_series = _numeric_series(financial_indicators, "revenue")
    net_profit_series = _numeric_series(financial_indicators, "net_profit")

    latest_net_profit = _safe_float(_pick_field(latest_financial, "net_profit"))
    latest_ocf = _safe_float(_pick_field(latest_financial, "ocf"))
    latest_market_cap = _safe_float(_pick_field(latest_market_cap, "market_cap"))
    latest_revenue = _safe_float(_pick_field(latest_financial, "revenue"))
    normalized_fx_to_cny = _normalize_fx_to_cny(
        currency=currency or "UNKNOWN",
        fx_to_cny=fx_to_cny,
    )

    return CompanyDossier(
        symbol=symbol,
        market=market or "UNKNOWN",
        currency=currency or "UNKNOWN",
        fx_to_cny=normalized_fx_to_cny,
        as_of_date=_latest_date([price_history, market_cap_history, financial_indicators]),
        latest_close=_safe_float(_pick_field(latest_price, "close")),
        latest_market_cap=latest_market_cap,
        latest_market_cap_cny=_convert_to_cny(latest_market_cap, normalized_fx_to_cny),
        latest_revenue=latest_revenue,
        latest_revenue_cny=_convert_to_cny(latest_revenue, normalized_fx_to_cny),
        latest_net_profit=latest_net_profit,
        latest_net_profit_cny=_convert_to_cny(latest_net_profit, normalized_fx_to_cny),
        latest_ocf=latest_ocf,
        latest_ocf_cny=_convert_to_cny(latest_ocf, normalized_fx_to_cny),
        latest_roe=_safe_float(_pick_field(latest_financial, "roe")),
        latest_debt_to_asset=_safe_float(_pick_field(latest_financial, "debt_to_asset")),
        revenue_yoy=_calc_yoy(revenue_series),
        net_profit_yoy=_calc_yoy(net_profit_series),
        ocf_to_profit=_calc_ratio(latest_ocf, latest_net_profit),
        price_return_since_start=_calc_price_return(price_history),
    )


def _latest_date(frames: list[pd.DataFrame]) -> str | None:
    """从多个表中取最新日期。"""
    max_date: pd.Timestamp | None = None
    for frame in frames:
        if frame.empty or "date" not in frame.columns:
            continue
        parsed = pd.Series(pd.to_datetime(frame["date"], errors="coerce"), index=frame.index)
        parsed = cast(pd.Series, parsed.dropna())
        if parsed.empty:
            continue
        current = parsed.max()
        if max_date is None or current > max_date:
            max_date = current
    if max_date is None:
        return None
    return max_date.strftime("%Y-%m-%d")


def _latest_row(frame: pd.DataFrame) -> pd.Series | None:
    """按日期排序后返回最后一行。"""
    if frame.empty:
        return None
    if "date" not in frame.columns:
        return frame.iloc[-1]

    working = frame.copy()
    working["__parsed_date__"] = pd.to_datetime(working["date"], errors="coerce")
    working = working.sort_values(by="__parsed_date__", na_position="first")
    if working.empty:
        return None
    return working.iloc[-1]


def _pick_field(row: pd.Series | None, field: str) -> object:
    """从行对象安全读取字段。"""
    if row is None:
        return None
    if field not in row.index:
        return None
    return row[field]


def _numeric_series(frame: pd.DataFrame, field: str) -> pd.Series:
    """提取并清洗数值序列。"""
    if frame.empty or field not in frame.columns:
        return pd.Series(dtype="float64")
    raw = pd.Series(frame[field], index=frame.index)
    numeric = pd.Series(pd.to_numeric(raw, errors="coerce"), index=frame.index)
    clean = cast(pd.Series, numeric.dropna())
    return pd.Series(clean, index=clean.index, dtype="float64")


def _calc_yoy(series: pd.Series) -> float | None:
    """按最后两期计算同比增速。

    返回值为小数（例如 0.15 代表 15%）。
    """
    if len(series) < 2:
        return None
    values = series.tolist()
    prev = float(values[-2])
    current = float(values[-1])
    if prev == 0:
        return None
    return (current - prev) / abs(prev)


def _calc_ratio(numerator: float | None, denominator: float | None) -> float | None:
    """计算比值，分母为空或为 0 时返回空值。"""
    if numerator is None or denominator is None or denominator == 0:
        return None
    return numerator / denominator


def _calc_price_return(price_history: pd.DataFrame) -> float | None:
    """计算区间价格涨跌幅。"""
    if price_history.empty or "close" not in price_history.columns:
        return None
    working = price_history.copy()
    if "date" in working.columns:
        working["__parsed_date__"] = pd.to_datetime(working["date"], errors="coerce")
        working = working.sort_values(by="__parsed_date__", na_position="first")
    closes = pd.Series(pd.to_numeric(working["close"], errors="coerce"), index=working.index)
    closes = cast(pd.Series, closes.dropna())
    if len(closes) < 2:
        return None
    start_price = float(closes.iloc[0])
    end_price = float(closes.iloc[-1])
    if start_price == 0:
        return None
    return (end_price - start_price) / start_price


def _safe_float(value: object) -> float | None:
    """将输入安全转换为浮点数。"""
    if _is_missing_value(value):
        return None
    try:
        return float(cast(Any, value))
    except (TypeError, ValueError):
        return None


def _safe_str(value: object) -> str | None:
    """将输入安全转换为字符串。"""
    if _is_missing_value(value):
        return None
    text = str(value).strip()
    return text or None


def _is_missing_value(value: object) -> bool:
    """判断标量值是否为空值（None/NaN/NaT/NA）。"""
    if value is None or value is pd.NA or value is pd.NaT:
        return True
    if isinstance(value, float):
        return math.isnan(value)
    return False


def _normalize_fx_to_cny(currency: str, fx_to_cny: float | None) -> float | None:
    """规范化换算汇率，CNY 默认返回 1。"""
    if currency.upper() == "CNY":
        return 1.0
    if fx_to_cny is None or fx_to_cny <= 0:
        return None
    return fx_to_cny


def _convert_to_cny(value: float | None, fx_to_cny: float | None) -> float | None:
    """将数值按汇率折算为 CNY。"""
    if value is None or fx_to_cny is None:
        return None
    return value * fx_to_cny
