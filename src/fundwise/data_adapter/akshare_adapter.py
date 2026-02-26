"""AkShare 数据适配器。

该模块负责：
1. 将标准 symbol（如 `600519.SH`、`00700.HK`）转换为 AkShare 参数；
2. 拉取并标准化行情、估值和核心财务字段；
3. 输出统一字段 DataFrame，供上层分析模块消费。
"""

from __future__ import annotations

import logging
import ssl
import time
import warnings
from collections.abc import Callable
from datetime import date, datetime
from pathlib import Path
from typing import Final, cast

import akshare as ak
import pandas as pd
import requests

from fundwise.data_adapter.symbols import SymbolInfo, parse_symbol

DataFrameFetcher = Callable[[], pd.DataFrame]
logger = logging.getLogger(__name__)

PRICE_COLUMNS: Final[list[str]] = [
    "symbol",
    "date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "turnover",
    "market",
    "currency",
]

MARKET_CAP_COLUMNS: Final[list[str]] = ["symbol", "date", "market_cap", "market", "currency"]
PE_COLUMNS: Final[list[str]] = ["symbol", "date", "pe", "market", "currency"]
INDEX_PE_COLUMNS: Final[list[str]] = ["date", "hs300_pe"]
INDUSTRY_PE_COLUMNS: Final[list[str]] = ["symbol", "date", "industry", "industry_pe", "market"]
INDUSTRY_ROE_COLUMNS: Final[list[str]] = [
    "symbol",
    "date",
    "industry",
    "industry_roe",
    "sample_size",
    "market",
]
BALANCE_COMPONENT_COLUMNS: Final[list[str]] = [
    "symbol",
    "date",
    "category",
    "side",
    "value",
    "market",
    "currency",
]

FINANCIAL_COLUMNS: Final[list[str]] = [
    "symbol",
    "date",
    "revenue",
    "net_profit",
    "ocf",
    "pe_ttm",
    "roe",
    "total_assets",
    "total_liabilities",
    "debt_to_asset",
    "market",
    "currency",
]


class AkshareDataAdapter:
    """AkShare 统一数据适配器。"""

    def __init__(
        self,
        retries: int = 3,
        base_sleep_seconds: float = 1.0,
        strict: bool = True,
        industry_cache_dir: Path | None = None,
    ):
        """初始化适配器。

        参数说明：
            retries: 网络错误重试次数。
            base_sleep_seconds: 重试退避基准秒数。
            strict: 严格模式。开启后接口失败抛异常；关闭后返回空 DataFrame。
            industry_cache_dir: 行业数据缓存目录；为空时默认 `data/cache/industry`。
        """
        self.retries = retries
        self.base_sleep_seconds = base_sleep_seconds
        self.strict = strict
        self.industry_cache_dir = industry_cache_dir or Path("data/cache/industry")
        self._industry_name_cache: dict[str, str | None] = {}

    def get_symbol_info(self, symbol: str) -> SymbolInfo:
        """解析并返回标准化 symbol 信息。"""
        return parse_symbol(symbol)

    def get_price_history(
        self,
        symbol: str,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> pd.DataFrame:
        """获取并标准化日频行情数据。"""
        info = parse_symbol(symbol)
        start_compact = _to_yyyymmdd(start_date)
        end_compact = _to_yyyymmdd(end_date)

        if info.market == "CN":
            data = self._fetch_dataframe(
                endpoint_name="stock_zh_a_hist_tx",
                fetcher=lambda: ak.stock_zh_a_hist_tx(
                    symbol=info.to_akshare_hist_symbol(),
                    start_date=start_compact,
                    end_date=end_compact,
                    adjust="",
                ),
            )
            if data.empty:
                return _empty_dataframe(PRICE_COLUMNS)
            result = pd.DataFrame(
                {
                    "symbol": info.symbol,
                    "date": _to_date_series(data, ["date", "日期"]),
                    "open": _to_numeric_series(data, ["open", "开盘"]),
                    "high": _to_numeric_series(data, ["high", "最高"]),
                    "low": _to_numeric_series(data, ["low", "最低"]),
                    "close": _to_numeric_series(data, ["close", "收盘"]),
                    "volume": _to_numeric_series(data, ["volume", "成交量"]),
                    "turnover": _to_numeric_series(data, ["amount", "成交额"]),
                    "market": info.market,
                    "currency": info.currency,
                }
            )
            return _finalize_sorted(result, PRICE_COLUMNS)

        data = self._fetch_dataframe(
            endpoint_name="stock_hk_daily",
            fetcher=lambda: ak.stock_hk_daily(symbol=info.code),
        )
        if data.empty:
            return _empty_dataframe(PRICE_COLUMNS)

        result = pd.DataFrame(
            {
                "symbol": info.symbol,
                "date": _to_date_series(data, ["date", "日期"]),
                "open": _to_numeric_series(data, ["open", "开盘"]),
                "high": _to_numeric_series(data, ["high", "最高"]),
                "low": _to_numeric_series(data, ["low", "最低"]),
                "close": _to_numeric_series(data, ["close", "收盘"]),
                "volume": _to_numeric_series(data, ["volume", "成交量"]),
                "turnover": _to_numeric_series(data, ["amount", "成交额"]),
                "market": info.market,
                "currency": info.currency,
            }
        )
        filtered = _filter_by_date_range(result, start_date=start_date, end_date=end_date)
        return _finalize_sorted(filtered, PRICE_COLUMNS)

    def get_market_cap_history(self, symbol: str, period: str | None = None) -> pd.DataFrame:
        """获取并标准化总市值历史数据。"""
        info = parse_symbol(symbol)
        normalized_period = _normalize_baidu_valuation_period(
            market=info.market,
            period=period,
            default_period="近十年",
        )
        if info.market == "CN":
            data = self._fetch_dataframe(
                endpoint_name="stock_zh_valuation_baidu",
                fetcher=lambda: ak.stock_zh_valuation_baidu(
                    symbol=info.code,
                    indicator="总市值",
                    period=normalized_period,
                ),
            )
        else:
            data = self._fetch_dataframe(
                endpoint_name="stock_hk_valuation_baidu",
                fetcher=lambda: ak.stock_hk_valuation_baidu(
                    symbol=info.code,
                    indicator="总市值",
                    period=normalized_period,
                ),
            )

        if data.empty:
            return _empty_dataframe(MARKET_CAP_COLUMNS)

        result = pd.DataFrame(
            {
                "symbol": info.symbol,
                "date": _to_date_series(data, ["date", "日期"]),
                "market_cap": _to_numeric_series(data, ["value", "总市值"]),
                "market": info.market,
                "currency": info.currency,
            }
        )
        return _finalize_sorted(result, MARKET_CAP_COLUMNS)

    def get_pe_history(
        self,
        symbol: str,
        period: str | None = None,
        indicator: str = "市盈率(TTM)",
    ) -> pd.DataFrame:
        """获取公司 PE 历史序列。"""
        info = parse_symbol(symbol)
        normalized_period = _normalize_baidu_valuation_period(
            market=info.market,
            period=period,
            default_period="近十年",
        )
        if info.market == "CN":
            data = self._fetch_optional_dataframe(
                endpoint_name="stock_zh_valuation_baidu",
                fetcher=lambda: ak.stock_zh_valuation_baidu(
                    symbol=info.code,
                    indicator=indicator,
                    period=normalized_period,
                ),
            )
        else:
            data = self._fetch_optional_dataframe(
                endpoint_name="stock_hk_valuation_baidu",
                fetcher=lambda: ak.stock_hk_valuation_baidu(
                    symbol=info.code,
                    indicator=indicator,
                    period=normalized_period,
                ),
            )

        if data.empty:
            return _empty_dataframe(PE_COLUMNS)

        result = pd.DataFrame(
            {
                "symbol": info.symbol,
                "date": _to_date_series(data, ["date", "日期"]),
                "pe": _to_numeric_series(data, ["value", "市盈率(TTM)", "市盈率(静)"]),
                "market": info.market,
                "currency": info.currency,
            }
        )
        result = cast(pd.DataFrame, result.loc[pd.to_numeric(result["pe"], errors="coerce") > 0, :])
        return _finalize_sorted(result, PE_COLUMNS)

    def get_financial_indicators_quarterly(self, symbol: str) -> pd.DataFrame:
        """获取季度口径核心财务字段，用于趋势图展示。"""
        info = parse_symbol(symbol)
        if info.market == "HK":
            data = self._fetch_optional_dataframe(
                endpoint_name="stock_financial_hk_analysis_indicator_em",
                fetcher=lambda: ak.stock_financial_hk_analysis_indicator_em(
                    symbol=info.code,
                    indicator="报告期",
                ),
            )
            if data.empty:
                return _empty_dataframe(FINANCIAL_COLUMNS)
            result = pd.DataFrame(
                {
                    "symbol": info.symbol,
                    "date": _to_date_series(data, ["REPORT_DATE", "date", "日期"]),
                    "revenue": _to_numeric_series(data, ["OPERATE_INCOME", "营业收入"]),
                    "net_profit": _to_numeric_series(data, ["HOLDER_PROFIT", "净利润"]),
                    "ocf": _to_numeric_series(data, ["NETCASH_OPERATE", "经营现金流净额"]),
                    "pe_ttm": pd.Series([pd.NA] * len(data)),
                    "roe": _to_numeric_series(data, ["ROE_AVG", "ROE"]),
                    "total_assets": _to_numeric_series(data, ["TOTAL_ASSETS", "资产总计"]),
                    "total_liabilities": _to_numeric_series(
                        data, ["TOTAL_LIABILITIES", "负债合计"]
                    ),
                    "debt_to_asset": _normalize_debt_ratio(
                        _to_numeric_series(data, ["DEBT_ASSET_RATIO", "资产负债率"])
                    ),
                    "market": info.market,
                    "currency": info.currency,
                }
            )
            result["debt_to_asset"] = _fill_debt_to_asset(result)
            return _finalize_sorted(result, FINANCIAL_COLUMNS)

        profit_sheet = self._fetch_optional_dataframe(
            endpoint_name="stock_profit_sheet_by_quarterly_em",
            fetcher=lambda: ak.stock_profit_sheet_by_quarterly_em(
                symbol=info.to_akshare_em_symbol()
            ),
        )
        cash_flow_sheet = self._fetch_optional_dataframe(
            endpoint_name="stock_cash_flow_sheet_by_quarterly_em",
            fetcher=lambda: ak.stock_cash_flow_sheet_by_quarterly_em(
                symbol=info.to_akshare_em_symbol()
            ),
        )
        balance_sheet = self._fetch_optional_dataframe(
            endpoint_name="stock_balance_sheet_by_report_em",
            fetcher=lambda: ak.stock_balance_sheet_by_report_em(symbol=info.to_akshare_em_symbol()),
        )
        roe_sheet = self._fetch_optional_dataframe(
            endpoint_name="stock_financial_analysis_indicator",
            fetcher=lambda: ak.stock_financial_analysis_indicator(symbol=info.code),
        )

        merged = _merge_financial_frames(
            symbol=info.symbol,
            market=info.market,
            currency=info.currency,
            revenue_frame=_extract_metric_frame(
                profit_sheet,
                metric_name="revenue",
                date_candidates=["REPORT_DATE", "报告期", "date", "日期"],
                value_candidates=[
                    "OPERATE_INCOME",
                    "TOTAL_OPERATE_INCOME",
                    "主营业务收入",
                    "营业总收入",
                ],
            ),
            net_profit_frame=_extract_metric_frame(
                profit_sheet,
                metric_name="net_profit",
                date_candidates=["REPORT_DATE", "报告期", "date", "日期"],
                value_candidates=[
                    "PARENT_NETPROFIT",
                    "NETPROFIT",
                    "归属于母公司股东的净利润",
                    "净利润",
                ],
            ),
            ocf_frame=_extract_metric_frame(
                cash_flow_sheet,
                metric_name="ocf",
                date_candidates=["REPORT_DATE", "报告期", "date", "日期"],
                value_candidates=["NETCASH_OPERATE", "经营活动产生的现金流量净额"],
            ),
            total_assets_frame=_extract_metric_frame(
                balance_sheet,
                metric_name="total_assets",
                date_candidates=["REPORT_DATE", "报告期", "date", "日期"],
                value_candidates=["TOTAL_ASSETS", "资产总计"],
            ),
            total_liabilities_frame=_extract_metric_frame(
                balance_sheet,
                metric_name="total_liabilities",
                date_candidates=["REPORT_DATE", "报告期", "date", "日期"],
                value_candidates=["TOTAL_LIABILITIES", "负债合计"],
            ),
            roe_frame=_extract_metric_frame(
                roe_sheet,
                metric_name="roe",
                date_candidates=["日期", "date", "REPORT_DATE", "报告期"],
                value_candidates=["净资产收益率(%)", "ROE", "ROE_AVG", "净资产收益率"],
            ),
        )
        return _finalize_sorted(merged, FINANCIAL_COLUMNS)

    def get_hs300_pe_history(self) -> pd.DataFrame:
        """获取沪深300滚动市盈率历史序列。"""
        data = self._fetch_optional_dataframe(
            endpoint_name="stock_index_pe_lg",
            fetcher=lambda: ak.stock_index_pe_lg(symbol="沪深300"),
        )
        if data.empty:
            return _empty_dataframe(INDEX_PE_COLUMNS)

        result = pd.DataFrame(
            {
                "date": _to_date_series(data, ["日期", "date"]),
                "hs300_pe": _to_numeric_series(
                    data,
                    ["滚动市盈率", "等权滚动市盈率", "静态市盈率"],
                ),
            }
        )
        result = cast(
            pd.DataFrame,
            result.loc[pd.to_numeric(result["hs300_pe"], errors="coerce") > 0, :],
        )
        return _finalize_sorted(result, INDEX_PE_COLUMNS)

    def get_industry_pe_history(self, symbol: str, lookback_years: int = 8) -> pd.DataFrame:
        """按行业快照构建行业 PE 历史序列。"""
        info = parse_symbol(symbol)
        if info.market != "CN":
            return _empty_dataframe(INDUSTRY_PE_COLUMNS)
        fallback = self._load_industry_cache(
            cache_type="industry_pe",
            symbol=info.symbol,
            columns=INDUSTRY_PE_COLUMNS,
        )
        industry_name = self._resolve_cn_industry_name(info.code)
        if not industry_name:
            logger.info("行业PE回退缓存: symbol=%s, reason=industry_name_unavailable", info.symbol)
            return fallback

        rows: list[dict[str, object]] = []
        for date_text in _quarter_end_candidates(max_years_back=max(lookback_years + 1, 4)):
            snapshot = self._fetch_optional_dataframe(
                endpoint_name="stock_industry_pe_ratio_cninfo",
                fetcher=lambda snapshot_date=date_text: ak.stock_industry_pe_ratio_cninfo(
                    symbol="证监会行业分类",
                    date=snapshot_date,
                ),
            )
            if snapshot.empty:
                continue
            matched = _pick_industry_row(snapshot, industry_name=industry_name)
            if matched is None:
                continue
            value = _safe_float_from_series(
                matched,
                ["静态市盈率-加权平均", "静态市盈率-算术平均", "静态市盈率-中位数"],
            )
            if value is None or value <= 0:
                continue
            point_date = _safe_str_from_series(matched, ["变动日期"]) or date_text
            rows.append(
                {
                    "symbol": info.symbol,
                    "date": point_date,
                    "industry": _safe_str_from_series(matched, ["行业名称"]) or industry_name,
                    "industry_pe": value,
                    "market": info.market,
                }
            )

        if not rows:
            logger.info("行业PE回退缓存: symbol=%s, reason=live_data_empty", info.symbol)
            return fallback
        result = _finalize_sorted(pd.DataFrame(rows), INDUSTRY_PE_COLUMNS)
        self._save_industry_cache(
            cache_type="industry_pe",
            symbol=info.symbol,
            frame=result,
        )
        logger.info("行业PE实时拉取成功并刷新缓存: symbol=%s, rows=%d", info.symbol, len(result))
        return result

    def get_industry_roe_history(
        self,
        symbol: str,
        sample_limit: int = 15,
    ) -> pd.DataFrame:
        """按行业成分股聚合计算行业 ROE（中位数）时间序列。"""
        info = parse_symbol(symbol)
        if info.market != "CN":
            return _empty_dataframe(INDUSTRY_ROE_COLUMNS)
        fallback = self._load_industry_cache(
            cache_type="industry_roe",
            symbol=info.symbol,
            columns=INDUSTRY_ROE_COLUMNS,
        )
        industry_name = self._resolve_cn_industry_name(info.code)
        if not industry_name:
            logger.info("行业ROE回退缓存: symbol=%s, reason=industry_name_unavailable", info.symbol)
            return fallback

        cons = self._fetch_optional_dataframe(
            endpoint_name="stock_board_industry_cons_em",
            fetcher=lambda: ak.stock_board_industry_cons_em(symbol=industry_name),
        )
        if cons.empty or "代码" not in cons.columns:
            logger.info("行业ROE回退缓存: symbol=%s, reason=industry_cons_empty", info.symbol)
            return fallback
        stock_codes = [str(item).zfill(6) for item in cons["代码"].dropna().astype(str).tolist()]
        stock_codes = stock_codes[: max(sample_limit, 1)]
        if not stock_codes:
            logger.info("行业ROE回退缓存: symbol=%s, reason=industry_cons_no_code", info.symbol)
            return fallback

        samples: list[pd.DataFrame] = []
        for stock_code in stock_codes:
            roe_frame = self._fetch_optional_dataframe(
                endpoint_name="stock_financial_analysis_indicator",
                fetcher=lambda code=stock_code: ak.stock_financial_analysis_indicator(symbol=code),
            )
            if roe_frame.empty:
                continue
            metric = _extract_metric_frame(
                roe_frame,
                metric_name="roe",
                date_candidates=["日期", "date", "REPORT_DATE", "报告期"],
                value_candidates=["净资产收益率(%)", "ROE", "ROE_AVG", "净资产收益率"],
            )
            metric = cast(
                pd.DataFrame,
                metric.loc[pd.to_numeric(metric["roe"], errors="coerce").notna(), :],
            )
            if metric.empty:
                continue
            metric = metric.rename(columns={"roe": stock_code})
            samples.append(metric)

        if not samples:
            logger.info("行业ROE回退缓存: symbol=%s, reason=roe_samples_empty", info.symbol)
            return fallback

        merged = samples[0]
        for sample in samples[1:]:
            merged = merged.merge(sample, on="date", how="outer")
        value_columns = [column for column in merged.columns if column != "date"]
        merged["industry_roe"] = merged[value_columns].median(axis=1, skipna=True)
        sample_counts = merged[value_columns].notna().sum(axis=1)
        result = pd.DataFrame(
            {
                "symbol": info.symbol,
                "date": merged["date"],
                "industry": industry_name,
                "industry_roe": merged["industry_roe"],
                "sample_size": sample_counts,
                "market": info.market,
            }
        )
        result = cast(
            pd.DataFrame,
            result.loc[pd.to_numeric(result["industry_roe"], errors="coerce").notna(), :],
        )
        if result.empty:
            logger.info("行业ROE回退缓存: symbol=%s, reason=aggregated_empty", info.symbol)
            return fallback
        finalized = _finalize_sorted(result, INDUSTRY_ROE_COLUMNS)
        self._save_industry_cache(
            cache_type="industry_roe",
            symbol=info.symbol,
            frame=finalized,
        )
        logger.info(
            "行业ROE实时拉取成功并刷新缓存: symbol=%s, rows=%d",
            info.symbol,
            len(finalized),
        )
        return finalized

    def get_balance_components(
        self,
        symbol: str,
        as_of_date: str | None = None,
    ) -> pd.DataFrame:
        """获取单期资产负债结构（合并同类项）。"""
        info = parse_symbol(symbol)
        if info.market == "HK":
            return self._get_hk_balance_components(info, as_of_date=as_of_date)
        return self._get_cn_balance_components(info, as_of_date=as_of_date)

    def get_financial_indicators(self, symbol: str) -> pd.DataFrame:
        """获取并标准化核心财务指标。"""
        info = parse_symbol(symbol)
        if info.market == "HK":
            return self._get_hk_financial_indicators(info)
        return self._get_cn_financial_indicators(info)

    def build_company_dataset(
        self,
        symbol: str,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> dict[str, pd.DataFrame]:
        """构建单公司最小闭环数据集。"""
        info = parse_symbol(symbol)
        return {
            "price_history": self.get_price_history(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
            ),
            "market_cap_history": self.get_market_cap_history(symbol=symbol, period="近十年"),
            "financial_indicators": self.get_financial_indicators(symbol=symbol),
            "financial_indicators_quarterly": self.get_financial_indicators_quarterly(
                symbol=symbol
            ),
            "pe_history": self.get_pe_history(
                symbol=symbol, period="近十年", indicator="市盈率(TTM)"
            ),
            "hs300_pe_history": self.get_hs300_pe_history()
            if info.market == "CN"
            else pd.DataFrame(),
            "industry_pe_history": self.get_industry_pe_history(symbol=symbol),
            "industry_roe_history": self.get_industry_roe_history(symbol=symbol),
            "balance_components": self.get_balance_components(symbol=symbol, as_of_date=end_date),
        }

    def _get_cn_financial_indicators(self, info: SymbolInfo) -> pd.DataFrame:
        """获取 A 股核心财务字段并统一格式。"""
        profit_sheet = self._fetch_dataframe(
            endpoint_name="stock_profit_sheet_by_yearly_em",
            fetcher=lambda: ak.stock_profit_sheet_by_yearly_em(symbol=info.to_akshare_em_symbol()),
        )
        cash_flow_sheet = self._fetch_dataframe(
            endpoint_name="stock_cash_flow_sheet_by_yearly_em",
            fetcher=lambda: ak.stock_cash_flow_sheet_by_yearly_em(
                symbol=info.to_akshare_em_symbol()
            ),
        )
        balance_sheet = self._fetch_dataframe(
            endpoint_name="stock_balance_sheet_by_yearly_em",
            fetcher=lambda: ak.stock_balance_sheet_by_yearly_em(symbol=info.to_akshare_em_symbol()),
        )
        roe_sheet = self._fetch_optional_dataframe(
            endpoint_name="stock_financial_analysis_indicator",
            fetcher=lambda: ak.stock_financial_analysis_indicator(symbol=info.code),
        )

        merged = _merge_financial_frames(
            symbol=info.symbol,
            market=info.market,
            currency=info.currency,
            revenue_frame=_extract_metric_frame(
                profit_sheet,
                metric_name="revenue",
                date_candidates=["REPORT_DATE", "报告期", "date", "日期"],
                value_candidates=[
                    "TOTAL_OPERATE_INCOME",
                    "OPERATE_INCOME",
                    "营业总收入",
                    "主营业务收入",
                ],
            ),
            net_profit_frame=_extract_metric_frame(
                profit_sheet,
                metric_name="net_profit",
                date_candidates=["REPORT_DATE", "报告期", "date", "日期"],
                value_candidates=[
                    "PARENT_NETPROFIT",
                    "NETPROFIT",
                    "归属于母公司股东的净利润",
                    "净利润",
                ],
            ),
            ocf_frame=_extract_metric_frame(
                cash_flow_sheet,
                metric_name="ocf",
                date_candidates=["REPORT_DATE", "报告期", "date", "日期"],
                value_candidates=["NETCASH_OPERATE", "经营活动产生的现金流量净额"],
            ),
            total_assets_frame=_extract_metric_frame(
                balance_sheet,
                metric_name="total_assets",
                date_candidates=["REPORT_DATE", "报告期", "date", "日期"],
                value_candidates=["TOTAL_ASSETS", "资产总计"],
            ),
            total_liabilities_frame=_extract_metric_frame(
                balance_sheet,
                metric_name="total_liabilities",
                date_candidates=["REPORT_DATE", "报告期", "date", "日期"],
                value_candidates=["TOTAL_LIABILITIES", "负债合计"],
            ),
            roe_frame=_extract_metric_frame(
                roe_sheet,
                metric_name="roe",
                date_candidates=["日期", "date", "REPORT_DATE", "报告期"],
                value_candidates=["净资产收益率(%)", "ROE", "ROE_AVG", "净资产收益率"],
            ),
        )
        return _finalize_sorted(merged, FINANCIAL_COLUMNS)

    def _get_hk_financial_indicators(self, info: SymbolInfo) -> pd.DataFrame:
        """获取港股核心财务字段并统一格式。"""
        data = self._fetch_dataframe(
            endpoint_name="stock_financial_hk_analysis_indicator_em",
            fetcher=lambda: ak.stock_financial_hk_analysis_indicator_em(
                symbol=info.code,
                indicator="年度",
            ),
        )
        if data.empty:
            return _empty_dataframe(FINANCIAL_COLUMNS)

        result = pd.DataFrame(
            {
                "symbol": info.symbol,
                "date": _to_date_series(data, ["REPORT_DATE", "date", "日期"]),
                "revenue": _to_numeric_series(data, ["OPERATE_INCOME", "营业收入"]),
                "net_profit": _to_numeric_series(data, ["HOLDER_PROFIT", "净利润"]),
                "ocf": _to_numeric_series(data, ["PER_NETCASH_OPERATE", "经营现金流净额"]),
                "pe_ttm": pd.Series([pd.NA] * len(data)),
                "roe": _to_numeric_series(data, ["ROE_AVG", "ROE"]),
                "total_assets": _to_numeric_series(data, ["TOTAL_ASSETS", "资产总计"]),
                "total_liabilities": _to_numeric_series(data, ["TOTAL_LIABILITIES", "负债合计"]),
                "debt_to_asset": _normalize_debt_ratio(
                    _to_numeric_series(data, ["DEBT_ASSET_RATIO", "资产负债率"])
                ),
                "market": info.market,
                "currency": info.currency,
            }
        )
        result["debt_to_asset"] = _fill_debt_to_asset(result)
        return _finalize_sorted(result, FINANCIAL_COLUMNS)

    def _resolve_cn_industry_name(self, stock_code: str) -> str | None:
        """解析 A 股所属行业名称。"""
        if stock_code in self._industry_name_cache:
            return self._industry_name_cache[stock_code]
        info_df = self._fetch_optional_dataframe(
            endpoint_name="stock_individual_info_em",
            fetcher=lambda: ak.stock_individual_info_em(symbol=stock_code),
        )
        if info_df.empty:
            self._industry_name_cache[stock_code] = None
            return None
        if {"item", "value"}.issubset(set(info_df.columns)):
            matched = cast(
                pd.DataFrame,
                info_df.loc[info_df["item"].isin(["行业", "所属行业"]), :],
            )
            if not matched.empty:
                candidate = str(matched.iloc[0]["value"]).strip()
                resolved = candidate or None
                self._industry_name_cache[stock_code] = resolved
                return resolved
        if "行业" in info_df.columns:
            value = str(info_df["行业"].iloc[0]).strip()
            resolved = value or None
            self._industry_name_cache[stock_code] = resolved
            return resolved
        self._industry_name_cache[stock_code] = None
        return None

    def _get_cn_balance_components(self, info: SymbolInfo, as_of_date: str | None) -> pd.DataFrame:
        """提取 A 股单期资产负债结构。"""
        data = self._fetch_optional_dataframe(
            endpoint_name="stock_balance_sheet_by_report_em",
            fetcher=lambda: ak.stock_balance_sheet_by_report_em(symbol=info.to_akshare_em_symbol()),
        )
        if data.empty:
            data = self._fetch_optional_dataframe(
                endpoint_name="stock_balance_sheet_by_yearly_em",
                fetcher=lambda: ak.stock_balance_sheet_by_yearly_em(
                    symbol=info.to_akshare_em_symbol()
                ),
            )
        if data.empty:
            return _empty_dataframe(BALANCE_COMPONENT_COLUMNS)

        row, report_date = _pick_balance_row(data, as_of_date=as_of_date)
        if row is None or report_date is None:
            return _empty_dataframe(BALANCE_COMPONENT_COLUMNS)

        asset_values = {
            "现金": _sum_row_candidates(row, ["MONETARYFUNDS", "货币资金"]),
            "应收款": _sum_row_candidates(
                row,
                [
                    "ACCOUNTS_RECE",
                    "应收账款",
                    "NOTES_RECE",
                    "应收票据",
                    "RECEIVABLE_FINANCING",
                    "应收款项融资",
                ],
            ),
            "预付款": _sum_row_candidates(row, ["PREPAYMENT", "预付款项"]),
            "存货": _sum_row_candidates(row, ["INVENTORY", "存货"]),
            "长期投资": _sum_row_candidates(
                row,
                ["LONG_TERM_EQUITY_INVEST", "长期股权投资", "INVEST_REAL_ESTATE", "投资性房地产"],
            ),
            "固定资产": _sum_row_candidates(row, ["FIXED_ASSET", "固定资产"]),
            "无形资产/商誉": _sum_row_candidates(
                row, ["INTANGIBLE_ASSET", "无形资产", "GOODWILL", "商誉"]
            ),
        }
        total_assets = _sum_row_candidates(row, ["TOTAL_ASSETS", "资产总计"])
        asset_core_sum = sum(asset_values.values())
        asset_values["其他资产"] = _non_negative(total_assets - asset_core_sum)

        liability_values = {
            "应付账款": _sum_row_candidates(
                row,
                ["ACCOUNTS_PAYABLE", "应付账款", "NOTES_PAYABLE", "应付票据"],
            ),
            "合同负债/预收款": _sum_row_candidates(
                row,
                ["CONTRACT_LIAB", "合同负债", "ADVANCE_RECEIVABLES", "预收款项"],
            ),
            "应付职工薪酬": _sum_row_candidates(row, ["STAFF_SALARY_PAYABLE", "应付职工薪酬"]),
            "应交税费": _sum_row_candidates(row, ["TAX_PAYABLE", "应交税费"]),
            "有息负债": _sum_row_candidates(
                row,
                ["SHORT_LOAN", "短期借款", "LONG_LOAN", "长期借款", "BOND_PAYABLE", "应付债券"],
            ),
        }
        total_liabilities = _sum_row_candidates(row, ["TOTAL_LIABILITIES", "负债合计"])
        liability_core_sum = sum(liability_values.values())
        liability_values["其他负债"] = _non_negative(total_liabilities - liability_core_sum)

        rows: list[dict[str, object]] = []
        for category, value in asset_values.items():
            rows.append(
                {
                    "symbol": info.symbol,
                    "date": report_date,
                    "category": category,
                    "side": "asset",
                    "value": value,
                    "market": info.market,
                    "currency": info.currency,
                }
            )
        for category, value in liability_values.items():
            rows.append(
                {
                    "symbol": info.symbol,
                    "date": report_date,
                    "category": category,
                    "side": "liability",
                    "value": value,
                    "market": info.market,
                    "currency": info.currency,
                }
            )
        return _finalize_sorted(pd.DataFrame(rows), BALANCE_COMPONENT_COLUMNS)

    def _get_hk_balance_components(self, info: SymbolInfo, as_of_date: str | None) -> pd.DataFrame:
        """提取港股单期资产负债结构。"""
        data = self._fetch_optional_dataframe(
            endpoint_name="stock_financial_hk_report_em",
            fetcher=lambda: ak.stock_financial_hk_report_em(
                stock=info.code,
                symbol="资产负债表",
                indicator="年度",
            ),
        )
        if data.empty:
            return _empty_dataframe(BALANCE_COMPONENT_COLUMNS)
        required = {"STD_ITEM_NAME", "AMOUNT", "STD_REPORT_DATE"}
        if not required.issubset(set(data.columns)):
            return _empty_dataframe(BALANCE_COMPONENT_COLUMNS)

        prepared = data.copy()
        prepared["__date__"] = pd.to_datetime(prepared["STD_REPORT_DATE"], errors="coerce")
        prepared = cast(pd.DataFrame, prepared.loc[prepared["__date__"].notna(), :])
        if prepared.empty:
            return _empty_dataframe(BALANCE_COMPONENT_COLUMNS)
        prepared = cast(pd.DataFrame, prepared.sort_values(by="__date__"))

        target_date = pd.to_datetime(as_of_date, errors="coerce") if as_of_date else pd.NaT
        if pd.isna(target_date):
            chosen_date = cast(pd.Timestamp, prepared["__date__"].iloc[-1])
        else:
            matched = cast(pd.DataFrame, prepared.loc[prepared["__date__"] <= target_date, :])
            chosen_date = (
                cast(pd.Timestamp, matched["__date__"].iloc[-1])
                if not matched.empty
                else cast(pd.Timestamp, prepared["__date__"].iloc[-1])
            )
        snapshot = cast(pd.DataFrame, prepared.loc[prepared["__date__"] == chosen_date, :])

        asset_values = {
            "现金": _sum_keyword_amount(snapshot, ["货币资金", "现金及现金等价物"]),
            "应收款": _sum_keyword_amount(snapshot, ["应收账款", "应收票据", "应收款项"]),
            "预付款": _sum_keyword_amount(snapshot, ["预付款"]),
            "存货": _sum_keyword_amount(snapshot, ["存货"]),
            "长期投资": _sum_keyword_amount(snapshot, ["长期投资", "股权投资", "投资物业"]),
            "固定资产": _sum_keyword_amount(snapshot, ["固定资产", "物业、厂房及设备"]),
            "无形资产/商誉": _sum_keyword_amount(snapshot, ["无形资产", "商誉"]),
        }
        total_assets = _sum_keyword_amount(snapshot, ["资产总计", "总资产"])
        asset_core_sum = sum(asset_values.values())
        asset_values["其他资产"] = _non_negative(total_assets - asset_core_sum)

        liability_values = {
            "应付账款": _sum_keyword_amount(snapshot, ["应付账款", "应付票据"]),
            "合同负债/预收款": _sum_keyword_amount(snapshot, ["合同负债", "预收", "递延收入"]),
            "应付职工薪酬": _sum_keyword_amount(snapshot, ["应付职工", "薪酬"]),
            "应交税费": _sum_keyword_amount(snapshot, ["应交税费", "税项"]),
            "有息负债": _sum_keyword_amount(snapshot, ["借款", "应付债券", "租赁负债"]),
        }
        total_liabilities = _sum_keyword_amount(snapshot, ["负债合计", "总负债"])
        liability_core_sum = sum(liability_values.values())
        liability_values["其他负债"] = _non_negative(total_liabilities - liability_core_sum)

        report_date = cast(pd.Timestamp, chosen_date).strftime("%Y-%m-%d")
        rows: list[dict[str, object]] = []
        for category, value in asset_values.items():
            rows.append(
                {
                    "symbol": info.symbol,
                    "date": report_date,
                    "category": category,
                    "side": "asset",
                    "value": value,
                    "market": info.market,
                    "currency": info.currency,
                }
            )
        for category, value in liability_values.items():
            rows.append(
                {
                    "symbol": info.symbol,
                    "date": report_date,
                    "category": category,
                    "side": "liability",
                    "value": value,
                    "market": info.market,
                    "currency": info.currency,
                }
            )
        return _finalize_sorted(pd.DataFrame(rows), BALANCE_COMPONENT_COLUMNS)

    def _fetch_dataframe(self, endpoint_name: str, fetcher: DataFrameFetcher) -> pd.DataFrame:
        """带重试获取 DataFrame；严格模式失败抛错。"""
        last_error: Exception | None = None
        for attempt in range(1, self.retries + 1):
            try:
                data = fetcher()
                if isinstance(data, pd.DataFrame):
                    return data
                raise TypeError(f"{endpoint_name} 返回类型非 DataFrame: {type(data)!r}")
            except (
                requests.exceptions.RequestException,
                ConnectionError,
                TimeoutError,
                ssl.SSLError,
            ) as exc:
                last_error = exc
                if attempt < self.retries:
                    time.sleep(self.base_sleep_seconds * attempt)
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                break

        message = _build_endpoint_error(endpoint_name, self.retries, last_error)
        if self.strict:
            raise RuntimeError(message) from last_error
        warnings.warn(message, stacklevel=2)
        return pd.DataFrame()

    def _fetch_optional_dataframe(
        self,
        endpoint_name: str,
        fetcher: DataFrameFetcher,
    ) -> pd.DataFrame:
        """以非严格模式获取可选数据。"""
        strict_before = self.strict
        self.strict = False
        try:
            return self._fetch_dataframe(endpoint_name=endpoint_name, fetcher=fetcher)
        finally:
            self.strict = strict_before

    def _industry_cache_path(self, cache_type: str, symbol: str) -> Path:
        """返回行业缓存文件路径。"""
        safe_symbol = symbol.replace(".", "_")
        return self.industry_cache_dir / f"{safe_symbol}-{cache_type}.csv"

    def _save_industry_cache(self, cache_type: str, symbol: str, frame: pd.DataFrame) -> None:
        """保存行业缓存快照。"""
        if frame.empty:
            return
        cache_path = self._industry_cache_path(cache_type=cache_type, symbol=symbol)
        try:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            frame.to_csv(cache_path, index=False)
            logger.debug("行业缓存已写入: path=%s, rows=%d", cache_path, len(frame))
        except Exception as exc:  # noqa: BLE001
            warnings.warn(
                f"行业缓存写入失败 {cache_path}: {type(exc).__name__}: {exc}",
                stacklevel=2,
            )

    def _load_industry_cache(
        self,
        cache_type: str,
        symbol: str,
        columns: list[str],
    ) -> pd.DataFrame:
        """读取行业缓存快照。"""
        cache_path = self._industry_cache_path(cache_type=cache_type, symbol=symbol)
        if not cache_path.exists():
            logger.debug("行业缓存未命中: path=%s", cache_path)
            return _empty_dataframe(columns)
        try:
            data = pd.read_csv(cache_path)
            logger.info("行业缓存命中: path=%s, rows=%d", cache_path, len(data))
        except Exception as exc:  # noqa: BLE001
            warnings.warn(
                f"行业缓存读取失败 {cache_path}: {type(exc).__name__}: {exc}",
                stacklevel=2,
            )
            return _empty_dataframe(columns)
        return _finalize_sorted(data, columns)


def _to_yyyymmdd(value: str | None) -> str:
    """将输入日期转换为 `YYYYMMDD`。"""
    if value is None:
        return datetime.now().strftime("%Y%m%d")
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        raise ValueError(f"无法解析日期: {value}")
    return parsed.strftime("%Y%m%d")


def _to_date_series(data: pd.DataFrame, candidates: list[str]) -> pd.Series:
    """从候选列中提取并标准化日期列。"""
    series = _pick_series(data, candidates)
    parsed = pd.Series(pd.to_datetime(series, errors="coerce"), index=series.index)
    return parsed.dt.strftime("%Y-%m-%d")


def _to_numeric_series(data: pd.DataFrame, candidates: list[str]) -> pd.Series:
    """从候选列中提取并转换为数值列。"""
    series = _pick_series(data, candidates)
    numeric = pd.to_numeric(series, errors="coerce")
    return pd.Series(numeric, index=series.index)


def _pick_series(data: pd.DataFrame, candidates: list[str]) -> pd.Series:
    """按优先级选择第一个存在的列。"""
    for name in candidates:
        if name in data.columns:
            column = data[name]
            if isinstance(column, pd.Series):
                return column
            return pd.Series(column, index=data.index)
    return pd.Series([pd.NA] * len(data), index=data.index, dtype="object")


def _filter_by_date_range(
    data: pd.DataFrame,
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    """按日期区间过滤 DataFrame（闭区间）。"""
    if data.empty:
        return data

    result = cast(pd.DataFrame, data.copy())
    date_series = pd.Series(pd.to_datetime(result["date"], errors="coerce"), index=result.index)
    result = cast(pd.DataFrame, result.assign(date=date_series))
    if start_date is not None:
        start_dt = pd.to_datetime(start_date, errors="coerce")
        if not pd.isna(start_dt):
            mask = cast(pd.Series, result["date"]) >= start_dt
            result = cast(pd.DataFrame, result.loc[mask, :])
    if end_date is not None:
        end_dt = pd.to_datetime(end_date, errors="coerce")
        if not pd.isna(end_dt):
            mask = cast(pd.Series, result["date"]) <= end_dt
            result = cast(pd.DataFrame, result.loc[mask, :])
    normalized_date = pd.Series(
        pd.to_datetime(result["date"], errors="coerce"),
        index=result.index,
    ).dt.strftime("%Y-%m-%d")
    result = cast(pd.DataFrame, result.assign(date=normalized_date))
    return cast(pd.DataFrame, result)


def _extract_metric_frame(
    frame: pd.DataFrame,
    metric_name: str,
    date_candidates: list[str],
    value_candidates: list[str],
) -> pd.DataFrame:
    """从原始财务表中提取单指标时间序列。"""
    if frame.empty:
        return pd.DataFrame(columns=["date", metric_name])
    return pd.DataFrame(
        {
            "date": _to_date_series(frame, date_candidates),
            metric_name: _to_numeric_series(frame, value_candidates),
        }
    )


def _merge_financial_frames(
    symbol: str,
    market: str,
    currency: str,
    revenue_frame: pd.DataFrame,
    net_profit_frame: pd.DataFrame,
    ocf_frame: pd.DataFrame,
    total_assets_frame: pd.DataFrame,
    total_liabilities_frame: pd.DataFrame,
    roe_frame: pd.DataFrame,
) -> pd.DataFrame:
    """将多个指标时间序列按日期合并成统一财务表。"""
    merged = revenue_frame.copy()
    for extra in [
        net_profit_frame,
        ocf_frame,
        total_assets_frame,
        total_liabilities_frame,
        roe_frame,
    ]:
        merged = merged.merge(extra, on="date", how="outer")

    if "pe_ttm" not in merged.columns:
        merged["pe_ttm"] = pd.NA
    merged["symbol"] = symbol
    merged["market"] = market
    merged["currency"] = currency
    merged["debt_to_asset"] = _fill_debt_to_asset(merged)
    return merged


def _fill_debt_to_asset(data: pd.DataFrame) -> pd.Series:
    """优先使用资产负债计算负债率，缺失时保留已有值。"""
    total_assets = _to_numeric_column(data, "total_assets")
    total_liabilities = _to_numeric_column(data, "total_liabilities")
    calculated = total_liabilities / total_assets
    existing = _normalize_debt_ratio(_to_numeric_column(data, "debt_to_asset"))
    return calculated.where(calculated.notna(), existing)


def _normalize_debt_ratio(series: pd.Series) -> pd.Series:
    """将可能是百分数的负债率统一转换为 0-1 区间。"""
    normalized = pd.Series(pd.to_numeric(series, errors="coerce"), index=series.index)
    percent_mask = normalized > 1
    normalized.loc[percent_mask] = normalized.loc[percent_mask] / 100.0
    return normalized


def _to_numeric_column(data: pd.DataFrame, column_name: str) -> pd.Series:
    """从 DataFrame 中提取单列并转换为数值 Series。"""
    if column_name in data.columns:
        source = data[column_name]
    else:
        source = pd.Series([pd.NA] * len(data), index=data.index)
    numeric = pd.to_numeric(source, errors="coerce")
    return pd.Series(numeric, index=data.index)


def _finalize_sorted(data: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """清理空日期、按日期排序并统一列顺序。"""
    result = cast(pd.DataFrame, data.copy())
    for column in columns:
        if column not in result.columns:
            result[column] = pd.NA
    result = cast(pd.DataFrame, result[columns])
    date_series = pd.Series(result["date"], index=result.index)
    result = cast(pd.DataFrame, result.loc[date_series.notna(), :])
    result = cast(pd.DataFrame, result.sort_values(by="date").reset_index(drop=True))
    return cast(pd.DataFrame, result)


def _empty_dataframe(columns: list[str]) -> pd.DataFrame:
    """创建指定结构的空 DataFrame。"""
    return pd.DataFrame(columns=columns)


def _build_endpoint_error(endpoint_name: str, retries: int, error: Exception | None) -> str:
    """构造统一接口错误信息。"""
    if error is None:
        return f"{endpoint_name} 获取失败，重试 {retries} 次后仍无结果。"
    return f"{endpoint_name} 获取失败，重试 {retries} 次后仍失败：{type(error).__name__}: {error}"


def _normalize_baidu_valuation_period(
    market: str,
    period: str | None,
    default_period: str,
) -> str:
    """将估值周期归一到接口支持集合。"""
    if market == "CN":
        supported = {"近一年", "近三年", "近五年", "近十年", "全部"}
        fallback = default_period if default_period in supported else "近十年"
    else:
        supported = {"近一年", "近三年", "全部"}
        fallback = "全部"
    if period in supported:
        return period
    return fallback


def _quarter_end_candidates(max_years_back: int = 8) -> list[str]:
    """生成近若干年的季末日期列表（倒序）。"""
    today = date.today()
    candidates: list[str] = []
    for year in range(today.year, today.year - max_years_back - 1, -1):
        for month, day_of_month in ((12, 31), (9, 30), (6, 30), (3, 31)):
            current = date(year, month, day_of_month)
            if current <= today:
                candidates.append(current.strftime("%Y%m%d"))
    return candidates


def _pick_industry_row(snapshot: pd.DataFrame, industry_name: str) -> pd.Series | None:
    """在行业快照中尽量匹配目标行业行。"""
    if snapshot.empty or "行业名称" not in snapshot.columns:
        return None
    normalized = snapshot.copy()
    normalized["__name__"] = normalized["行业名称"].astype(str).str.strip()
    target = industry_name.strip()
    exact = cast(pd.DataFrame, normalized.loc[normalized["__name__"] == target, :])
    if exact.empty:
        fuzzy_mask = normalized["__name__"].apply(
            lambda text: isinstance(text, str) and (target in text or text in target)
        )
        fuzzy = cast(
            pd.DataFrame,
            normalized.loc[fuzzy_mask, :],
        )
        if fuzzy.empty:
            return None
        exact = fuzzy
    if "行业层级" in exact.columns:
        levels = pd.to_numeric(exact["行业层级"], errors="coerce")
        if levels.notna().any():
            max_level = float(levels.max())
            exact = cast(pd.DataFrame, exact.loc[levels == max_level, :])
    return exact.iloc[0]


def _safe_float_from_series(row: pd.Series, candidates: list[str]) -> float | None:
    """从行对象按优先级提取浮点值。"""
    for key in candidates:
        if key in row.index:
            value = pd.to_numeric(row[key], errors="coerce")
            if not pd.isna(value):
                return float(value)
    return None


def _safe_str_from_series(row: pd.Series, candidates: list[str]) -> str | None:
    """从行对象按优先级提取字符串。"""
    for key in candidates:
        if key in row.index:
            value = str(row[key]).strip()
            if value and value.lower() != "nan":
                return value
    return None


def _pick_balance_row(
    frame: pd.DataFrame,
    as_of_date: str | None = None,
) -> tuple[pd.Series | None, str | None]:
    """从资产负债表中选择目标期行。"""
    if frame.empty:
        return None, None
    date_series = pd.Series(
        pd.to_datetime(
            _pick_series(frame, ["REPORT_DATE", "报告期", "date", "日期"]),
            errors="coerce",
        ),
        index=frame.index,
    )
    valid_mask = date_series.notna()
    if not valid_mask.any():
        return None, None
    date_valid = cast(pd.Series, date_series.loc[valid_mask])
    ordered_index = date_valid.sort_values().index
    target = pd.to_datetime(as_of_date, errors="coerce") if as_of_date else pd.NaT
    if pd.isna(target):
        selected_idx = ordered_index[-1]
        selected = frame.loc[selected_idx]
        selected_date = cast(pd.Timestamp, date_series.loc[selected_idx]).strftime("%Y-%m-%d")
        return selected, selected_date

    target_series = cast(pd.Series, date_series.loc[ordered_index])
    exact_index = target_series[target_series == target].index
    if len(exact_index) > 0:
        selected_idx = exact_index[-1]
        selected = frame.loc[selected_idx]
        selected_date = cast(pd.Timestamp, date_series.loc[selected_idx]).strftime("%Y-%m-%d")
        return selected, selected_date

    historical_index = target_series[target_series <= target].index
    if len(historical_index) > 0:
        selected_idx = historical_index[-1]
    else:
        selected_idx = ordered_index[-1]
    selected = frame.loc[selected_idx]
    selected_date = cast(pd.Timestamp, date_series.loc[selected_idx]).strftime("%Y-%m-%d")
    return selected, selected_date


def _sum_row_candidates(row: pd.Series, candidates: list[str]) -> float:
    """对行内多候选列求和。"""
    values: list[float] = []
    for key in candidates:
        if key not in row.index:
            continue
        value = pd.to_numeric(row[key], errors="coerce")
        if pd.isna(value):
            continue
        values.append(float(value))
    if not values:
        return 0.0
    return float(sum(values))


def _sum_keyword_amount(frame: pd.DataFrame, keywords: list[str]) -> float:
    """在行式报表中按关键词汇总金额。"""
    if frame.empty:
        return 0.0
    if not {"STD_ITEM_NAME", "AMOUNT"}.issubset(set(frame.columns)):
        return 0.0
    names = frame["STD_ITEM_NAME"].astype(str)
    mask = pd.Series(False, index=frame.index)
    for keyword in keywords:
        mask = mask | names.str.contains(keyword, regex=False, na=False)
    matched = cast(pd.DataFrame, frame.loc[mask, :])
    if matched.empty:
        return 0.0
    numeric = pd.to_numeric(matched["AMOUNT"], errors="coerce").dropna()
    if numeric.empty:
        return 0.0
    return float(numeric.sum())


def _non_negative(value: float | None) -> float:
    """将差额值限制为非负。"""
    if value is None:
        return 0.0
    return float(max(value, 0.0))
