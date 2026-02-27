"""生成结构化 Markdown 投研报告（含关键图表）。"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
import time
from typing import Any

import akshare as ak
import matplotlib
import pandas as pd

matplotlib.use("Agg")
import matplotlib.pyplot as plt


@dataclass(frozen=True)
class SymbolMeta:
    raw: str
    market: str  # "a" | "hk"
    code: str

    @property
    def a_hist_symbol(self) -> str:
        if self.market != "a":
            raise ValueError("仅 A 股支持该属性")
        exchange = self.raw.split(".")[1].lower()
        return f"{exchange}{self.code}"

    @property
    def a_em_symbol(self) -> str:
        if self.market != "a":
            raise ValueError("仅 A 股支持该属性")
        exchange = self.raw.split(".")[1].upper()
        return f"{exchange}{self.code}"


def _parse_symbol(symbol: str) -> SymbolMeta:
    text = symbol.strip().upper()
    if "." in text:
        code, suffix = text.split(".", 1)
        if suffix in {"SH", "SZ"}:
            return SymbolMeta(raw=f"{code}.{suffix}", market="a", code=code)
        if suffix == "HK":
            return SymbolMeta(raw=f"{code}.{suffix}", market="hk", code=code.zfill(5))
    if text.isdigit() and len(text) == 6:
        # 默认按 A 股处理，无法推断交易所时按 SH 回退。
        return SymbolMeta(raw=f"{text}.SH", market="a", code=text)
    if text.isdigit() and len(text) <= 5:
        return SymbolMeta(raw=f"{text.zfill(5)}.HK", market="hk", code=text.zfill(5))
    raise ValueError(f"无法识别 symbol: {symbol}")


def _safe_to_datetime(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce")


def _safe_to_numeric(series: pd.Series) -> pd.Series:
    normalized = (
        series.astype(str)
        .str.replace(",", "", regex=False)
        .str.replace("%", "", regex=False)
        .str.replace("--", "", regex=False)
        .str.replace("nan", "", regex=False)
        .str.strip()
    )
    return pd.to_numeric(normalized, errors="coerce")


def _find_column(df: pd.DataFrame, keywords: list[str]) -> str | None:
    columns = [str(c) for c in df.columns]
    for key in keywords:
        for col in columns:
            if key in col:
                return col
    return None


def _find_best_column(
    df: pd.DataFrame, exact_candidates: list[str], fuzzy_candidates: list[str] | None = None
) -> str | None:
    columns = [str(c) for c in df.columns]
    for candidate in exact_candidates:
        if candidate in columns:
            return candidate
    if fuzzy_candidates:
        return _find_column(df, fuzzy_candidates)
    return None


def _fetch_price(meta: SymbolMeta, start_date: str, end_date: str) -> pd.DataFrame:
    if meta.market == "a":
        df = _call_with_retry(
            lambda: ak.stock_zh_a_hist_tx(
                symbol=meta.a_hist_symbol,
                start_date=start_date.replace("-", ""),
                end_date=end_date.replace("-", ""),
            )
        )
        date_col, close_col = "date", "close"
    else:
        df = _call_with_retry(lambda: ak.stock_hk_daily(symbol=meta.code))
        date_col, close_col = "date", "close"
        df[date_col] = _safe_to_datetime(df[date_col])
        df = df[df[date_col] >= pd.Timestamp(start_date)]
        df = df[df[date_col] <= pd.Timestamp(end_date)]

    out = pd.DataFrame(
        {
            "date": _safe_to_datetime(df[date_col]),
            "close": _safe_to_numeric(df[close_col]),
        }
    ).dropna(subset=["date", "close"])
    return out.sort_values("date")


def _fetch_pe(meta: SymbolMeta) -> pd.DataFrame:
    try:
        if meta.market == "a":
            df = _call_with_retry(
                lambda: ak.stock_zh_valuation_baidu(
                    symbol=meta.code, indicator="市盈率(TTM)", period="近一年"
                )
            )
        else:
            df = _call_with_retry(
                lambda: ak.stock_hk_valuation_baidu(
                    symbol=meta.code, indicator="市盈率(TTM)", period="近一年"
                )
            )
    except Exception:
        return pd.DataFrame(columns=["date", "pe_ttm"])
    out = pd.DataFrame(
        {
            "date": _safe_to_datetime(df["date"]),
            "pe_ttm": _safe_to_numeric(df["value"]),
        }
    ).dropna(subset=["date", "pe_ttm"])
    return out.sort_values("date")


def _build_financial_panel(meta: SymbolMeta) -> pd.DataFrame:
    if meta.market == "a":
        profit = _call_with_retry(lambda: ak.stock_profit_sheet_by_yearly_em(symbol=meta.a_em_symbol))
        cashflow = _call_with_retry(lambda: ak.stock_cash_flow_sheet_by_yearly_em(symbol=meta.a_em_symbol))
        balance = _call_with_retry(lambda: ak.stock_balance_sheet_by_yearly_em(symbol=meta.a_em_symbol))

        profit_date_col = _find_column(profit, ["REPORT_DATE", "报告期"])
        cash_date_col = _find_column(cashflow, ["REPORT_DATE", "报告期"])
        balance_date_col = _find_column(balance, ["REPORT_DATE", "报告期"])

        revenue_col = _find_best_column(
            profit,
            exact_candidates=["TOTAL_OPERATE_INCOME", "OPERATE_INCOME"],
            fuzzy_candidates=["营业总收入", "营业收入"],
        )
        net_profit_col = _find_best_column(
            profit,
            exact_candidates=["PARENT_NETPROFIT", "NETPROFIT", "HOLDER_PROFIT"],
            fuzzy_candidates=["归属于母公司股东的净利润", "净利润"],
        )
        ocf_col = _find_best_column(
            cashflow,
            exact_candidates=["NETCASH_OPERATE"],
            fuzzy_candidates=["经营活动产生的现金流量净额", "经营活动现金流量净额"],
        )
        assets_col = _find_best_column(
            balance, exact_candidates=["ASSET_BALANCE", "TOTAL_ASSETS"], fuzzy_candidates=["资产总计"]
        )
        liabilities_col = _find_best_column(
            balance, exact_candidates=["LIAB_BALANCE", "TOTAL_LIABILITIES"], fuzzy_candidates=["负债合计"]
        )

        if not all(
            [
                profit_date_col,
                cash_date_col,
                balance_date_col,
                revenue_col,
                net_profit_col,
                assets_col,
                liabilities_col,
            ]
        ):
            raise ValueError("A 股财务字段未能完整识别，请检查上游字段变化")

        profit_panel = pd.DataFrame(
            {
                "date": _safe_to_datetime(profit[profit_date_col]),
                "revenue": _safe_to_numeric(profit[revenue_col]),
                "net_profit": _safe_to_numeric(profit[net_profit_col]),
            }
        )
        cash_panel = pd.DataFrame(
            {
                "date": _safe_to_datetime(cashflow[cash_date_col]),
                "ocf": _safe_to_numeric(cashflow[ocf_col]) if ocf_col else pd.NA,
            }
        )
        balance_panel = pd.DataFrame(
            {
                "date": _safe_to_datetime(balance[balance_date_col]),
                "total_assets": _safe_to_numeric(balance[assets_col]),
                "total_liabilities": _safe_to_numeric(balance[liabilities_col]),
            }
        )
        merged = (
            profit_panel.merge(cash_panel, on="date", how="outer")
            .merge(balance_panel, on="date", how="outer")
            .dropna(subset=["date"])
        )
    else:
        hk = _call_with_retry(
            lambda: ak.stock_financial_hk_analysis_indicator_em(symbol=meta.code, indicator="年度")
        )
        date_col = _find_column(hk, ["REPORT_DATE", "报告期"])
        revenue_col = _find_column(hk, ["OPERATE_INCOME", "营业总收入", "营业收入"])
        net_profit_col = _find_column(hk, ["HOLDER_PROFIT", "净利润"])
        roe_col = _find_column(hk, ["ROE_AVG", "ROE"])

        if not all([date_col, revenue_col, net_profit_col]):
            raise ValueError("港股财务字段未能完整识别，请检查上游字段变化")

        merged = pd.DataFrame(
            {
                "date": _safe_to_datetime(hk[date_col]),
                "revenue": _safe_to_numeric(hk[revenue_col]),
                "net_profit": _safe_to_numeric(hk[net_profit_col]),
                "roe": _safe_to_numeric(hk[roe_col]) if roe_col else pd.NA,
            }
        )

    merged = merged.dropna(subset=["date"]).sort_values("date")
    if {"total_assets", "total_liabilities"}.issubset(merged.columns):
        merged["debt_to_asset"] = merged["total_liabilities"] / merged["total_assets"]
    return merged


def _plot_line(
    df: pd.DataFrame,
    x_col: str,
    y_col: str,
    title: str,
    y_label: str,
    output_path: Path,
) -> None:
    plt.figure(figsize=(9, 4))
    plt.plot(df[x_col], df[y_col], linewidth=2)
    plt.title(title)
    plt.xlabel("日期")
    plt.ylabel(y_label)
    plt.grid(alpha=0.3)
    plt.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=150)
    plt.close()


def _plot_financial(financial: pd.DataFrame, output_path: Path) -> None:
    plt.figure(figsize=(9, 4))
    plt.bar(financial["date"], financial["revenue"], alpha=0.5, label="营收")
    plt.plot(financial["date"], financial["net_profit"], linewidth=2, label="净利润")
    plt.title("营收与净利润趋势")
    plt.xlabel("报告期")
    plt.ylabel("金额")
    plt.legend()
    plt.grid(alpha=0.3)
    plt.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=150)
    plt.close()


def _plot_debt_or_roe(financial: pd.DataFrame, meta: SymbolMeta, output_path: Path) -> tuple[str, str]:
    plt.figure(figsize=(9, 4))
    if "debt_to_asset" in financial.columns and financial["debt_to_asset"].notna().any():
        y_col = "debt_to_asset"
        title = "资产负债率趋势"
        y_label = "资产负债率"
    elif "roe" in financial.columns and financial["roe"].notna().any():
        y_col = "roe"
        title = "ROE 趋势"
        y_label = "ROE"
    else:
        y_col = "net_profit"
        title = "净利润趋势"
        y_label = "净利润"
    series = financial[y_col]
    plt.plot(financial["date"], series, linewidth=2)
    plt.title(title)
    plt.xlabel("报告期")
    plt.ylabel(y_label)
    plt.grid(alpha=0.3)
    plt.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=150)
    plt.close()
    return title, y_col


def _fmt(value: float | int | None, ndigits: int = 2) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    return f"{value:,.{ndigits}f}"


def _build_markdown(
    meta: SymbolMeta,
    price_df: pd.DataFrame,
    pe_df: pd.DataFrame,
    fin_df: pd.DataFrame,
    has_pe_chart: bool,
    report_date: str,
) -> str:
    latest_price = price_df.iloc[-1] if not price_df.empty else None
    latest_pe = pe_df.iloc[-1] if not pe_df.empty else None

    latest_revenue_row = (
        fin_df.dropna(subset=["revenue"]).sort_values("date").iloc[-1]
        if "revenue" in fin_df.columns and fin_df["revenue"].notna().any()
        else None
    )
    latest_profit_row = (
        fin_df.dropna(subset=["net_profit"]).sort_values("date").iloc[-1]
        if "net_profit" in fin_df.columns and fin_df["net_profit"].notna().any()
        else None
    )
    latest_fin = fin_df.sort_values("date").iloc[-1] if not fin_df.empty else None

    price_chart = "charts/price_trend.png"
    pe_chart = "charts/pe_trend.png"
    fin_chart = "charts/financial_trend.png"
    risk_chart = "charts/risk_trend.png"

    lines = [
        f"# {meta.raw} 结构化投研报告",
        "",
        f"- 生成日期：{report_date}",
        "- 数据源：AkShare",
        "",
        "## 1. 执行摘要",
        "",
        f"- 最新收盘价：{_fmt(float(latest_price['close'])) if latest_price is not None else 'N/A'}",
        f"- 最新 PE(TTM)：{_fmt(float(latest_pe['pe_ttm'])) if latest_pe is not None else 'N/A'}",
        f"- 最新营收：{_fmt(float(latest_revenue_row['revenue'])) if latest_revenue_row is not None else 'N/A'}",
        f"- 最新净利润：{_fmt(float(latest_profit_row['net_profit'])) if latest_profit_row is not None else 'N/A'}",
        "",
        "## 2. 核心数据快照",
        "",
        "| 指标 | 数值 |",
        "| --- | --- |",
        f"| 收盘价日期 | {latest_price['date'].date() if latest_price is not None else 'N/A'} |",
        f"| PE 日期 | {latest_pe['date'].date() if latest_pe is not None else 'N/A'} |",
        f"| 财报日期 | {latest_fin['date'].date() if latest_fin is not None else 'N/A'} |",
        "",
        "## 3. 关键图表",
        "",
        "### 3.1 价格趋势",
        "",
        f"![价格趋势]({price_chart})",
        "",
        "### 3.2 估值趋势（PE TTM）",
        "",
        f"![PE趋势]({pe_chart})" if has_pe_chart else "PE 数据拉取失败或为空，未生成该图。",
        "",
        "### 3.3 营收与净利润趋势",
        "",
        f"![营收净利润趋势]({fin_chart})",
        "",
        "### 3.4 风险指标趋势",
        "",
        f"![风险趋势]({risk_chart})",
        "",
        "## 4. 结论与跟踪建议",
        "",
        "- 建议结合林奇框架继续跟踪增长质量、估值区间与风险信号。",
        "- 定性维度（管理层、渠道、护城河）建议结合公告与调研纪要人工复核。",
    ]
    return "\n".join(lines)


def _call_with_retry(fetcher: Any, retries: int = 2, base_sleep: float = 1.2) -> Any:
    last_error: Exception | None = None
    for idx in range(retries + 1):
        try:
            return fetcher()
        except Exception as err:
            last_error = err
            if idx < retries:
                time.sleep(base_sleep * (idx + 1))
    raise RuntimeError(f"请求失败，重试后仍未成功: {last_error}")


def generate_report(
    symbol: str,
    start_date: str,
    end_date: str,
    report_date: str,
    out_root: Path,
) -> Path:
    meta = _parse_symbol(symbol)
    base_dir = out_root / report_date / meta.raw.replace(".", "_")
    chart_dir = base_dir / "charts"
    base_dir.mkdir(parents=True, exist_ok=True)
    chart_dir.mkdir(parents=True, exist_ok=True)

    price_df = _fetch_price(meta, start_date=start_date, end_date=end_date)
    pe_df = _fetch_pe(meta)
    fin_df = _build_financial_panel(meta)

    if price_df.empty or pe_df.empty or fin_df.empty:
        if price_df.empty or fin_df.empty:
            raise ValueError("关键数据为空（价格或财务），无法生成报告")

    _plot_line(price_df, "date", "close", "价格趋势", "收盘价", chart_dir / "price_trend.png")
    has_pe_chart = not pe_df.empty
    if has_pe_chart:
        _plot_line(pe_df, "date", "pe_ttm", "PE(TTM) 趋势", "PE(TTM)", chart_dir / "pe_trend.png")
    _plot_financial(fin_df, chart_dir / "financial_trend.png")
    _plot_debt_or_roe(fin_df, meta, chart_dir / "risk_trend.png")

    markdown = _build_markdown(
        meta,
        price_df,
        pe_df,
        fin_df,
        has_pe_chart=has_pe_chart,
        report_date=report_date,
    )
    report_path = base_dir / f"report-{meta.raw.replace('.', '_')}-{report_date}.md"
    report_path.write_text(markdown, encoding="utf-8")
    return report_path


def main() -> None:
    parser = argparse.ArgumentParser(description="生成结构化 Markdown 报告（含图表）")
    parser.add_argument("--symbol", default="600519.SH", help="股票代码，如 600519.SH 或 00700.HK")
    parser.add_argument("--start-date", default="2024-01-01", help="起始日期，YYYY-MM-DD")
    parser.add_argument("--end-date", default=date.today().isoformat(), help="结束日期，YYYY-MM-DD")
    parser.add_argument("--report-date", default=date.today().isoformat(), help="报告日期，YYYY-MM-DD")
    parser.add_argument("--out-dir", default="reports/structured", help="输出目录")
    args = parser.parse_args()

    report_path = generate_report(
        symbol=args.symbol,
        start_date=args.start_date,
        end_date=args.end_date,
        report_date=args.report_date,
        out_root=Path(args.out_dir),
    )
    print(report_path)


if __name__ == "__main__":
    main()
