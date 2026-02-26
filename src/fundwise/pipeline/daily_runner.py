"""日常投研批处理流水线。"""

from __future__ import annotations

import csv
import hashlib
import json
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Protocol

import pandas as pd

from fundwise.company_dossier import CompanyDossier, build_company_dossier
from fundwise.data_adapter.symbols import SymbolInfo
from fundwise.market_timing_panel import build_market_timing_panel
from fundwise.report_engine import (
    generate_company_dossier_charts,
    generate_market_timing_charts,
    generate_watchlist_charts,
    render_company_dossier_markdown,
    render_market_timing_markdown,
)
from fundwise.storage import (
    JOB_FAILED,
    JOB_SUCCESS,
    finish_data_job,
    init_sqlite_metadata_db,
    record_dataset_snapshot,
    record_report,
    resolve_fx_to_cny,
    start_data_job,
    upsert_symbol,
)
from fundwise.watchlist_screener import (
    WatchlistScore,
    render_watchlist_markdown,
    score_company_dossier,
)


class AdapterProtocol(Protocol):
    """数据适配器协议。"""

    def get_symbol_info(self, symbol: str) -> SymbolInfo:
        """返回标准化 symbol 信息。"""
        ...

    def build_company_dataset(
        self,
        symbol: str,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> dict[str, pd.DataFrame]:
        """拉取单公司标准化数据集。"""
        ...


@dataclass(frozen=True, slots=True)
class PipelineConfig:
    """日常流水线配置。"""

    symbols: list[str]
    start_date: str | None
    end_date: str | None
    report_root: Path
    normalized_root: Path
    db_path: Path
    run_date: str | None = None


@dataclass(slots=True)
class PipelineResult:
    """流水线执行结果。"""

    run_date: str
    success_symbols: list[str]
    failed_symbols: dict[str, str]
    generated_files: list[str]
    summary_markdown_path: str
    summary_json_path: str

    @property
    def has_failures(self) -> bool:
        """是否存在失败标的。"""
        return bool(self.failed_symbols)

    def to_dict(self) -> dict[str, Any]:
        """转换为字典。"""
        return {
            "run_date": self.run_date,
            "success_symbols": self.success_symbols,
            "failed_symbols": self.failed_symbols,
            "generated_files": self.generated_files,
            "summary_markdown_path": self.summary_markdown_path,
            "summary_json_path": self.summary_json_path,
        }


def load_symbols(symbols_text: str, symbols_file: Path | None) -> list[str]:
    """加载并去重 symbol 列表。"""
    values: list[str] = []
    if symbols_text:
        values.extend([item.strip() for item in symbols_text.split(",") if item.strip()])

    if symbols_file is not None and symbols_file.exists():
        lines = symbols_file.read_text(encoding="utf-8").splitlines()
        values.extend(
            [line.strip() for line in lines if line.strip() and not line.strip().startswith("#")]
        )

    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        normalized = value.upper()
        if normalized in seen:
            continue
        seen.add(normalized)
        result.append(normalized)
    return result


def run_daily_pipeline(config: PipelineConfig, *, adapter: AdapterProtocol) -> PipelineResult:
    """执行日常投研流水线。"""
    symbols = _normalize_symbols(config.symbols)
    if not symbols:
        raise ValueError("未提供有效 symbol 列表")

    run_date = config.run_date or date.today().strftime("%Y-%m-%d")
    report_root = config.report_root.expanduser().resolve() / run_date
    normalized_root = config.normalized_root.expanduser().resolve() / run_date
    company_report_root = report_root / "company_dossier"
    watchlist_report_root = report_root / "watchlist_screener"
    timing_report_root = report_root / "market_timing_panel"
    for path in [
        report_root,
        normalized_root,
        company_report_root,
        watchlist_report_root,
        timing_report_root,
    ]:
        path.mkdir(parents=True, exist_ok=True)

    db_path = init_sqlite_metadata_db(config.db_path)
    upsert_symbol(
        db_path=db_path,
        symbol="PIPELINE",
        market="META",
        currency="CNY",
        name="日常流水线",
        is_active=True,
    )

    pipeline_job_id = start_data_job(
        db_path=db_path,
        job_type="run_daily_pipeline",
        symbol="PIPELINE",
    )

    success_symbols: list[str] = []
    failed_symbols: dict[str, str] = {}
    dossiers: list[CompanyDossier] = []
    scores: list[WatchlistScore] = []
    generated_files: list[str] = []

    try:
        for raw_symbol in symbols:
            _process_single_symbol(
                raw_symbol=raw_symbol,
                adapter=adapter,
                db_path=db_path,
                config=config,
                normalized_root=normalized_root,
                company_report_root=company_report_root,
                run_date=run_date,
                success_symbols=success_symbols,
                failed_symbols=failed_symbols,
                dossiers=dossiers,
                scores=scores,
                generated_files=generated_files,
            )

        if scores:
            generated_files.extend(
                _generate_watchlist_report(
                    db_path=db_path,
                    run_date=run_date,
                    report_root=watchlist_report_root,
                    scores=scores,
                )
            )
            generated_files.extend(
                _generate_market_timing_report(
                    db_path=db_path,
                    run_date=run_date,
                    report_root=timing_report_root,
                    dossiers=dossiers,
                )
            )

        summary_markdown_path, summary_json_path = _write_summary_files(
            report_root=report_root,
            run_date=run_date,
            success_symbols=success_symbols,
            failed_symbols=failed_symbols,
            generated_files=generated_files,
        )
        generated_files.extend([str(summary_markdown_path), str(summary_json_path)])

        status = JOB_SUCCESS if success_symbols and not failed_symbols else JOB_FAILED
        error_message: str | None = None
        if status == JOB_FAILED:
            error_message = _build_pipeline_error_message(
                success_count=len(success_symbols),
                failed_symbols=failed_symbols,
            )
        finish_data_job(
            db_path=db_path,
            job_id=pipeline_job_id,
            status=status,
            error_message=error_message,
        )

        return PipelineResult(
            run_date=run_date,
            success_symbols=success_symbols,
            failed_symbols=failed_symbols,
            generated_files=generated_files,
            summary_markdown_path=str(summary_markdown_path),
            summary_json_path=str(summary_json_path),
        )
    except Exception as exc:  # noqa: BLE001
        finish_data_job(
            db_path=db_path,
            job_id=pipeline_job_id,
            status=JOB_FAILED,
            error_message=f"{type(exc).__name__}: {exc}",
        )
        raise


def _process_single_symbol(
    *,
    raw_symbol: str,
    adapter: AdapterProtocol,
    db_path: Path,
    config: PipelineConfig,
    normalized_root: Path,
    company_report_root: Path,
    run_date: str,
    success_symbols: list[str],
    failed_symbols: dict[str, str],
    dossiers: list[CompanyDossier],
    scores: list[WatchlistScore],
    generated_files: list[str],
) -> None:
    """执行单个 symbol 的拉数、入库与报告渲染。"""
    try:
        symbol_info = adapter.get_symbol_info(raw_symbol)
    except Exception as exc:  # noqa: BLE001
        failed_symbols[raw_symbol.upper()] = f"{type(exc).__name__}: {exc}"
        return

    upsert_symbol(
        db_path=db_path,
        symbol=symbol_info.symbol,
        market=symbol_info.market,
        currency=symbol_info.currency,
        name=None,
        is_active=True,
    )

    symbol_job_id = start_data_job(
        db_path=db_path,
        job_type="daily_symbol_refresh",
        symbol=symbol_info.symbol,
    )

    try:
        dataset = adapter.build_company_dataset(
            symbol=symbol_info.symbol,
            start_date=config.start_date,
            end_date=config.end_date,
        )

        symbol_data_dir = normalized_root / symbol_info.symbol.replace(".", "_")
        symbol_data_dir.mkdir(parents=True, exist_ok=True)
        for dataset_type, frame in dataset.items():
            as_of_date = _latest_as_of_date(frame, fallback_date=run_date)
            file_base = symbol_data_dir / f"{dataset_type}-{as_of_date}"
            saved_path = _save_frame(frame, file_base=file_base)
            checksum = _file_checksum(saved_path)
            record_dataset_snapshot(
                db_path=db_path,
                symbol=symbol_info.symbol,
                dataset_type=dataset_type,
                as_of_date=as_of_date,
                file_path=saved_path,
                row_count=len(frame),
                checksum=checksum,
            )
            generated_files.append(str(saved_path))

        fx_to_cny = resolve_fx_to_cny(
            db_path=db_path,
            currency=symbol_info.currency,
            date=config.end_date,
        )
        dossier = build_company_dossier(
            symbol=symbol_info.symbol,
            dataset=dataset,
            fx_to_cny=fx_to_cny,
        )
        score = score_company_dossier(dossier)
        dossiers.append(dossier)
        scores.append(score)
        success_symbols.append(symbol_info.symbol)

        report_date = dossier.as_of_date or run_date
        symbol_report_dir = company_report_root / symbol_info.symbol.replace(".", "_")
        symbol_report_dir.mkdir(parents=True, exist_ok=True)
        charts = generate_company_dossier_charts(
            symbol=symbol_info.symbol,
            dataset=dataset,
            report_dir=symbol_report_dir,
        )
        markdown = render_company_dossier_markdown(
            dossier=dossier,
            dataset=dataset,
            charts=[(item.title, item.relative_path) for item in charts],
        )
        report_path = symbol_report_dir / f"company-dossier-{report_date}.md"
        report_path.write_text(markdown, encoding="utf-8")

        record_report(
            db_path=db_path,
            symbol=symbol_info.symbol,
            report_type="company_dossier",
            report_date=report_date,
            file_path=report_path,
        )
        generated_files.append(str(report_path))
        generated_files.extend([str(item.absolute_path) for item in charts])
        finish_data_job(db_path=db_path, job_id=symbol_job_id, status=JOB_SUCCESS)
    except Exception as exc:  # noqa: BLE001
        failed_symbols[symbol_info.symbol] = f"{type(exc).__name__}: {exc}"
        finish_data_job(
            db_path=db_path,
            job_id=symbol_job_id,
            status=JOB_FAILED,
            error_message=f"{type(exc).__name__}: {exc}",
        )


def _generate_watchlist_report(
    *,
    db_path: Path,
    run_date: str,
    report_root: Path,
    scores: list[WatchlistScore],
) -> list[str]:
    """生成观察池报告与图表。"""
    upsert_symbol(
        db_path=db_path,
        symbol="WATCHLIST",
        market="META",
        currency="CNY",
        name="观察池",
        is_active=True,
    )
    as_of_date = max([item.as_of_date for item in scores if item.as_of_date] or [run_date])
    report_root.mkdir(parents=True, exist_ok=True)

    charts = generate_watchlist_charts(scores=scores, report_dir=report_root)
    markdown = render_watchlist_markdown(
        scores=scores,
        charts=[(item.title, item.relative_path) for item in charts],
    )
    markdown_path = report_root / f"watchlist-{as_of_date}.md"
    markdown_path.write_text(markdown, encoding="utf-8")

    csv_path = report_root / f"watchlist-{as_of_date}.csv"
    _write_score_csv(csv_path, scores)

    record_report(
        db_path=db_path,
        symbol="WATCHLIST",
        report_type="watchlist_screener",
        report_date=as_of_date,
        file_path=markdown_path,
    )
    return [str(markdown_path), str(csv_path), *[str(item.absolute_path) for item in charts]]


def _generate_market_timing_report(
    *,
    db_path: Path,
    run_date: str,
    report_root: Path,
    dossiers: list[CompanyDossier],
) -> list[str]:
    """生成市场择时面板报告与图表。"""
    upsert_symbol(
        db_path=db_path,
        symbol="MARKET",
        market="META",
        currency="CNY",
        name="市场面板",
        is_active=True,
    )
    panel, scores = build_market_timing_panel(dossiers)
    report_date = panel.as_of_date or run_date
    report_root.mkdir(parents=True, exist_ok=True)

    charts = generate_market_timing_charts(
        panel=panel,
        scores=scores,
        report_dir=report_root,
    )
    markdown = render_market_timing_markdown(
        panel=panel,
        scores=scores,
        charts=[(item.title, item.relative_path) for item in charts],
    )
    markdown_path = report_root / f"market-timing-{report_date}.md"
    markdown_path.write_text(markdown, encoding="utf-8")

    csv_path = report_root / f"market-timing-scores-{report_date}.csv"
    _write_score_csv(csv_path, scores)

    record_report(
        db_path=db_path,
        symbol="MARKET",
        report_type="market_timing_panel",
        report_date=report_date,
        file_path=markdown_path,
    )
    return [str(markdown_path), str(csv_path), *[str(item.absolute_path) for item in charts]]


def _write_score_csv(path: Path, scores: list[WatchlistScore]) -> None:
    """将评分明细写入 CSV。"""
    fieldnames = [
        "symbol",
        "total_score",
        "tier",
        "growth",
        "quality",
        "valuation",
        "momentum",
        "as_of_date",
        "notes",
    ]
    rows: list[dict[str, object]] = []
    for item in scores:
        rows.append(
            {
                "symbol": item.symbol,
                "total_score": item.total_score,
                "tier": item.tier,
                "growth": item.factor_scores.growth,
                "quality": item.factor_scores.quality,
                "valuation": item.factor_scores.valuation,
                "momentum": item.factor_scores.momentum,
                "as_of_date": item.as_of_date,
                "notes": "；".join(item.notes),
            }
        )
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _latest_as_of_date(data: pd.DataFrame, *, fallback_date: str) -> str:
    """获取数据快照日期（优先最新 date）。"""
    if "date" in data.columns and not data.empty:
        parsed = pd.to_datetime(data["date"], errors="coerce").dropna()
        if not parsed.empty:
            return parsed.max().strftime("%Y-%m-%d")
    return fallback_date


def _save_frame(data: pd.DataFrame, file_base: Path) -> Path:
    """优先保存为 parquet，不可用时回退 CSV。"""
    parquet_path = file_base.with_suffix(".parquet")
    try:
        data.to_parquet(parquet_path, index=False)
        return parquet_path
    except Exception:  # noqa: BLE001
        csv_path = file_base.with_suffix(".csv")
        data.to_csv(csv_path, index=False)
        return csv_path


def _file_checksum(path: Path) -> str:
    """计算文件 SHA256 校验值。"""
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(8192), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_summary_files(
    *,
    report_root: Path,
    run_date: str,
    success_symbols: list[str],
    failed_symbols: dict[str, str],
    generated_files: list[str],
) -> tuple[Path, Path]:
    """写入流水线摘要 Markdown 与 JSON。"""
    summary_data = {
        "run_date": run_date,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "success_symbols": success_symbols,
        "failed_symbols": failed_symbols,
        "generated_files": generated_files,
    }
    summary_json_path = report_root / f"pipeline-summary-{run_date}.json"
    summary_json_path.write_text(
        json.dumps(summary_data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    lines = [
        "# 日常流水线执行摘要",
        "",
        f"- 运行日期：{run_date}",
        f"- 成功标的：{len(success_symbols)}",
        f"- 失败标的：{len(failed_symbols)}",
        f"- 产物数量：{len(generated_files)}",
        "",
    ]
    if success_symbols:
        lines.extend(["## 成功标的", ""])
        for symbol in success_symbols:
            lines.append(f"- {symbol}")
        lines.append("")

    if failed_symbols:
        lines.extend(["## 失败标的", ""])
        for symbol, reason in failed_symbols.items():
            lines.append(f"- {symbol}: {reason}")
        lines.append("")

    lines.extend(["## 产物索引", ""])
    for file_path in generated_files:
        lines.append(f"- {file_path}")
    lines.append("")
    summary_markdown_path = report_root / f"pipeline-summary-{run_date}.md"
    summary_markdown_path.write_text("\n".join(lines), encoding="utf-8")
    return summary_markdown_path, summary_json_path


def _build_pipeline_error_message(success_count: int, failed_symbols: dict[str, str]) -> str:
    """构造流水线失败摘要。"""
    if not failed_symbols and success_count == 0:
        return "流水线无成功标的。"
    if not failed_symbols:
        return "流水线未通过成功条件。"
    preview = "；".join([f"{symbol}: {reason}" for symbol, reason in failed_symbols.items()])
    return f"成功标的 {success_count} 个；失败明细：{preview}"


def _normalize_symbols(symbols: list[str]) -> list[str]:
    """标准化并去重 symbol 列表。"""
    seen: set[str] = set()
    result: list[str] = []
    for symbol in symbols:
        normalized = symbol.strip().upper()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        result.append(normalized)
    return result
