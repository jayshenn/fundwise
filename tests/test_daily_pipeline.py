"""日常流水线测试。"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd

from fundwise.data_adapter.symbols import SymbolInfo, parse_symbol
from fundwise.pipeline import PipelineConfig, run_daily_pipeline
from fundwise.storage import init_sqlite_metadata_db, upsert_fx_rate


class _FakeAdapter:
    """用于流水线测试的假适配器。"""

    def __init__(self, *, fail_dataset_symbols: set[str] | None = None) -> None:
        self.fail_dataset_symbols = fail_dataset_symbols or set()

    def get_symbol_info(self, symbol: str) -> SymbolInfo:
        """返回标准化 symbol。"""
        return parse_symbol(symbol)

    def build_company_dataset(
        self,
        symbol: str,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> dict[str, pd.DataFrame]:
        """返回固定结构的测试数据。"""
        del start_date, end_date
        if symbol in self.fail_dataset_symbols:
            raise RuntimeError(f"模拟拉数失败: {symbol}")

        info = parse_symbol(symbol)
        price = pd.DataFrame(
            {
                "symbol": [info.symbol, info.symbol],
                "date": ["2026-02-19", "2026-02-20"],
                "close": [10.0, 11.0],
                "market": [info.market, info.market],
                "currency": [info.currency, info.currency],
            }
        )
        cap = pd.DataFrame(
            {
                "symbol": [info.symbol],
                "date": ["2026-02-20"],
                "market_cap": [1_000.0],
                "market": [info.market],
                "currency": [info.currency],
            }
        )
        financial = pd.DataFrame(
            {
                "symbol": [info.symbol, info.symbol],
                "date": ["2025-12-31", "2026-12-31"],
                "revenue": [200.0, 260.0],
                "net_profit": [50.0, 65.0],
                "ocf": [48.0, 70.0],
                "roe": [0.12, 0.15],
                "debt_to_asset": [0.40, 0.35],
                "total_assets": [300.0, 380.0],
                "total_liabilities": [120.0, 133.0],
                "market": [info.market, info.market],
                "currency": [info.currency, info.currency],
            }
        )
        return {
            "price_history": price,
            "market_cap_history": cap,
            "financial_indicators": financial,
        }


def _count_rows(db_path: Path, query: str, params: tuple[object, ...] = ()) -> int:
    """执行计数查询。"""
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(query, params).fetchone()
    if row is None:
        return 0
    return int(row[0])


def test_run_daily_pipeline_all_success(tmp_path: Path) -> None:
    """全成功场景应生成完整产物并写入成功状态。"""
    db_path = init_sqlite_metadata_db(tmp_path / "metadata.db")
    upsert_fx_rate(
        db_path=db_path,
        date="2026-02-20",
        base_currency="HKD",
        quote_currency="CNY",
        rate=0.92,
        source="manual",
    )

    config = PipelineConfig(
        symbols=["600519.SH", "00700.HK"],
        start_date="2026-01-01",
        end_date="2026-02-20",
        report_root=tmp_path / "reports" / "daily",
        normalized_root=tmp_path / "data" / "normalized",
        db_path=db_path,
        run_date="2026-02-26",
    )
    result = run_daily_pipeline(config, adapter=_FakeAdapter())

    assert result.run_date == "2026-02-26"
    assert result.success_symbols == ["600519.SH", "00700.HK"]
    assert result.failed_symbols == {}
    assert Path(result.summary_markdown_path).exists()
    assert Path(result.summary_json_path).exists()
    assert any("watchlist-" in path for path in result.generated_files)
    assert any("market-timing-" in path for path in result.generated_files)

    reports_count = _count_rows(db_path, "SELECT COUNT(*) FROM reports;")
    assert reports_count >= 4

    pipeline_success = _count_rows(
        db_path,
        "SELECT COUNT(*) FROM data_jobs WHERE job_type = ? AND status = ?;",
        ("run_daily_pipeline", "success"),
    )
    symbol_success = _count_rows(
        db_path,
        "SELECT COUNT(*) FROM data_jobs WHERE job_type = ? AND status = ?;",
        ("daily_symbol_refresh", "success"),
    )
    assert pipeline_success == 1
    assert symbol_success == 2


def test_run_daily_pipeline_partial_failure(tmp_path: Path) -> None:
    """部分失败时应保留成功产物并记录失败状态。"""
    db_path = init_sqlite_metadata_db(tmp_path / "metadata.db")
    config = PipelineConfig(
        symbols=["600519.SH", "000333.SZ"],
        start_date="2026-01-01",
        end_date="2026-02-20",
        report_root=tmp_path / "reports" / "daily",
        normalized_root=tmp_path / "data" / "normalized",
        db_path=db_path,
        run_date="2026-02-26",
    )
    result = run_daily_pipeline(
        config,
        adapter=_FakeAdapter(fail_dataset_symbols={"000333.SZ"}),
    )

    assert result.success_symbols == ["600519.SH"]
    assert "000333.SZ" in result.failed_symbols
    assert "模拟拉数失败" in result.failed_symbols["000333.SZ"]
    assert any("watchlist-" in path for path in result.generated_files)
    assert any("market-timing-" in path for path in result.generated_files)

    pipeline_failed = _count_rows(
        db_path,
        "SELECT COUNT(*) FROM data_jobs WHERE job_type = ? AND status = ?;",
        ("run_daily_pipeline", "failed"),
    )
    symbol_failed = _count_rows(
        db_path,
        "SELECT COUNT(*) FROM data_jobs WHERE job_type = ? AND status = ?;",
        ("daily_symbol_refresh", "failed"),
    )
    assert pipeline_failed == 1
    assert symbol_failed == 1
