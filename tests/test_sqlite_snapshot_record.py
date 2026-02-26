"""SQLite 快照索引写入测试。"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from fundwise.storage.sqlite_store import (
    init_sqlite_metadata_db,
    record_dataset_snapshot,
    upsert_symbol,
)


def _query_snapshot_row_count(db_path: Path) -> int:
    with sqlite3.connect(db_path) as conn:
        value = conn.execute("SELECT COUNT(*) FROM dataset_snapshots;").fetchone()
    assert value is not None
    return int(value[0])


def _query_snapshot_payload(db_path: Path) -> tuple[int, str]:
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT row_count, checksum
            FROM dataset_snapshots
            WHERE symbol = ?
              AND dataset_type = ?
              AND as_of_date = ?
            LIMIT 1;
            """,
            ("600519.SH", "price_history", "2025-01-01"),
        ).fetchone()
    assert row is not None
    return int(row[0]), str(row[1])


def test_record_dataset_snapshot_insert_and_update(tmp_path: Path) -> None:
    db_path = init_sqlite_metadata_db(tmp_path / "fundwise.db")
    upsert_symbol(
        db_path=db_path,
        symbol="600519.SH",
        market="CN",
        currency="CNY",
        name="贵州茅台",
        is_active=True,
    )
    file_path = tmp_path / "price_history.parquet"
    file_path.write_text("demo", encoding="utf-8")

    record_dataset_snapshot(
        db_path=db_path,
        symbol="600519.SH",
        dataset_type="price_history",
        as_of_date="2025-01-01",
        file_path=file_path,
        row_count=10,
        checksum="abc",
    )
    assert _query_snapshot_row_count(db_path) == 1
    assert _query_snapshot_payload(db_path) == (10, "abc")

    record_dataset_snapshot(
        db_path=db_path,
        symbol="600519.SH",
        dataset_type="price_history",
        as_of_date="2025-01-01",
        file_path=file_path,
        row_count=25,
        checksum="xyz",
    )
    assert _query_snapshot_row_count(db_path) == 1
    assert _query_snapshot_payload(db_path) == (25, "xyz")
