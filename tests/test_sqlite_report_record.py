"""SQLite 报告索引写入测试。"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from fundwise.storage import init_sqlite_metadata_db, record_report, upsert_symbol


def _query_report_count(db_path: Path) -> int:
    with sqlite3.connect(db_path) as conn:
        value = conn.execute("SELECT COUNT(*) FROM reports;").fetchone()
    assert value is not None
    return int(value[0])


def test_record_report_upsert(tmp_path: Path) -> None:
    db_path = init_sqlite_metadata_db(tmp_path / "fundwise.db")
    upsert_symbol(
        db_path=db_path,
        symbol="600519.SH",
        market="CN",
        currency="CNY",
        name="贵州茅台",
        is_active=True,
    )
    report_path = tmp_path / "company-dossier-2025-01-01.md"
    report_path.write_text("demo", encoding="utf-8")

    record_report(
        db_path=db_path,
        symbol="600519.SH",
        report_type="company_dossier",
        report_date="2025-01-01",
        file_path=report_path,
    )
    assert _query_report_count(db_path) == 1

    record_report(
        db_path=db_path,
        symbol="600519.SH",
        report_type="company_dossier",
        report_date="2025-01-01",
        file_path=report_path,
    )
    assert _query_report_count(db_path) == 1
