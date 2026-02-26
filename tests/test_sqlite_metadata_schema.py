"""本地 SQLite 元数据库表结构测试。"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))


def _list_tables(db_path: Path) -> set[str]:
    """返回数据库中的全部非系统表名。"""
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';"
        ).fetchall()
    return {row[0] for row in rows}


def test_init_sqlite_metadata_db_creates_required_tables(tmp_path: Path) -> None:
    """验证首次初始化会创建预期的元数据表。"""
    from fundwise.storage.sqlite_store import METADATA_TABLES, init_sqlite_metadata_db

    db_path = tmp_path / "data" / "metadata" / "fundwise.db"
    created = init_sqlite_metadata_db(db_path)

    assert created == db_path.resolve()
    assert created.exists()

    created_tables = _list_tables(created)
    for table_name in METADATA_TABLES:
        assert table_name in created_tables


def test_init_sqlite_metadata_db_is_idempotent(tmp_path: Path) -> None:
    """验证重复初始化是幂等且不会破坏表结构。"""
    from fundwise.storage.sqlite_store import METADATA_TABLES, init_sqlite_metadata_db

    db_path = tmp_path / "fundwise.db"
    first = init_sqlite_metadata_db(db_path)
    second = init_sqlite_metadata_db(db_path)

    assert first == second
    created_tables = _list_tables(first)
    assert set(METADATA_TABLES).issubset(created_tables)
