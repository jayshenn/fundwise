"""SQLite 股票池元数据测试。"""

from __future__ import annotations

from pathlib import Path

from fundwise.storage import init_sqlite_metadata_db, list_symbols, upsert_symbol


def test_upsert_and_list_symbols(tmp_path: Path) -> None:
    """写入后应能按活跃状态与市场过滤查询。"""
    db_path = init_sqlite_metadata_db(tmp_path / "fundwise.db")

    upsert_symbol(
        db_path=db_path,
        symbol="600519.SH",
        market="CN",
        currency="CNY",
        name="贵州茅台",
        is_active=True,
    )
    upsert_symbol(
        db_path=db_path,
        symbol="00700.HK",
        market="HK",
        currency="HKD",
        name="腾讯控股",
        is_active=False,
    )

    active_symbols = list_symbols(db_path=db_path)
    assert len(active_symbols) == 1
    assert active_symbols[0]["symbol"] == "600519.SH"

    all_symbols = list_symbols(db_path=db_path, only_active=False)
    assert [item["symbol"] for item in all_symbols] == ["00700.HK", "600519.SH"]

    hk_symbols = list_symbols(db_path=db_path, only_active=False, market="HK")
    assert len(hk_symbols) == 1
    assert hk_symbols[0]["symbol"] == "00700.HK"


def test_upsert_symbol_can_update_status(tmp_path: Path) -> None:
    """重复写入应更新名称和活跃状态。"""
    db_path = init_sqlite_metadata_db(tmp_path / "fundwise.db")

    upsert_symbol(
        db_path=db_path,
        symbol="600519.SH",
        market="CN",
        currency="CNY",
        name="贵州茅台",
        is_active=True,
    )
    upsert_symbol(
        db_path=db_path,
        symbol="600519.SH",
        market="CN",
        currency="CNY",
        name="茅台股份",
        is_active=False,
    )

    active_symbols = list_symbols(db_path=db_path)
    assert active_symbols == []

    all_symbols = list_symbols(db_path=db_path, only_active=False)
    assert len(all_symbols) == 1
    assert all_symbols[0]["name"] == "茅台股份"
    assert all_symbols[0]["is_active"] == 0
