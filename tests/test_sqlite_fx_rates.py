"""SQLite 汇率表读写能力测试。"""

from __future__ import annotations

from pathlib import Path

from fundwise.storage import get_fx_rate, init_sqlite_metadata_db, resolve_fx_to_cny, upsert_fx_rate


def test_upsert_and_get_fx_rate_exact_date(tmp_path: Path) -> None:
    """同日同方向查询应返回写入值。"""
    db_path = init_sqlite_metadata_db(tmp_path / "fx.db")
    upsert_fx_rate(
        db_path=db_path,
        date="2026-02-20",
        base_currency="HKD",
        quote_currency="CNY",
        rate=0.92,
        source="manual",
    )

    rate = get_fx_rate(
        db_path=db_path,
        date="2026-02-20",
        base_currency="HKD",
        quote_currency="CNY",
        allow_nearest=False,
    )
    assert rate == 0.92


def test_get_fx_rate_supports_reverse_lookup(tmp_path: Path) -> None:
    """反向汇率未直接存储时应自动倒数。"""
    db_path = init_sqlite_metadata_db(tmp_path / "fx.db")
    upsert_fx_rate(
        db_path=db_path,
        date="2026-02-20",
        base_currency="CNY",
        quote_currency="HKD",
        rate=1.08,
        source="manual",
    )

    rate = get_fx_rate(
        db_path=db_path,
        date="2026-02-20",
        base_currency="HKD",
        quote_currency="CNY",
        allow_nearest=False,
    )
    assert rate is not None
    assert round(rate, 8) == round(1.0 / 1.08, 8)


def test_get_fx_rate_supports_nearest_fallback(tmp_path: Path) -> None:
    """指定日期缺失时可回退到最近历史汇率。"""
    db_path = init_sqlite_metadata_db(tmp_path / "fx.db")
    upsert_fx_rate(
        db_path=db_path,
        date="2026-02-18",
        base_currency="HKD",
        quote_currency="CNY",
        rate=0.91,
        source="manual",
    )

    nearest_rate = get_fx_rate(
        db_path=db_path,
        date="2026-02-20",
        base_currency="HKD",
        quote_currency="CNY",
        allow_nearest=True,
    )
    exact_rate = get_fx_rate(
        db_path=db_path,
        date="2026-02-20",
        base_currency="HKD",
        quote_currency="CNY",
        allow_nearest=False,
    )
    assert nearest_rate == 0.91
    assert exact_rate is None


def test_resolve_fx_to_cny_returns_one_for_cny(tmp_path: Path) -> None:
    """CNY 对 CNY 固定返回 1。"""
    db_path = init_sqlite_metadata_db(tmp_path / "fx.db")
    rate = resolve_fx_to_cny(
        db_path=db_path,
        currency="CNY",
        date="2026-02-20",
    )
    assert rate == 1.0


def test_upsert_fx_rate_rejects_non_positive_rate(tmp_path: Path) -> None:
    """非正汇率应抛出异常。"""
    db_path = init_sqlite_metadata_db(tmp_path / "fx.db")
    try:
        upsert_fx_rate(
            db_path=db_path,
            date="2026-02-20",
            base_currency="HKD",
            quote_currency="CNY",
            rate=0,
            source="manual",
        )
    except ValueError as exc:
        assert "必须大于 0" in str(exc)
    else:
        raise AssertionError("预期抛出 ValueError")
