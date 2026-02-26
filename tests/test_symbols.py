"""股票代码解析与格式转换测试。"""

from __future__ import annotations

import pytest

from fundwise.data_adapter.symbols import infer_cn_exchange, parse_symbol


def test_parse_symbol_cn_with_exchange() -> None:
    info = parse_symbol("600519.SH")
    assert info.symbol == "600519.SH"
    assert info.code == "600519"
    assert info.exchange == "SH"
    assert info.market == "CN"
    assert info.currency == "CNY"
    assert info.to_akshare_hist_symbol() == "sh600519"
    assert info.to_akshare_em_symbol() == "SH600519"


def test_parse_symbol_hk_with_zero_padding() -> None:
    info = parse_symbol("700.hk")
    assert info.symbol == "00700.HK"
    assert info.code == "00700"
    assert info.exchange == "HK"
    assert info.market == "HK"
    assert info.currency == "HKD"
    assert info.to_akshare_hist_symbol() == "00700"
    assert info.to_akshare_em_symbol() == "00700"


def test_parse_symbol_prefix_and_plain_numeric() -> None:
    prefixed = parse_symbol("sz000001")
    assert prefixed.symbol == "000001.SZ"

    numeric_cn = parse_symbol("600000")
    assert numeric_cn.symbol == "600000.SH"

    numeric_hk = parse_symbol("700", default_market="HK")
    assert numeric_hk.symbol == "00700.HK"


def test_infer_cn_exchange() -> None:
    assert infer_cn_exchange("600519") == "SH"
    assert infer_cn_exchange("000001") == "SZ"


def test_parse_symbol_invalid() -> None:
    with pytest.raises(ValueError):
        parse_symbol("ABCDEF")
