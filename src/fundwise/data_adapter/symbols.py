"""股票代码标准化与 AkShare 参数转换工具。"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Final, Literal

SymbolMarket = Literal["CN", "HK"]
SymbolExchange = Literal["SH", "SZ", "HK"]

_CN_EXCHANGE_RE: Final[re.Pattern[str]] = re.compile(r"^(?P<code>\d{6})\.(?P<ex>SH|SZ)$")
_HK_EXCHANGE_RE: Final[re.Pattern[str]] = re.compile(r"^(?P<code>\d{1,5})\.HK$")
_CN_PREFIX_RE: Final[re.Pattern[str]] = re.compile(r"^(?P<ex>SH|SZ)(?P<code>\d{6})$")
_HK_PREFIX_RE: Final[re.Pattern[str]] = re.compile(r"^HK(?P<code>\d{1,5})$")


@dataclass(frozen=True, slots=True)
class SymbolInfo:
    """标准化后的股票代码信息。"""

    raw_symbol: str
    symbol: str
    code: str
    exchange: SymbolExchange
    market: SymbolMarket
    currency: str

    def to_akshare_hist_symbol(self) -> str:
        """返回行情接口常用参数格式。"""
        if self.market == "CN":
            return f"{self.exchange.lower()}{self.code}"
        return self.code

    def to_akshare_em_symbol(self) -> str:
        """返回东方财富系列接口常用参数格式。"""
        if self.market == "CN":
            return f"{self.exchange}{self.code}"
        return self.code


def infer_cn_exchange(code: str) -> Literal["SH", "SZ"]:
    """根据 6 位 A 股代码推断交易所。"""
    if not re.fullmatch(r"\d{6}", code):
        raise ValueError(f"A 股代码必须为 6 位数字，当前值: {code}")
    if code.startswith(("5", "6", "9")):
        return "SH"
    return "SZ"


def parse_symbol(symbol: str, default_market: SymbolMarket | None = None) -> SymbolInfo:
    """解析并标准化股票代码。

    支持格式：
    - `600519.SH`
    - `00700.HK` / `700.HK`
    - `SH600519` / `SZ000001` / `HK00700`
    - 纯数字（`600519` 或 `00700`，需依赖规则或 `default_market`）
    """
    normalized = symbol.strip().upper()
    if not normalized:
        raise ValueError("symbol 不能为空")

    cn_match = _CN_EXCHANGE_RE.match(normalized)
    if cn_match:
        code = cn_match.group("code")
        exchange: Literal["SH", "SZ"] = cn_match.group("ex")  # type: ignore[assignment]
        return SymbolInfo(
            raw_symbol=symbol,
            symbol=f"{code}.{exchange}",
            code=code,
            exchange=exchange,
            market="CN",
            currency="CNY",
        )

    hk_match = _HK_EXCHANGE_RE.match(normalized)
    if hk_match:
        code = hk_match.group("code").zfill(5)
        return SymbolInfo(
            raw_symbol=symbol,
            symbol=f"{code}.HK",
            code=code,
            exchange="HK",
            market="HK",
            currency="HKD",
        )

    cn_prefixed_match = _CN_PREFIX_RE.match(normalized)
    if cn_prefixed_match:
        code = cn_prefixed_match.group("code")
        exchange: Literal["SH", "SZ"] = cn_prefixed_match.group("ex")  # type: ignore[assignment]
        return SymbolInfo(
            raw_symbol=symbol,
            symbol=f"{code}.{exchange}",
            code=code,
            exchange=exchange,
            market="CN",
            currency="CNY",
        )

    hk_prefixed_match = _HK_PREFIX_RE.match(normalized)
    if hk_prefixed_match:
        code = hk_prefixed_match.group("code").zfill(5)
        return SymbolInfo(
            raw_symbol=symbol,
            symbol=f"{code}.HK",
            code=code,
            exchange="HK",
            market="HK",
            currency="HKD",
        )

    if normalized.isdigit():
        if len(normalized) == 6 and default_market != "HK":
            exchange = infer_cn_exchange(normalized)
            return SymbolInfo(
                raw_symbol=symbol,
                symbol=f"{normalized}.{exchange}",
                code=normalized,
                exchange=exchange,
                market="CN",
                currency="CNY",
            )
        if len(normalized) <= 5 and default_market != "CN":
            code = normalized.zfill(5)
            return SymbolInfo(
                raw_symbol=symbol,
                symbol=f"{code}.HK",
                code=code,
                exchange="HK",
                market="HK",
                currency="HKD",
            )

    raise ValueError(f"不支持的 symbol 格式: {symbol}")

