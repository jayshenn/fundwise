from __future__ import annotations

import argparse
import csv
import inspect
import json
import re
import signal
import time
from collections import Counter
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import akshare as ak
import pandas as pd

DEFAULT_DOC_PATH = Path("docs/AKShare-东方财富数据接口一览.md")
DEFAULT_OUTPUT_PATH = Path("docs/AKShare-东方财富接口可用性测试结果.csv")
DEFAULT_A_STOCK_CODE = "688041"


@dataclass(frozen=True)
class InterfaceSpec:
    name: str
    doc_line: int
    occurrences: int


@dataclass(frozen=True)
class ProbeRecord:
    checked_at: str
    interface_name: str
    doc_line: int
    occurrences: int
    exists_in_akshare: bool
    call_status: str
    duration_ms: int
    row_count: int | None
    column_count: int | None
    result_type: str | None
    used_kwargs: str
    error_type: str | None
    error_message: str | None


class InterfaceCallTimeoutError(TimeoutError):
    """Raised when a single interface check exceeds the timeout."""


@contextmanager
def alarm_timeout(seconds: int):
    if seconds <= 0 or not hasattr(signal, "SIGALRM"):
        yield
        return

    def handler(signum: int, frame: Any) -> None:
        raise InterfaceCallTimeoutError(f"interface call exceeded {seconds}s")

    old_handler = signal.getsignal(signal.SIGALRM)
    signal.signal(signal.SIGALRM, handler)
    signal.setitimer(signal.ITIMER_REAL, float(seconds))
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0.0)
        signal.signal(signal.SIGALRM, old_handler)


def parse_documented_interfaces(doc_path: Path) -> list[InterfaceSpec]:
    text = doc_path.read_text(encoding="utf-8")
    pattern = re.compile(r'"([a-zA-Z0-9_]+)"')

    all_names: list[str] = []
    first_line: dict[str, int] = {}

    for line_no, line in enumerate(text.splitlines(), start=1):
        for match in pattern.finditer(line):
            name = match.group(1)
            all_names.append(name)
            first_line.setdefault(name, line_no)

    counts = Counter(all_names)
    ordered_unique = list(dict.fromkeys(all_names))
    return [
        InterfaceSpec(name=name, doc_line=first_line[name], occurrences=counts[name])
        for name in ordered_unique
    ]


def _default_for_required_param(param_name: str, stock_symbol: str) -> Any:
    today = date.today()
    if param_name in {"symbol", "code", "stock", "stock_code", "stock_symbol"}:
        return stock_symbol
    if param_name in {"date", "trade_date"}:
        return today.strftime("%Y%m%d")
    if param_name == "start_date":
        return (today - timedelta(days=30)).strftime("%Y%m%d")
    if param_name == "end_date":
        return today.strftime("%Y%m%d")
    if param_name == "period":
        return "daily"
    if param_name == "adjust":
        return ""
    if param_name == "indicator":
        return "全部股票"
    if param_name == "market":
        return "沪股通"
    if param_name == "year":
        return str(today.year)
    if param_name in {"from_page", "to_page", "page"}:
        return 1
    return ""


def build_call_kwargs(
    func_name: str,
    signature: inspect.Signature,
    stock_symbol: str = DEFAULT_A_STOCK_CODE,
) -> dict[str, Any]:
    kwargs: dict[str, Any] = {}

    for param in signature.parameters.values():
        if param.kind not in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            inspect.Parameter.KEYWORD_ONLY,
        ):
            continue
        if param.default is inspect._empty:
            kwargs[param.name] = _default_for_required_param(param.name, stock_symbol=stock_symbol)

    symbol_param = signature.parameters.get("symbol")
    if (
        func_name.startswith("stock_")
        and symbol_param is not None
        and symbol_param.default in {"", None}
        and "symbol" not in kwargs
    ):
        kwargs["symbol"] = stock_symbol

    timeout_param = signature.parameters.get("timeout")
    if timeout_param is not None and "timeout" not in kwargs:
        kwargs["timeout"] = 10

    return kwargs


def _shape_of(result: Any) -> tuple[int | None, int | None, str]:
    if isinstance(result, pd.DataFrame):
        return int(result.shape[0]), int(result.shape[1]), "DataFrame"
    if isinstance(result, pd.Series):
        return int(result.shape[0]), 1, "Series"
    if isinstance(result, dict):
        return len(result), None, "dict"
    if isinstance(result, (list, tuple, set)):
        return len(result), None, type(result).__name__
    return None, None, type(result).__name__


def probe_interface(
    spec: InterfaceSpec,
    timeout_seconds: int,
    stock_symbol: str = DEFAULT_A_STOCK_CODE,
) -> ProbeRecord:
    checked_at = time.strftime("%Y-%m-%dT%H:%M:%S")
    func = getattr(ak, spec.name, None)
    if func is None:
        return ProbeRecord(
            checked_at=checked_at,
            interface_name=spec.name,
            doc_line=spec.doc_line,
            occurrences=spec.occurrences,
            exists_in_akshare=False,
            call_status="missing",
            duration_ms=0,
            row_count=None,
            column_count=None,
            result_type=None,
            used_kwargs="{}",
            error_type="AttributeError",
            error_message="interface not found in akshare",
        )

    try:
        signature = inspect.signature(func)
    except Exception as exc:  # pragma: no cover
        return ProbeRecord(
            checked_at=checked_at,
            interface_name=spec.name,
            doc_line=spec.doc_line,
            occurrences=spec.occurrences,
            exists_in_akshare=True,
            call_status="error",
            duration_ms=0,
            row_count=None,
            column_count=None,
            result_type=None,
            used_kwargs="{}",
            error_type=type(exc).__name__,
            error_message=str(exc)[:300],
        )

    kwargs = build_call_kwargs(spec.name, signature, stock_symbol=stock_symbol)
    kwargs_json = json.dumps(kwargs, ensure_ascii=False, sort_keys=True)

    start = time.perf_counter()
    try:
        with alarm_timeout(timeout_seconds):
            result = func(**kwargs)
        duration_ms = int((time.perf_counter() - start) * 1000)
        rows, cols, result_type = _shape_of(result)
        return ProbeRecord(
            checked_at=checked_at,
            interface_name=spec.name,
            doc_line=spec.doc_line,
            occurrences=spec.occurrences,
            exists_in_akshare=True,
            call_status="ok",
            duration_ms=duration_ms,
            row_count=rows,
            column_count=cols,
            result_type=result_type,
            used_kwargs=kwargs_json,
            error_type=None,
            error_message=None,
        )
    except InterfaceCallTimeoutError as exc:
        duration_ms = int((time.perf_counter() - start) * 1000)
        return ProbeRecord(
            checked_at=checked_at,
            interface_name=spec.name,
            doc_line=spec.doc_line,
            occurrences=spec.occurrences,
            exists_in_akshare=True,
            call_status="timeout",
            duration_ms=duration_ms,
            row_count=None,
            column_count=None,
            result_type=None,
            used_kwargs=kwargs_json,
            error_type=type(exc).__name__,
            error_message=str(exc)[:300],
        )
    except Exception as exc:
        duration_ms = int((time.perf_counter() - start) * 1000)
        return ProbeRecord(
            checked_at=checked_at,
            interface_name=spec.name,
            doc_line=spec.doc_line,
            occurrences=spec.occurrences,
            exists_in_akshare=True,
            call_status="error",
            duration_ms=duration_ms,
            row_count=None,
            column_count=None,
            result_type=None,
            used_kwargs=kwargs_json,
            error_type=type(exc).__name__,
            error_message=str(exc).replace("\n", " ")[:300],
        )


def write_report_csv(records: list[ProbeRecord], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if not records:
        return

    fieldnames = list(asdict(records[0]).keys())
    with output_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for record in records:
            writer.writerow(asdict(record))


def run_probe(
    doc_path: Path,
    output_path: Path,
    timeout_seconds: int,
    limit: int | None = None,
    stock_symbol: str = DEFAULT_A_STOCK_CODE,
) -> list[ProbeRecord]:
    specs = parse_documented_interfaces(doc_path)
    if limit is not None:
        specs = specs[:limit]

    records: list[ProbeRecord] = []
    for spec in specs:
        records.append(
            probe_interface(
                spec,
                timeout_seconds=timeout_seconds,
                stock_symbol=stock_symbol,
            )
        )

    write_report_csv(records, output_path)
    return records


def _print_summary(records: list[ProbeRecord], output_path: Path) -> None:
    total = len(records)
    status_counter = Counter(r.call_status for r in records)
    ok = status_counter.get("ok", 0)
    missing = status_counter.get("missing", 0)
    timeout = status_counter.get("timeout", 0)
    error = status_counter.get("error", 0)

    print(f"checked interfaces: {total}")
    print(f"ok={ok}, missing={missing}, timeout={timeout}, error={error}")
    print(f"report: {output_path}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Probe AKShare 东财接口可用性并输出 CSV 报告")
    parser.add_argument("--doc-path", type=Path, default=DEFAULT_DOC_PATH)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH)
    parser.add_argument("--timeout", type=int, default=10, help="timeout in seconds per interface call")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument(
        "--stock-symbol",
        type=str,
        default=DEFAULT_A_STOCK_CODE,
        help="A-share stock symbol used when an interface needs a stock code",
    )
    parser.add_argument("--strict", action="store_true", help="exit with code 1 when any non-ok status exists")
    args = parser.parse_args()

    records = run_probe(
        doc_path=args.doc_path,
        output_path=args.output,
        timeout_seconds=args.timeout,
        limit=args.limit,
        stock_symbol=args.stock_symbol,
    )
    _print_summary(records, output_path=args.output)

    if args.strict and any(r.call_status != "ok" for r in records):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
