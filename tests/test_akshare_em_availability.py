import inspect
import os
from pathlib import Path

import pytest

from scripts.akshare_em_availability import (
    DEFAULT_A_STOCK_CODE,
    DEFAULT_DOC_PATH,
    build_call_kwargs,
    parse_documented_interfaces,
    run_probe,
)


def test_parse_documented_interfaces_count_and_duplicates():
    specs = parse_documented_interfaces(DEFAULT_DOC_PATH)
    names = [s.name for s in specs]

    assert len(specs) >= 200
    assert len(specs) == len(set(names))

    duplicate_map = {s.name: s.occurrences for s in specs if s.occurrences > 1}
    assert duplicate_map["stock_us_famous_spot_em"] == 2
    assert duplicate_map["index_global_hist_em"] == 2


def test_build_call_kwargs_uses_default_stock_symbol_for_required_symbol():
    sig = inspect.Signature(
        parameters=[
            inspect.Parameter("symbol", inspect.Parameter.POSITIONAL_OR_KEYWORD),
            inspect.Parameter("period", inspect.Parameter.POSITIONAL_OR_KEYWORD),
        ]
    )

    kwargs = build_call_kwargs("stock_dummy_em", sig)

    assert kwargs["symbol"] == DEFAULT_A_STOCK_CODE
    assert kwargs["period"] == "daily"


def test_build_call_kwargs_fills_optional_empty_stock_symbol_with_default_value():
    sig = inspect.Signature(
        parameters=[
            inspect.Parameter(
                "symbol",
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                default="",
            )
        ]
    )

    kwargs = build_call_kwargs("stock_profit_forecast_em", sig)

    assert kwargs["symbol"] == DEFAULT_A_STOCK_CODE


@pytest.mark.integration
@pytest.mark.skipif(
    os.getenv("RUN_AKSHARE_LIVE_TESTS") != "1",
    reason="Set RUN_AKSHARE_LIVE_TESTS=1 to run live availability checks.",
)
def test_run_probe_live_generates_csv(tmp_path: Path):
    output_path = tmp_path / "akshare_em_probe.csv"
    records = run_probe(
        doc_path=DEFAULT_DOC_PATH,
        output_path=output_path,
        timeout_seconds=8,
        limit=20,
    )

    assert output_path.exists()
    assert len(records) == 20
    assert all(r.interface_name for r in records)
