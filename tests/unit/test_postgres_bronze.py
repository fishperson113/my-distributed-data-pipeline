from __future__ import annotations

from pathlib import Path

import pytest

from data_pipeline.ingestion.common.models import RawExtraction
from data_pipeline.warehouse.postgres_bronze import (
    bronze_records,
    migration_files,
    payload_checksum,
)


def test_checksum_includes_raw_envelope_fetch_time() -> None:
    common = {
        "source": "ssi_iboard",
        "dataset": "fund_daily",
        "provider": "SSI",
        "request": {"symbol": "E1VFVN30"},
        "records": [{"symbol": "E1VFVN30", "trade_date": "2026-09-18"}],
    }
    first = RawExtraction(**common, fetched_at="2026-09-19T00:00:00+00:00")
    second = RawExtraction(**common, fetched_at="2026-09-20T00:00:00+00:00")

    assert payload_checksum(first) != payload_checksum(second)


def test_bronze_records_reject_unknown_dataset() -> None:
    extraction = RawExtraction(
        source="example",
        dataset="holdings",
        provider="example",
        request={},
        records=[],
    )

    with pytest.raises(ValueError, match="Unsupported Bronze dataset"):
        bronze_records(extraction)


def test_migration_files_are_sorted_and_ignore_non_sql(tmp_path: Path) -> None:
    (tmp_path / "002_second.sql").touch()
    (tmp_path / "001_first.sql").touch()
    (tmp_path / "notes.txt").touch()

    assert [path.name for path in migration_files(tmp_path)] == [
        "001_first.sql",
        "002_second.sql",
    ]


def test_records_are_keyed_on_symbol_and_instant() -> None:
    extraction = RawExtraction(
        source="ssi_iboard",
        dataset="fund_daily",
        provider="SSI",
        request={"symbol": "E1VFVN30"},
        records=[
            {"symbol": "e1vfvn30", "ts_epoch": 1790302500, "close": 34.7},
            {"symbol": "E1VFVN30", "ts_epoch": 1790303400, "close": 34.8},
        ],
    )

    records = bronze_records(extraction)

    assert [record.source_record_key for record in records] == [
        "E1VFVN30|1790302500",
        "E1VFVN30|1790303400",
    ]


def test_a_revised_bar_keeps_the_key_of_the_instant_it_revises() -> None:
    """A changed value must collide with the bar it replaces, not land beside it."""

    def extraction_with(close: float) -> RawExtraction:
        return RawExtraction(
            source="vnstock",
            dataset="stock_daily",
            provider="kbs",
            request={},
            records=[{"symbol": "FPT", "ts_epoch": 1790302500, "close": close}],
        )

    original = bronze_records(extraction_with(65.4))[0]
    revised = bronze_records(extraction_with(65.9))[0]

    assert original.source_record_key == revised.source_record_key
    assert original.payload_checksum != revised.payload_checksum


def test_records_reject_a_missing_instant() -> None:
    extraction = RawExtraction(
        source="vnstock",
        dataset="stock_daily",
        provider="kbs",
        request={},
        records=[{"symbol": "FPT", "close": 65.4}],
    )

    with pytest.raises(ValueError, match="integer 'ts_epoch'"):
        bronze_records(extraction)


def test_records_reject_a_blank_symbol() -> None:
    extraction = RawExtraction(
        source="vnstock",
        dataset="stock_daily",
        provider="kbs",
        request={},
        records=[{"symbol": "   ", "ts_epoch": 1790302500}],
    )

    with pytest.raises(ValueError, match="non-empty 'symbol'"):
        bronze_records(extraction)
