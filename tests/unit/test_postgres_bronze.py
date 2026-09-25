from __future__ import annotations

from pathlib import Path

import pytest

from data_pipeline.ingestion.common.models import RawExtraction
from data_pipeline.warehouse.postgres_bronze import (
    bronze_records,
    migration_files,
    payload_checksum,
)


def test_stock_records_have_stable_daily_natural_keys() -> None:
    extraction = RawExtraction(
        source="vnstock",
        dataset="stock_daily",
        provider="kbs",
        request={"symbols": ["FPT"]},
        records=[{"symbol": "fpt", "time": "2026-09-18T00:00:00.000Z", "close": 103.0}],
    )

    records = bronze_records(extraction)

    assert records[0].source_record_key == records[0].payload_checksum
    assert len(records[0].source_record_key) == 64


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
