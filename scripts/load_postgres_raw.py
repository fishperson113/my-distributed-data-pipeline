"""Load raw extraction JSON envelopes into Postgres Bronze tables."""

from __future__ import annotations

import argparse
from collections.abc import Iterable
from datetime import date
from pathlib import Path
from typing import Any

from data_pipeline.config import InfrastructureSettings
from data_pipeline.ingestion.common.models import RawExtraction
from data_pipeline.warehouse.mongo_sink import iter_raw_envelopes
from data_pipeline.warehouse.postgres_bronze import load_extraction_to_postgres
from data_pipeline.warehouse.settings import PostgresBronzeSettings


def _partition_date(raw_path: Path, envelope: dict[str, Any]) -> str:
    """Return the partition from a date directory or the envelope request end date."""

    for parent in raw_path.parents:
        if parent.name.startswith("date="):
            partition_date = parent.name.removeprefix("date=")
            date.fromisoformat(partition_date)
            return partition_date

    request = envelope.get("request")
    if isinstance(request, dict) and isinstance(request.get("end"), str):
        partition_date = request["end"]
        date.fromisoformat(partition_date)
        return partition_date

    raise ValueError(
        f"Cannot determine the partition date for {raw_path}. "
        "Use a date=YYYY-MM-DD directory or provide request.end in the envelope."
    )


def _raw_extraction(envelope: dict[str, Any]) -> RawExtraction:
    """Build the extraction contract expected by the Bronze loader."""

    payload = dict(envelope)
    payload.pop("record_count", None)
    return RawExtraction(**payload)


def load_raw_files_to_postgres(
    *,
    raw_storage_path: Path,
    warehouse_postgres_dsn: str,
    json_paths: Iterable[Path] | None = None,
) -> int:
    """Load selected raw envelopes into their dataset-aligned Bronze tables."""

    loaded = 0
    for raw_path, envelope in iter_raw_envelopes(raw_storage_path, json_paths=json_paths):
        load_extraction_to_postgres(
            extraction=_raw_extraction(envelope),
            partition_date=_partition_date(raw_path, envelope),
            warehouse_postgres_dsn=warehouse_postgres_dsn,
            raw_path=raw_path,
        )
        loaded += 1
    return loaded


def main() -> int:
    """Load selected raw extraction JSON files into Postgres Bronze."""

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--path",
        action="append",
        type=Path,
        help="JSON file or directory to scan; repeat for multiple paths.",
    )
    args = parser.parse_args()

    infrastructure = InfrastructureSettings.from_env()
    postgres = PostgresBronzeSettings.from_env()
    loaded = load_raw_files_to_postgres(
        raw_storage_path=infrastructure.raw_storage_path,
        warehouse_postgres_dsn=postgres.warehouse_postgres_dsn,
        json_paths=args.path,
    )
    print(f"postgres load succeeded: files={loaded}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
