"""Postgres Bronze raw landing and migration helpers."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Protocol
from uuid import UUID, uuid4

from data_pipeline.ingestion.common.models import RawExtraction

SUPPORTED_DATASETS = frozenset({"stock_daily", "fund_daily"})
DATASET_TABLES = {"stock_daily": "stock", "fund_daily": "fund"}
SOURCE_CONTRACT_VERSION = "1.0"


class Cursor(Protocol):
    def execute(self, query: str, params: Iterable[Any] | None = None) -> Any: ...

    def fetchone(self) -> tuple[UUID] | None: ...

    def __enter__(self) -> "Cursor": ...

    def __exit__(self, *args: object) -> None: ...


class Connection(Protocol):
    def cursor(self) -> Cursor: ...

    def __enter__(self) -> "Connection": ...

    def __exit__(self, *args: object) -> None: ...


ConnectionFactory = Callable[[str], Connection]


@dataclass(frozen=True)
class BronzeRecord:
    """A typed reference to one raw payload retained as JSONB."""

    source_record_key: str
    payload: dict[str, Any]
    payload_checksum: str


@dataclass(frozen=True)
class BronzeLoadResult:
    """Details of one idempotent Bronze batch load."""

    batch_id: UUID
    dataset: str
    record_count: int


def _default_connection_factory(dsn: str) -> Connection:
    import psycopg2

    return psycopg2.connect(dsn)


def _record_checksum(payload: dict[str, Any]) -> str:
    serialized = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def bronze_records(extraction: RawExtraction) -> list[BronzeRecord]:
    """Validate an extraction and map each source record to a Bronze natural key."""

    if extraction.dataset not in SUPPORTED_DATASETS:
        raise ValueError(f"Unsupported Bronze dataset: {extraction.dataset!r}.")

    records: list[BronzeRecord] = []
    for payload in extraction.records:
        if not isinstance(payload, dict):
            raise ValueError(f"{extraction.dataset} records must be JSON objects.")
        checksum = _record_checksum(payload)
        records.append(
            BronzeRecord(
                source_record_key=checksum,
                payload=payload,
                payload_checksum=checksum,
            )
        )
    return records


def payload_checksum(extraction: RawExtraction) -> str:
    """Hash the complete raw extraction envelope when no raw file is available."""

    serialized = json.dumps(
        extraction.to_dict(), ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _raw_object_checksum(extraction: RawExtraction, raw_path: Path | None) -> str:
    if raw_path is not None:
        return hashlib.sha256(raw_path.read_bytes()).hexdigest()
    return payload_checksum(extraction)


def _requested_date(request: dict[str, Any], field: str) -> date | None:
    value = request.get(field)
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"Request {field!r} must be an ISO date.")
    return date.fromisoformat(value)


def load_extraction_to_postgres(
    *,
    extraction: RawExtraction,
    partition_date: str,
    warehouse_postgres_dsn: str,
    dagster_run_id: str | None = None,
    raw_path: Path | None = None,
    connection_factory: ConnectionFactory | None = None,
) -> BronzeLoadResult:
    """Persist an extraction as an append-only, idempotent Postgres Bronze batch."""

    partition = date.fromisoformat(partition_date)
    records = bronze_records(extraction)
    checksum = _raw_object_checksum(extraction, raw_path)
    batch_id = uuid4()
    connect = connection_factory or _default_connection_factory

    with connect(warehouse_postgres_dsn) as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO bronze.ingestion_batch (
                batch_id, source_name, dataset_name, provider_name, partition_key,
                requested_from, requested_to, request_metadata, dagster_run_id,
                raw_object_path, payload_checksum, schema_version, started_at,
                finished_at, status, record_count
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s,
                %s, %s, %s
            )
            ON CONFLICT (source_name, dataset_name, partition_key, payload_checksum)
            DO UPDATE SET dagster_run_id = EXCLUDED.dagster_run_id
            RETURNING batch_id
            """,
            (
                batch_id,
                extraction.source,
                extraction.dataset,
                extraction.provider,
                partition.isoformat(),
                _requested_date(extraction.request, "start"),
                _requested_date(extraction.request, "end"),
                json.dumps(extraction.request, ensure_ascii=False),
                dagster_run_id,
                str(raw_path) if raw_path else "in-memory-extraction",
                checksum,
                SOURCE_CONTRACT_VERSION,
                extraction.fetched_at,
                extraction.fetched_at,
                "completed",
                len(records),
            ),
        )
        batch_row = cursor.fetchone()
        if batch_row is None:
            raise RuntimeError("Bronze batch insert did not return a batch id.")
        persisted_batch_id = batch_row[0]

        table_name = DATASET_TABLES[extraction.dataset]
        record_id_column = f"{table_name}_record_id"
        for record in records:
            cursor.execute(
                f"""
                INSERT INTO bronze.{table_name} (
                    {record_id_column}, batch_id, source_record_key, payload,
                    payload_checksum
                ) VALUES (%s, %s, %s, %s::jsonb, %s)
                ON CONFLICT (batch_id, source_record_key) DO NOTHING
                """,
                (
                    uuid4(),
                    persisted_batch_id,
                    record.source_record_key,
                    json.dumps(record.payload, ensure_ascii=False),
                    record.payload_checksum,
                ),
            )

    return BronzeLoadResult(
        batch_id=persisted_batch_id,
        dataset=extraction.dataset,
        record_count=len(records),
    )


def migration_files(migrations_path: Path) -> list[Path]:
    """Return ordered, versioned SQL migrations from the repository migration path."""

    return sorted(path for path in migrations_path.glob("*.sql") if path.is_file())


def apply_migrations(
    *,
    warehouse_postgres_dsn: str,
    migrations_path: Path,
    connection_factory: ConnectionFactory | None = None,
) -> list[str]:
    """Apply each unrecorded Bronze migration in filename order."""

    connect = connection_factory or _default_connection_factory
    applied_versions: list[str] = []
    with connect(warehouse_postgres_dsn) as connection, connection.cursor() as cursor:
        cursor.execute("CREATE SCHEMA IF NOT EXISTS bronze")
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS bronze.schema_migrations (
                version TEXT PRIMARY KEY,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        for migration_path in migration_files(migrations_path):
            version = migration_path.name
            cursor.execute(
                "SELECT 1 FROM bronze.schema_migrations WHERE version = %s", (version,)
            )
            if cursor.fetchone() is not None:
                continue
            cursor.execute(migration_path.read_text(encoding="utf-8"))
            cursor.execute(
                "INSERT INTO bronze.schema_migrations (version) VALUES (%s)", (version,)
            )
            applied_versions.append(version)
    return applied_versions
