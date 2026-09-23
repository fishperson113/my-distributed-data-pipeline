"""Environment settings for the local ELT warehouse (Mongo/DuckDB/Postgres)."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


@dataclass(frozen=True)
class WarehouseSettings:
    """Connection settings for the ELT warehouse, distinct from Dagster's own Postgres."""

    mongo_uri: str
    mongo_database: str
    duckdb_path: Path
    warehouse_postgres_dsn: str

    @classmethod
    def from_env(cls) -> "WarehouseSettings":
        """Load warehouse settings from process env with a `.env` fallback."""

        load_dotenv(override=False)

        mongo_uri = os.getenv("MONGO_URI", "").strip()
        if not mongo_uri:
            raise ValueError("MONGO_URI cannot be blank.")

        mongo_database = os.getenv("MONGO_DATABASE", "").strip()
        if not mongo_database:
            raise ValueError("MONGO_DATABASE cannot be blank.")

        warehouse_postgres_dsn = os.getenv("WAREHOUSE_POSTGRES_DSN", "").strip()
        if not warehouse_postgres_dsn:
            raise ValueError("WAREHOUSE_POSTGRES_DSN cannot be blank.")

        return cls(
            mongo_uri=mongo_uri,
            mongo_database=mongo_database,
            duckdb_path=Path(os.getenv("DUCKDB_PATH", "storage/warehouse/warehouse.duckdb")),
            warehouse_postgres_dsn=warehouse_postgres_dsn,
        )
