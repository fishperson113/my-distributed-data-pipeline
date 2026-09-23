"""Sync dbt's DuckDB marts into the warehouse Postgres serving layer."""

from __future__ import annotations

from data_pipeline.warehouse.postgres_sync import sync_marts_to_postgres
from data_pipeline.warehouse.settings import WarehouseSettings


def main() -> int:
    """Overwrite each warehouse Postgres table with its current DuckDB mart contents."""

    warehouse = WarehouseSettings.from_env()
    synced_tables = sync_marts_to_postgres(
        duckdb_path=warehouse.duckdb_path,
        warehouse_postgres_dsn=warehouse.warehouse_postgres_dsn,
    )
    print(f"postgres sync succeeded: tables={synced_tables}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
