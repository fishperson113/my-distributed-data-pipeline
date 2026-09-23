"""Run the full local ELT flow: Mongo load, DuckDB load, dbt, Postgres sync.

Requires `compose.dev.yml` running (Mongo, warehouse Postgres) and the `elt`
dependency group installed (`uv sync --group elt`). Run from the repository
root so dbt resolves `DUCKDB_PATH` and the raw storage path the same way the
individual scripts do.
"""

from __future__ import annotations

import argparse
import subprocess

from data_pipeline.config import InfrastructureSettings
from data_pipeline.warehouse.duckdb_loader import load_mongo_to_duckdb
from data_pipeline.warehouse.mongo_sink import load_raw_files_to_mongo
from data_pipeline.warehouse.postgres_sync import sync_marts_to_postgres
from data_pipeline.warehouse.settings import WarehouseSettings

DBT_PROJECT_DIR = "src/data_pipeline/dbt"


def _run_dbt(*args: str) -> None:
    subprocess.run(
        ["dbt", *args, "--project-dir", DBT_PROJECT_DIR, "--profiles-dir", DBT_PROJECT_DIR],
        check=True,
    )


def main() -> int:
    """Run Mongo load, DuckDB load, dbt run/test, then sync marts to Postgres."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--skip-tests", action="store_true")
    args = parser.parse_args()

    infrastructure = InfrastructureSettings.from_env()
    warehouse = WarehouseSettings.from_env()

    upserted = load_raw_files_to_mongo(
        raw_storage_path=infrastructure.raw_storage_path,
        mongo_uri=warehouse.mongo_uri,
        mongo_database=warehouse.mongo_database,
    )
    print(f"mongo load succeeded: upserted={upserted}")

    loaded_counts = load_mongo_to_duckdb(
        mongo_uri=warehouse.mongo_uri,
        mongo_database=warehouse.mongo_database,
        duckdb_path=warehouse.duckdb_path,
    )
    print(f"duckdb load succeeded: {loaded_counts}")

    _run_dbt("run")
    if not args.skip_tests:
        _run_dbt("test")

    synced_tables = sync_marts_to_postgres(
        duckdb_path=warehouse.duckdb_path,
        warehouse_postgres_dsn=warehouse.warehouse_postgres_dsn,
    )
    print(f"postgres sync succeeded: tables={synced_tables}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
