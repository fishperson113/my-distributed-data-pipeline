"""Load Bronze records from Mongo into DuckDB raw tables for dbt to transform."""

from __future__ import annotations

from data_pipeline.warehouse.duckdb_loader import load_mongo_to_duckdb
from data_pipeline.warehouse.settings import WarehouseSettings


def main() -> int:
    """Replace each DuckDB raw table with the current Mongo collection contents."""

    warehouse = WarehouseSettings.from_env()
    loaded_counts = load_mongo_to_duckdb(
        mongo_uri=warehouse.mongo_uri,
        mongo_database=warehouse.mongo_database,
        duckdb_path=warehouse.duckdb_path,
    )
    print(f"duckdb load succeeded: {loaded_counts}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
