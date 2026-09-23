"""Sync dbt's DuckDB marts into the warehouse Postgres serving layer.

Uses DuckDB's `postgres` extension to attach and copy tables directly,
so this module needs no separate Postgres driver dependency.
"""

from __future__ import annotations

from pathlib import Path

# dbt's default schema-generation macro prefixes a model's configured schema
# (`marts` in dbt_project.yml) with DuckDB's target schema (`main`).
MART_SCHEMA = "main_marts"


def sync_marts_to_postgres(*, duckdb_path: Path, warehouse_postgres_dsn: str) -> list[str]:
    """Overwrite Postgres tables with current DuckDB `main_marts` contents."""

    import duckdb

    # A read-only DuckDB connection also makes attached databases read-only.
    # This operation writes replacement mart tables to the attached Postgres DB.
    connection = duckdb.connect(str(duckdb_path))
    try:
        connection.execute("INSTALL postgres")
        connection.execute("LOAD postgres")
        connection.execute(f"ATTACH '{warehouse_postgres_dsn}' AS pg (TYPE postgres)")

        mart_tables = [
            row[0]
            for row in connection.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = ?",
                [MART_SCHEMA],
            ).fetchall()
        ]
        for table_name in mart_tables:
            connection.execute(
                f'CREATE OR REPLACE TABLE pg.public."{table_name}" AS '
                f'SELECT * FROM "{MART_SCHEMA}"."{table_name}"'
            )
        connection.execute("DETACH pg")
        return mart_tables
    finally:
        connection.close()
