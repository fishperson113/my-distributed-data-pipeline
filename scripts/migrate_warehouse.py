"""Apply versioned Postgres Bronze migrations."""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

from data_pipeline.warehouse.postgres_bronze import apply_migrations


def main() -> int:
    """Apply all pending repository migrations to the warehouse Postgres database."""

    load_dotenv(override=False)
    dsn = os.getenv("WAREHOUSE_POSTGRES_DSN", "").strip()
    if not dsn:
        raise ValueError("WAREHOUSE_POSTGRES_DSN cannot be blank.")
    applied = apply_migrations(
        warehouse_postgres_dsn=dsn,
        migrations_path=Path("migrations"),
    )
    print(f"warehouse migrations applied: {applied}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
