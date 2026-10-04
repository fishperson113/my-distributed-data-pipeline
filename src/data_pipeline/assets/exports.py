"""Gold-layer CSV export.

Dumps the dbt marts table to a CSV under ``storage/exports/``. The ``exports``
Compose service serves that directory, and the run metadata carries a
``download_url`` link to it built from ``EXPORT_BASE_URL``. The file is overwritten on every
materialization and written via a temp file so a download never sees a
half-written export.
"""

from __future__ import annotations

import os
from pathlib import Path

import dagster as dg

from data_pipeline.warehouse.settings import PostgresBronzeSettings

EXPORT_DIR = Path(__file__).parents[3] / "storage" / "exports"
GOLD_TABLE = "marts.exp_vn30_vs_fund_daily"


@dg.asset(
    key=["exports", "exp_vn30_vs_fund_daily_csv"],
    deps=[dg.AssetKey(["marts", "exp_vn30_vs_fund_daily"])],
    group_name="exports",
    description="CSV dump of the gold mart marts.exp_vn30_vs_fund_daily.",
)
def gold_vn30_vs_fund_csv() -> dg.MaterializeResult:
    import psycopg2

    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    target = EXPORT_DIR / "exp_vn30_vs_fund_daily.csv"
    tmp = target.with_suffix(".csv.tmp")

    dsn = PostgresBronzeSettings.from_env().warehouse_postgres_dsn
    with psycopg2.connect(dsn) as connection, connection.cursor() as cursor:
        with tmp.open("w", encoding="utf-8", newline="") as handle:
            cursor.copy_expert(
                f"COPY (SELECT * FROM {GOLD_TABLE} "
                "ORDER BY trade_date, bucket_ts, stock_symbol) "
                "TO STDOUT WITH CSV HEADER",
                handle,
            )
        row_count = cursor.rowcount
    tmp.replace(target)

    base_url = os.getenv("EXPORT_BASE_URL", "http://localhost:3001").rstrip("/")
    return dg.MaterializeResult(
        metadata={
            "download_url": dg.MetadataValue.url(f"{base_url}/{target.name}"),
            "path": str(target),
            "row_count": row_count,
            "size_bytes": target.stat().st_size,
        }
    )
