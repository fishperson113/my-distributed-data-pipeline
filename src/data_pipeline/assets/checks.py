"""Idempotency and consistency asset checks on the Postgres Bronze layer.

These run as part of the daily job and appear next to each Bronze asset in the
Dagster UI:

- ``no_duplicate_records`` proves re-runs are idempotent: the
  ``(batch_id, source_record_key)`` uniqueness holds, so loading the same
  extraction again adds no rows.
- ``record_count_matches`` proves each load is atomic and complete: the rows
  actually stored equal the ``record_count`` recorded on the batch.
"""

from __future__ import annotations

import dagster as dg

from data_pipeline.assets.bronze import bronze_fund_daily, bronze_stock_daily
from data_pipeline.warehouse.settings import PostgresBronzeSettings


def _scalar(query: str) -> int:
    """Run a single-value aggregate query against the warehouse and return it."""

    import psycopg2

    dsn = PostgresBronzeSettings.from_env().warehouse_postgres_dsn
    with psycopg2.connect(dsn) as connection, connection.cursor() as cursor:
        cursor.execute(query)
        row = cursor.fetchone()
        return int(row[0]) if row and row[0] is not None else 0


def _no_duplicate_records(table: str) -> dg.AssetCheckResult:
    duplicate_groups = _scalar(
        f"""
        SELECT count(*) FROM (
            SELECT batch_id, source_record_key
            FROM bronze.{table}
            GROUP BY batch_id, source_record_key
            HAVING count(*) > 1
        ) AS duplicates
        """
    )
    return dg.AssetCheckResult(
        passed=duplicate_groups == 0,
        metadata={"duplicate_key_groups": duplicate_groups},
    )


def _record_count_matches(table: str, dataset: str) -> dg.AssetCheckResult:
    mismatched_batches = _scalar(
        f"""
        SELECT count(*) FROM (
            SELECT batch.batch_id
            FROM bronze.ingestion_batch AS batch
            LEFT JOIN bronze.{table} AS records
                ON records.batch_id = batch.batch_id
            WHERE batch.dataset_name = '{dataset}'
            GROUP BY batch.batch_id, batch.record_count
            HAVING count(records.{table}_record_id) <> batch.record_count
        ) AS mismatches
        """
    )
    return dg.AssetCheckResult(
        passed=mismatched_batches == 0,
        metadata={"batches_with_count_mismatch": mismatched_batches},
    )


@dg.asset_check(
    asset=bronze_stock_daily,
    name="stock_no_duplicate_records",
    description="Each (batch_id, source_record_key) is unique: re-runs add no rows.",
)
def bronze_stock_no_duplicate_records() -> dg.AssetCheckResult:
    return _no_duplicate_records("stock")


@dg.asset_check(
    asset=bronze_stock_daily,
    name="stock_record_count_matches",
    description="Stored rows per batch equal the batch's declared record_count.",
)
def bronze_stock_record_count_matches() -> dg.AssetCheckResult:
    return _record_count_matches("stock", "stock_daily")


@dg.asset_check(
    asset=bronze_fund_daily,
    name="fund_no_duplicate_records",
    description="Each (batch_id, source_record_key) is unique: re-runs add no rows.",
)
def bronze_fund_no_duplicate_records() -> dg.AssetCheckResult:
    return _no_duplicate_records("fund")


@dg.asset_check(
    asset=bronze_fund_daily,
    name="fund_record_count_matches",
    description="Stored rows per batch equal the batch's declared record_count.",
)
def bronze_fund_record_count_matches() -> dg.AssetCheckResult:
    return _record_count_matches("fund", "fund_daily")


bronze_asset_checks = [
    bronze_stock_no_duplicate_records,
    bronze_stock_record_count_matches,
    bronze_fund_no_duplicate_records,
    bronze_fund_record_count_matches,
]
