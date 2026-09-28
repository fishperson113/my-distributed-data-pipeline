"""Remove pre-intraday Bronze rows from a warehouse that predates the conversion.

Ingestion used to land one bar per trading day. It now lands every intraday bar,
keyed on the bar's instant, and the payload carries `ts_epoch`. The conversion
deliberately shipped without a migration, so a warehouse that already holds
daily-shaped rows keeps them -- and the staging models cannot type them: a
payload with no `ts_epoch` yields a null `bar_ts`, which fails the `not_null`
tests and blocks the whole dbt build.

A legacy row is identified by the absence of `ts_epoch` in its payload, not by
its age, so re-running this after the warehouse is clean is a no-op.

Nothing is deleted without `--apply`; the default is a dry run.
"""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass

from dotenv import load_dotenv

BRONZE_TABLES = ("stock", "fund")


@dataclass(frozen=True)
class Plan:
    """What a run would remove, per table."""

    legacy_rows: dict[str, int]
    orphan_batches: int

    @property
    def total_rows(self) -> int:
        return sum(self.legacy_rows.values())

    @property
    def is_empty(self) -> bool:
        return self.total_rows == 0 and self.orphan_batches == 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually delete. Without it the script only reports what it would do.",
    )
    parser.add_argument(
        "--dsn",
        default=None,
        help="Warehouse DSN. Defaults to WAREHOUSE_POSTGRES_DSN from the environment.",
    )
    return parser


def _count_legacy(cursor, table: str) -> int:
    cursor.execute(
        f"SELECT count(*) FROM bronze.{table} WHERE NOT (payload ? 'ts_epoch')"
    )
    return int(cursor.fetchone()[0])


def _count_orphan_batches(cursor) -> int:
    """Batches whose every row is legacy, and so would be left behind empty."""

    cursor.execute(
        """
        SELECT count(*) FROM bronze.ingestion_batch AS batch
        WHERE NOT EXISTS (
            SELECT 1 FROM bronze.stock AS s
            WHERE s.batch_id = batch.batch_id AND s.payload ? 'ts_epoch'
        )
        AND NOT EXISTS (
            SELECT 1 FROM bronze.fund AS f
            WHERE f.batch_id = batch.batch_id AND f.payload ? 'ts_epoch'
        )
        """
    )
    return int(cursor.fetchone()[0])


def survey(cursor) -> Plan:
    """Report what a run would remove, touching nothing."""

    return Plan(
        legacy_rows={table: _count_legacy(cursor, table) for table in BRONZE_TABLES},
        orphan_batches=_count_orphan_batches(cursor),
    )


def purge(cursor) -> Plan:
    """Delete legacy rows, then the batches left holding nothing.

    Child rows go first: `bronze.stock` and `bronze.fund` both carry a foreign
    key onto `bronze.ingestion_batch`.
    """

    removed: dict[str, int] = {}
    for table in BRONZE_TABLES:
        cursor.execute(
            f"DELETE FROM bronze.{table} WHERE NOT (payload ? 'ts_epoch')"
        )
        removed[table] = cursor.rowcount

    cursor.execute(
        """
        DELETE FROM bronze.ingestion_batch AS batch
        WHERE NOT EXISTS (
            SELECT 1 FROM bronze.stock AS s WHERE s.batch_id = batch.batch_id
        )
        AND NOT EXISTS (
            SELECT 1 FROM bronze.fund AS f WHERE f.batch_id = batch.batch_id
        )
        """
    )
    return Plan(legacy_rows=removed, orphan_batches=cursor.rowcount)


def main() -> int:
    """Survey the warehouse, and purge it when --apply is given."""

    import psycopg2

    load_dotenv(override=False)
    args = build_parser().parse_args()
    dsn = (args.dsn or os.getenv("WAREHOUSE_POSTGRES_DSN", "")).strip()
    if not dsn:
        raise ValueError("WAREHOUSE_POSTGRES_DSN cannot be blank.")

    with psycopg2.connect(dsn) as connection:
        with connection.cursor() as cursor:
            plan = survey(cursor)
            for table, count in plan.legacy_rows.items():
                print(f"bronze.{table}: {count} legacy row(s) without ts_epoch")
            print(f"batches left empty by that removal: {plan.orphan_batches}")

            if plan.is_empty:
                print("nothing to clear; the warehouse already holds only intraday bars")
                return 0
            if not args.apply:
                print("dry run: pass --apply to delete")
                return 0

            removed = purge(cursor)

        connection.commit()

    for table, count in removed.legacy_rows.items():
        print(f"deleted {count} row(s) from bronze.{table}")
    print(f"deleted {removed.orphan_batches} empty batch(es)")
    print("re-run dbt build so staging and marts reflect the cleared warehouse")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
