"""Unit tests for the legacy Bronze cleanup, without a live database.

The scenario these cover actually happened on the VPS: a warehouse carrying one
day of pre-conversion rows (three stock symbols and one fund) failed the whole
dbt build, because a payload with no ``ts_epoch`` types as a null ``bar_ts``.
"""

from __future__ import annotations

import pytest

from scripts.clear_legacy_bronze import BRONZE_TABLES, Plan, purge, survey


class _FakeCursor:
    """A cursor that answers the counting queries and records the deletes."""

    def __init__(self, legacy: dict[str, int], orphans: int) -> None:
        self._legacy = dict(legacy)
        self._orphans = orphans
        self._result = 0
        self.rowcount = 0
        self.deletes: list[str] = []

    def execute(self, query: str, params: object = None) -> None:
        normalised = " ".join(query.split())
        if normalised.startswith("DELETE FROM bronze.ingestion_batch"):
            self.deletes.append("ingestion_batch")
            self.rowcount = self._orphans
            return
        for table in BRONZE_TABLES:
            if normalised.startswith(f"DELETE FROM bronze.{table}"):
                self.deletes.append(table)
                self.rowcount = self._legacy[table]
                return
        # The orphan-batch count mentions both Bronze tables inside its EXISTS
        # subqueries, so it is matched on its own FROM clause before the
        # per-table counts are considered.
        if normalised.startswith("SELECT count(*) FROM bronze.ingestion_batch"):
            self._result = self._orphans
            return
        for table in BRONZE_TABLES:
            if normalised.startswith(f"SELECT count(*) FROM bronze.{table}"):
                self._result = self._legacy[table]
                return
        raise AssertionError(f"unexpected query: {normalised}")

    def fetchone(self) -> tuple[int]:
        return (self._result,)


def test_survey_reports_legacy_rows_without_deleting_anything() -> None:
    cursor = _FakeCursor({"stock": 3, "fund": 1}, orphans=2)

    plan = survey(cursor)

    assert plan.legacy_rows == {"stock": 3, "fund": 1}
    assert plan.orphan_batches == 2
    assert plan.total_rows == 4
    assert not plan.is_empty
    assert cursor.deletes == []


def test_survey_of_a_clean_warehouse_is_empty() -> None:
    plan = survey(_FakeCursor({"stock": 0, "fund": 0}, orphans=0))

    assert plan.is_empty
    assert plan.total_rows == 0


def test_purge_deletes_child_rows_before_their_batches() -> None:
    """Both Bronze tables carry a foreign key onto ingestion_batch."""

    cursor = _FakeCursor({"stock": 3, "fund": 1}, orphans=2)

    removed = purge(cursor)

    assert cursor.deletes == ["stock", "fund", "ingestion_batch"]
    assert cursor.deletes.index("ingestion_batch") == len(cursor.deletes) - 1
    assert removed.legacy_rows == {"stock": 3, "fund": 1}
    assert removed.orphan_batches == 2


def test_purge_on_a_clean_warehouse_removes_nothing() -> None:
    removed = purge(_FakeCursor({"stock": 0, "fund": 0}, orphans=0))

    assert removed.is_empty


@pytest.mark.parametrize("rows,orphans,expected", [(0, 0, True), (1, 0, False), (0, 1, False)])
def test_plan_reports_emptiness_from_either_side(
    rows: int, orphans: int, expected: bool
) -> None:
    plan = Plan(legacy_rows={"stock": rows, "fund": 0}, orphan_batches=orphans)

    assert plan.is_empty is expected
