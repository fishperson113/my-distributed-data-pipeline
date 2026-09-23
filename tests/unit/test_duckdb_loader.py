from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

duckdb = pytest.importorskip("duckdb")

from data_pipeline.warehouse.duckdb_loader import load_mongo_to_duckdb  # noqa: E402


class _FakeCollection:
    def __init__(self, documents: list[dict[str, Any]]) -> None:
        self._documents = documents

    def find(self) -> list[dict[str, Any]]:
        return self._documents


class _FakeDatabase:
    def __init__(self, collections: dict[str, list[dict[str, Any]]]) -> None:
        self._collections = {name: _FakeCollection(docs) for name, docs in collections.items()}

    def __getitem__(self, name: str) -> _FakeCollection:
        return self._collections.get(name, _FakeCollection([]))


def test_load_mongo_to_duckdb_writes_raw_tables(tmp_path: Path) -> None:
    fake_database = _FakeDatabase(
        {
            "stock_daily": [{"_id": "FPT:2026-09-18", "symbol": "FPT", "close": 103.0}],
            "fund_daily": [{"_id": "E1VFVN30:2026-09-18", "symbol": "E1VFVN30", "close": 35.5}],
        }
    )
    duckdb_path = tmp_path / "warehouse.duckdb"

    loaded_counts = load_mongo_to_duckdb(
        mongo_uri="mongodb://unused",
        mongo_database="unused",
        duckdb_path=duckdb_path,
        database_factory=lambda _uri, _db: fake_database,
    )

    assert loaded_counts == {"stock_daily": 1, "fund_daily": 1}

    connection = duckdb.connect(str(duckdb_path))
    try:
        row = connection.execute(
            "SELECT payload ->> 'symbol' FROM raw.stock_daily WHERE id = ?",
            ["FPT:2026-09-18"],
        ).fetchone()
    finally:
        connection.close()
    assert row is not None
    assert row[0] == "FPT"
