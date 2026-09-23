"""Load Bronze records from Mongo into DuckDB `raw` tables for dbt to transform.

Each DuckDB raw table keeps one JSON `payload` column per Mongo document,
mirroring the JSONB-payload Bronze pattern, so dbt staging models own field
extraction and typing rather than this loader.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any, Protocol

from data_pipeline.warehouse.mongo_sink import RECORD_KEY_BUILDERS


class SupportsFind(Protocol):
    def find(self) -> Iterable[dict[str, Any]]: ...


class SupportsDatabase(Protocol):
    def __getitem__(self, name: str) -> SupportsFind: ...


MongoDatabaseFactory = Callable[[str, str], SupportsDatabase]


def _default_database_factory(mongo_uri: str, mongo_database: str) -> SupportsDatabase:
    """Connect to Mongo and return the source database handle."""

    from pymongo import MongoClient

    return MongoClient(mongo_uri)[mongo_database]


def load_mongo_to_duckdb(
    *,
    mongo_uri: str,
    mongo_database: str,
    duckdb_path: Path,
    database_factory: MongoDatabaseFactory | None = None,
) -> dict[str, int]:
    """Replace each `raw.<dataset>` DuckDB table with the current Mongo collection contents."""

    import duckdb

    database = (database_factory or _default_database_factory)(mongo_uri, mongo_database)
    duckdb_path.parent.mkdir(parents=True, exist_ok=True)

    loaded_counts: dict[str, int] = {}
    connection = duckdb.connect(str(duckdb_path))
    try:
        connection.execute("CREATE SCHEMA IF NOT EXISTS raw")
        for dataset in RECORD_KEY_BUILDERS:
            connection.execute(f'CREATE OR REPLACE TABLE raw."{dataset}" (id VARCHAR PRIMARY KEY, payload JSON)')
            rows = [(document["_id"], json.dumps(document)) for document in database[dataset].find()]
            if rows:
                connection.executemany(
                    f'INSERT INTO raw."{dataset}" (id, payload) VALUES (?, ?::JSON)', rows
                )
            loaded_counts[dataset] = len(rows)
    finally:
        connection.close()

    return loaded_counts
