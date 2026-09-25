"""Mirror `storage/raw` JSON envelopes into MongoDB as the Bronze raw landing layer.

The filesystem envelope remains the source of truth. Mongo is an optional,
dataset-aligned compatibility mirror. Each document is one logical record
(not one envelope), upserted by a deterministic natural key so reruns stay
idempotent.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterable, Iterator
from pathlib import Path
from typing import Any, Protocol

RECORD_KEY_BUILDERS: dict[str, Callable[[dict[str, Any]], str]] = {
    "stock_daily": lambda record: f"{record['symbol']}:{str(record['time'])[:10]}",
    "fund_daily": lambda record: f"{record['symbol']}:{record['trade_date']}",
}


class SupportsUpsert(Protocol):
    def replace_one(
        self, filter: dict[str, Any], replacement: dict[str, Any], upsert: bool
    ) -> Any: ...


class SupportsCollection(Protocol):
    def __getitem__(self, name: str) -> SupportsUpsert: ...


MongoDatabaseFactory = Callable[[str, str], SupportsCollection]


def _default_database_factory(mongo_uri: str, mongo_database: str) -> SupportsCollection:
    """Connect to Mongo and return the target database handle."""

    from pymongo import MongoClient

    return MongoClient(mongo_uri)[mongo_database]


def iter_raw_envelopes(
    raw_storage_path: Path,
    json_paths: Iterable[Path] | None = None,
) -> Iterator[tuple[Path, dict[str, Any]]]:
    """Yield JSON raw extraction envelopes from the default root or explicit paths."""

    if json_paths is None:
        # Keep the source/dataset/date partition convention for the default
        # root, while accepting any JSON filename.
        candidates = raw_storage_path.glob("*/*/date=*/*.json")
    else:
        discovered: set[Path] = set()
        for path in json_paths:
            if path.is_file() and path.suffix.lower() == ".json":
                discovered.add(path)
            elif path.is_dir():
                discovered.update(path.rglob("*.json"))
        candidates = discovered

    for json_path in sorted(candidates):
        envelope = json.loads(json_path.read_text(encoding="utf-8"))
        yield json_path, envelope


def documents_from_envelope(envelope: dict[str, Any]) -> Iterator[tuple[str, dict[str, Any]]]:
    """Yield (collection_name, document) pairs for one raw extraction envelope."""

    dataset = envelope["dataset"]
    key_builder = RECORD_KEY_BUILDERS.get(dataset)
    if key_builder is None:
        raise ValueError(f"No Mongo record-key builder registered for dataset {dataset!r}.")

    for record in envelope["records"]:
        document = dict(record)
        document["_id"] = key_builder(record)
        document["source"] = envelope["source"]
        document["provider"] = envelope["provider"]
        document["fetched_at"] = envelope["fetched_at"]
        yield dataset, document


def load_raw_files_to_mongo(
    *,
    raw_storage_path: Path,
    mongo_uri: str,
    mongo_database: str,
    database_factory: MongoDatabaseFactory | None = None,
    json_paths: Iterable[Path] | None = None,
) -> int:
    """Upsert every record from `storage/raw` envelopes into dataset-aligned Mongo collections."""

    database = (database_factory or _default_database_factory)(mongo_uri, mongo_database)
    upserted = 0
    for _, envelope in iter_raw_envelopes(raw_storage_path, json_paths=json_paths):
        for collection_name, document in documents_from_envelope(envelope):
            database[collection_name].replace_one(
                {"_id": document["_id"]}, document, upsert=True
            )
            upserted += 1
    return upserted
