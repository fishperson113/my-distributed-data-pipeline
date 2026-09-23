from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from data_pipeline.warehouse.mongo_sink import (
    documents_from_envelope,
    iter_raw_envelopes,
    load_raw_files_to_mongo,
)


def _write_envelope(path: Path, envelope: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(envelope), encoding="utf-8")


def _stock_envelope() -> dict[str, Any]:
    return {
        "source": "vnstock",
        "dataset": "stock_daily",
        "provider": "kbs",
        "fetched_at": "2026-09-19T00:00:00+00:00",
        "records": [
            {"symbol": "FPT", "time": "2026-09-18T00:00:00.000Z", "close": 103.0},
        ],
    }


def _fund_envelope() -> dict[str, Any]:
    return {
        "source": "ssi_iboard",
        "dataset": "fund_daily",
        "provider": "SSI",
        "fetched_at": "2026-09-19T00:00:00+00:00",
        "records": [
            {"symbol": "E1VFVN30", "trade_date": "2026-09-18", "close": 35.5},
        ],
    }


def test_iter_raw_envelopes_finds_any_json_filename(tmp_path: Path) -> None:
    _write_envelope(
        tmp_path / "vnstock" / "stock_daily" / "date=2026-09-18" / "fpt.json",
        _stock_envelope(),
    )

    found = list(iter_raw_envelopes(tmp_path))

    assert len(found) == 1
    assert found[0][1]["dataset"] == "stock_daily"


def test_documents_from_envelope_builds_stable_stock_key() -> None:
    documents = list(documents_from_envelope(_stock_envelope()))

    assert len(documents) == 1
    collection_name, document = documents[0]
    assert collection_name == "stock_daily"
    assert document["_id"] == "FPT:2026-09-18"
    assert document["source"] == "vnstock"
    assert document["provider"] == "kbs"


def test_documents_from_envelope_builds_stable_fund_key() -> None:
    documents = list(documents_from_envelope(_fund_envelope()))

    assert len(documents) == 1
    _, document = documents[0]
    assert document["_id"] == "E1VFVN30:2026-09-18"


class _FakeCollection:
    def __init__(self) -> None:
        self.upserted: list[dict[str, Any]] = []

    def replace_one(
        self, filter: dict[str, Any], replacement: dict[str, Any], upsert: bool
    ) -> None:
        assert upsert is True
        self.upserted.append(replacement)


class _FakeDatabase:
    def __init__(self) -> None:
        self.collections: dict[str, _FakeCollection] = {}

    def __getitem__(self, name: str) -> _FakeCollection:
        return self.collections.setdefault(name, _FakeCollection())


def test_load_raw_files_to_mongo_upserts_every_record(tmp_path: Path) -> None:
    _write_envelope(
        tmp_path / "vnstock" / "stock_daily" / "date=2026-09-18" / "payload.json",
        _stock_envelope(),
    )
    _write_envelope(
        tmp_path / "ssi" / "fund_daily" / "date=2026-09-18" / "payload.json",
        _fund_envelope(),
    )
    fake_database = _FakeDatabase()

    upserted = load_raw_files_to_mongo(
        raw_storage_path=tmp_path,
        mongo_uri="mongodb://unused",
        mongo_database="unused",
        database_factory=lambda _uri, _db: fake_database,
    )

    assert upserted == 2
    assert len(fake_database.collections["stock_daily"].upserted) == 1
    assert len(fake_database.collections["fund_daily"].upserted) == 1
