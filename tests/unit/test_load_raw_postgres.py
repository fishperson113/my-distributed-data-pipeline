"""Tests for the raw JSON-to-Postgres command."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path


def _load_script_module() -> object:
    script_path = Path(__file__).parents[2] / "scripts" / "load_postgres_raw.py"
    spec = importlib.util.spec_from_file_location("load_postgres_raw", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_load_raw_files_uses_request_end_for_legacy_raw_directory(
    tmp_path: Path,
    monkeypatch: object,
) -> None:
    module = _load_script_module()
    raw_path = tmp_path / "raw" / "ssi" / "E1VFVN30_daily_2026-08-28_2026-09-27.json"
    raw_path.parent.mkdir(parents=True)
    raw_path.write_text(
        json.dumps(
            {
                "source": "ssi_iboard",
                "dataset": "fund_daily",
                "provider": "SSI",
                "request": {"start": "2026-08-28", "end": "2026-09-27"},
                "records": [{"symbol": "E1VFVN30", "trade_date": "2026-09-27"}],
                "fetched_at": "2026-09-27T09:00:43.654762+00:00",
                "record_count": 1,
            }
        ),
        encoding="utf-8",
    )
    calls: list[dict[str, object]] = []

    def fake_load_extraction_to_postgres(**kwargs: object) -> object:
        calls.append(kwargs)
        return object()

    monkeypatch.setattr(module, "load_extraction_to_postgres", fake_load_extraction_to_postgres)

    loaded = module.load_raw_files_to_postgres(
        raw_storage_path=tmp_path / "raw",
        warehouse_postgres_dsn="postgresql://warehouse",
        json_paths=[raw_path.parent],
    )

    assert loaded == 1
    assert calls == [
        {
            "extraction": module.RawExtraction(
                source="ssi_iboard",
                dataset="fund_daily",
                provider="SSI",
                request={"start": "2026-08-28", "end": "2026-09-27"},
                records=[{"symbol": "E1VFVN30", "trade_date": "2026-09-27"}],
                fetched_at="2026-09-27T09:00:43.654762+00:00",
            ),
            "partition_date": "2026-09-27",
            "warehouse_postgres_dsn": "postgresql://warehouse",
            "raw_path": raw_path,
        }
    ]
