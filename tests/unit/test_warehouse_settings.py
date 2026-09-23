from pathlib import Path

import pytest

from data_pipeline.warehouse.settings import WarehouseSettings


def test_warehouse_settings_load_from_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MONGO_URI", "mongodb://root:pw@localhost:27017")
    monkeypatch.setenv("MONGO_DATABASE", "bronze")
    monkeypatch.setenv("DUCKDB_PATH", "custom/warehouse.duckdb")
    monkeypatch.setenv("WAREHOUSE_POSTGRES_DSN", "postgresql://u:p@localhost:5433/warehouse")

    settings = WarehouseSettings.from_env()

    assert settings.mongo_uri == "mongodb://root:pw@localhost:27017"
    assert settings.mongo_database == "bronze"
    assert settings.duckdb_path == Path("custom/warehouse.duckdb")
    assert settings.warehouse_postgres_dsn == "postgresql://u:p@localhost:5433/warehouse"


def test_warehouse_settings_rejects_blank_mongo_uri(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MONGO_URI", "")
    monkeypatch.setenv("MONGO_DATABASE", "bronze")
    monkeypatch.setenv("WAREHOUSE_POSTGRES_DSN", "postgresql://u:p@localhost:5433/warehouse")

    with pytest.raises(ValueError, match="MONGO_URI"):
        WarehouseSettings.from_env()


def test_warehouse_settings_rejects_blank_postgres_dsn(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MONGO_URI", "mongodb://root:pw@localhost:27017")
    monkeypatch.setenv("MONGO_DATABASE", "bronze")
    monkeypatch.setenv("WAREHOUSE_POSTGRES_DSN", "")

    with pytest.raises(ValueError, match="WAREHOUSE_POSTGRES_DSN"):
        WarehouseSettings.from_env()
