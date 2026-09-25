import pytest

from data_pipeline.warehouse.settings import MongoSettings


def test_mongo_settings_load_from_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MONGO_URI", "mongodb://root:pw@localhost:27017")
    monkeypatch.setenv("MONGO_DATABASE", "bronze")

    settings = MongoSettings.from_env()

    assert settings.mongo_uri == "mongodb://root:pw@localhost:27017"
    assert settings.mongo_database == "bronze"


def test_warehouse_settings_rejects_blank_mongo_uri(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MONGO_URI", "")
    monkeypatch.setenv("MONGO_DATABASE", "bronze")

    with pytest.raises(ValueError, match="MONGO_URI"):
        MongoSettings.from_env()


def test_mongo_settings_rejects_blank_mongo_database(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MONGO_URI", "mongodb://root:pw@localhost:27017")
    monkeypatch.setenv("MONGO_DATABASE", "")

    with pytest.raises(ValueError, match="MONGO_DATABASE"):
        MongoSettings.from_env()
