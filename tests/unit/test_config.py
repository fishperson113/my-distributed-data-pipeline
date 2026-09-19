from pathlib import Path

import pytest

from data_pipeline.config import SourceSettings


def test_source_settings_load_from_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VNSTOCK_PROVIDER", "VCI")
    monkeypatch.setenv("SSI_HISTORY_URL", "https://example.test/history")
    monkeypatch.setenv("SOURCE_HTTP_TIMEOUT_SECONDS", "12.5")
    monkeypatch.setenv("SOURCE_HTTP_MAX_ATTEMPTS", "5")
    monkeypatch.setenv("RAW_STORAGE_PATH", "custom/raw")
    monkeypatch.setenv("STOCK_SYMBOLS", "FPT, vnm,FPT")
    monkeypatch.setenv("FUND_SYMBOL", "e1vfvn30")

    settings = SourceSettings.from_env()

    assert settings.vnstock_provider == "vci"
    assert settings.ssi_history_url == "https://example.test/history"
    assert settings.http_timeout_seconds == 12.5
    assert settings.http_max_attempts == 5
    assert settings.raw_storage_path == Path("custom/raw")
    assert settings.stock_symbols == ("FPT", "VNM")
    assert settings.fund_symbol == "E1VFVN30"


def test_source_settings_reject_invalid_attempt_count(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SOURCE_HTTP_MAX_ATTEMPTS", "0")

    with pytest.raises(ValueError, match="at least 1"):
        SourceSettings.from_env()
