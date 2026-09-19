from pathlib import Path

import pytest

from data_pipeline.config import InfrastructureSettings, load_pipeline_config


def test_infrastructure_settings_load_from_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SSI_HISTORY_URL", "https://example.test/history")
    monkeypatch.setenv("SOURCE_HTTP_TIMEOUT_SECONDS", "12.5")
    monkeypatch.setenv("SOURCE_HTTP_MAX_ATTEMPTS", "5")
    monkeypatch.setenv("RAW_STORAGE_PATH", "custom/raw")
    monkeypatch.setenv("PIPELINE_CONFIG_PATH", "custom-config.yml")

    settings = InfrastructureSettings.from_env()

    assert settings.ssi_history_url == "https://example.test/history"
    assert settings.http_timeout_seconds == 12.5
    assert settings.http_max_attempts == 5
    assert settings.raw_storage_path == Path("custom/raw")
    assert settings.pipeline_config_path == Path("custom-config.yml")


def test_infrastructure_settings_reject_invalid_attempt_count(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SOURCE_HTTP_MAX_ATTEMPTS", "0")

    with pytest.raises(ValueError, match="at least 1"):
        InfrastructureSettings.from_env()


def test_pipeline_config_loads_operating_policy(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yml"
    config_path.write_text(
        """
ingestion:
  stock:
    provider: VCI
    symbols: [fpt, VNM, fpt]
  fund:
    symbol: e1vfvn30
partitions:
  daily_market:
    start_date: "2025-01-01"
    timezone: Asia/Ho_Chi_Minh
schedules:
  daily_market_ingestion:
    hour: 7
    minute: 15
    enabled_by_default: true
""".strip(),
        encoding="utf-8",
    )

    config = load_pipeline_config(config_path)

    assert config.stock.provider == "vci"
    assert config.stock.symbols == ("FPT", "VNM")
    assert config.fund.symbol == "E1VFVN30"
    assert config.daily_partition.start_date == "2025-01-01"
    assert config.daily_partition.timezone == "Asia/Ho_Chi_Minh"
    assert config.daily_schedule.hour == 7
    assert config.daily_schedule.minute == 15
    assert config.daily_schedule.enabled_by_default is True


def test_pipeline_config_rejects_invalid_schedule_hour(tmp_path: Path) -> None:
    config_path = tmp_path / "invalid.yml"
    config_path.write_text(
        """
ingestion:
  stock:
    provider: kbs
    symbols: [FPT]
  fund:
    symbol: E1VFVN30
partitions:
  daily_market:
    start_date: "2025-01-01"
    timezone: Asia/Ho_Chi_Minh
schedules:
  daily_market_ingestion:
    hour: 25
    minute: 0
    enabled_by_default: false
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="hour must be 0..23"):
        load_pipeline_config(config_path)
