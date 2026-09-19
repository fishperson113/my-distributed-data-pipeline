"""Infrastructure environment settings and versioned operating policy."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv


@dataclass(frozen=True)
class InfrastructureSettings:
    """Deployment-specific settings loaded from the environment."""

    ssi_history_url: str
    http_timeout_seconds: float
    http_max_attempts: int
    raw_storage_path: Path
    pipeline_config_path: Path

    @classmethod
    def from_env(cls) -> "InfrastructureSettings":
        """Load infrastructure settings from process env with a `.env` fallback."""

        load_dotenv(override=False)
        timeout_seconds = float(os.getenv("SOURCE_HTTP_TIMEOUT_SECONDS", "30"))
        if timeout_seconds <= 0:
            raise ValueError("SOURCE_HTTP_TIMEOUT_SECONDS must be greater than 0.")

        max_attempts = int(os.getenv("SOURCE_HTTP_MAX_ATTEMPTS", "3"))
        if max_attempts < 1:
            raise ValueError("SOURCE_HTTP_MAX_ATTEMPTS must be at least 1.")

        ssi_history_url = os.getenv(
            "SSI_HISTORY_URL",
            "https://iboard-api.ssi.com.vn/statistics/charts/history",
        ).strip()
        if not ssi_history_url:
            raise ValueError("SSI_HISTORY_URL cannot be blank.")

        return cls(
            ssi_history_url=ssi_history_url,
            http_timeout_seconds=timeout_seconds,
            http_max_attempts=max_attempts,
            raw_storage_path=Path(os.getenv("RAW_STORAGE_PATH", "storage/raw")),
            pipeline_config_path=Path(os.getenv("PIPELINE_CONFIG_PATH", "config.yml")),
        )


@dataclass(frozen=True)
class StockPolicy:
    provider: str
    symbols: tuple[str, ...]


@dataclass(frozen=True)
class FundPolicy:
    symbol: str


@dataclass(frozen=True)
class DailyPartitionPolicy:
    start_date: str
    timezone: str


@dataclass(frozen=True)
class DailySchedulePolicy:
    hour: int
    minute: int
    enabled_by_default: bool


@dataclass(frozen=True)
class PipelineConfig:
    """Versioned policy controlling what the pipeline runs and when."""

    stock: StockPolicy
    fund: FundPolicy
    daily_partition: DailyPartitionPolicy
    daily_schedule: DailySchedulePolicy


def _mapping(value: Any, path: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{path} must be a YAML mapping.")
    return value


def _required_text(mapping: dict[str, Any], key: str, path: str) -> str:
    value = mapping.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{path}.{key} must be a non-empty string.")
    return value.strip()


@lru_cache(maxsize=8)
def load_pipeline_config(path: str | Path | None = None) -> PipelineConfig:
    """Load and validate the operational YAML configuration."""

    infrastructure = InfrastructureSettings.from_env()
    config_path = Path(path) if path is not None else infrastructure.pipeline_config_path
    if not config_path.is_file():
        raise FileNotFoundError(f"Pipeline config not found: {config_path}")

    raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    root = _mapping(raw, "config")
    ingestion = _mapping(root.get("ingestion"), "ingestion")
    stock = _mapping(ingestion.get("stock"), "ingestion.stock")
    fund = _mapping(ingestion.get("fund"), "ingestion.fund")
    partitions = _mapping(root.get("partitions"), "partitions")
    daily_partition = _mapping(partitions.get("daily_market"), "partitions.daily_market")
    schedules = _mapping(root.get("schedules"), "schedules")
    daily_schedule = _mapping(
        schedules.get("daily_market_ingestion"),
        "schedules.daily_market_ingestion",
    )

    provider = _required_text(stock, "provider", "ingestion.stock").lower()
    if provider not in {"kbs", "vci"}:
        raise ValueError("ingestion.stock.provider must be either 'kbs' or 'vci'.")

    raw_symbols = stock.get("symbols")
    if not isinstance(raw_symbols, list):
        raise ValueError("ingestion.stock.symbols must be a YAML list.")
    symbols = tuple(
        dict.fromkeys(
            str(symbol).strip().upper() for symbol in raw_symbols if str(symbol).strip()
        )
    )
    if not symbols:
        raise ValueError("ingestion.stock.symbols must contain at least one symbol.")

    fund_symbol = _required_text(fund, "symbol", "ingestion.fund").upper()
    start_date = _required_text(daily_partition, "start_date", "partitions.daily_market")
    date.fromisoformat(start_date)
    timezone = _required_text(daily_partition, "timezone", "partitions.daily_market")

    hour = daily_schedule.get("hour")
    minute = daily_schedule.get("minute")
    enabled = daily_schedule.get("enabled_by_default")
    if not isinstance(hour, int) or not 0 <= hour <= 23:
        raise ValueError("schedules.daily_market_ingestion.hour must be 0..23.")
    if not isinstance(minute, int) or not 0 <= minute <= 59:
        raise ValueError("schedules.daily_market_ingestion.minute must be 0..59.")
    if not isinstance(enabled, bool):
        raise ValueError(
            "schedules.daily_market_ingestion.enabled_by_default must be boolean."
        )

    return PipelineConfig(
        stock=StockPolicy(provider=provider, symbols=symbols),
        fund=FundPolicy(symbol=fund_symbol),
        daily_partition=DailyPartitionPolicy(start_date=start_date, timezone=timezone),
        daily_schedule=DailySchedulePolicy(
            hour=hour,
            minute=minute,
            enabled_by_default=enabled,
        ),
    )
