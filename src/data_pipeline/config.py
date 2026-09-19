"""Environment-backed runtime settings shared by standalone and Dagster code."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


@dataclass(frozen=True)
class SourceSettings:
    """Source settings that vary between local development and the VPS."""

    vnstock_provider: str
    ssi_history_url: str
    http_timeout_seconds: float
    http_max_attempts: int
    raw_storage_path: Path
    stock_symbols: tuple[str, ...]
    fund_symbol: str

    @classmethod
    def from_env(cls) -> "SourceSettings":
        """Load settings from process environment, with a local `.env` fallback."""

        load_dotenv(override=False)

        provider = os.getenv("VNSTOCK_PROVIDER", "kbs").strip().lower()
        if provider not in {"kbs", "vci"}:
            raise ValueError("VNSTOCK_PROVIDER must be either 'kbs' or 'vci'.")

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

        raw_storage_path = Path(os.getenv("RAW_STORAGE_PATH", "storage/raw"))

        stock_symbols = tuple(
            dict.fromkeys(
                symbol.strip().upper()
                for symbol in os.getenv("STOCK_SYMBOLS", "FPT").split(",")
                if symbol.strip()
            )
        )
        if not stock_symbols:
            raise ValueError("STOCK_SYMBOLS must contain at least one symbol.")

        fund_symbol = os.getenv("FUND_SYMBOL", "E1VFVN30").strip().upper()
        if not fund_symbol:
            raise ValueError("FUND_SYMBOL cannot be blank.")

        return cls(
            vnstock_provider=provider,
            ssi_history_url=ssi_history_url,
            http_timeout_seconds=timeout_seconds,
            http_max_attempts=max_attempts,
            raw_storage_path=raw_storage_path,
            stock_symbols=stock_symbols,
            fund_symbol=fund_symbol,
        )
