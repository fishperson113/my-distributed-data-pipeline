"""Standalone-compatible vnstock extraction client."""

from __future__ import annotations

import json
import os
from collections.abc import Callable, Iterable
from datetime import date, timedelta
from typing import Any

from data_pipeline.ingestion.common.models import RawExtraction

MarketFactory = Callable[[], Any]


def _default_market_factory() -> Any:
    """Create the vnstock Unified API market client with telemetry disabled."""

    os.environ.setdefault("VNSTOCK_TELEMETRY", "off")
    from vnstock import Market

    return Market()


def _frame_records(frame: Any) -> list[dict[str, Any]]:
    """Convert a pandas-compatible frame to JSON-safe records."""

    if frame is None or not hasattr(frame, "to_json"):
        raise TypeError("vnstock returned an unsupported response instead of a DataFrame.")

    return json.loads(frame.to_json(orient="records", date_format="iso"))


def _inclusive_date_range(start: str, end: str) -> tuple[date, date, str]:
    """Validate an inclusive range and derive the provider's exclusive end date."""

    start_date = date.fromisoformat(start)
    end_date = date.fromisoformat(end)
    if end_date < start_date:
        raise ValueError("The end date must be on or after the start date.")
    return start_date, end_date, (end_date + timedelta(days=1)).isoformat()


def _within_requested_range(record: dict[str, Any], start: date, end: date) -> bool:
    """Keep provider rows inside the public inclusive date contract."""

    value = record.get("time")
    if not isinstance(value, str) or len(value) < 10:
        return True

    try:
        record_date = date.fromisoformat(value[:10])
    except ValueError:
        return True
    return start <= record_date <= end


def extract_stock_daily(
    *,
    symbols: Iterable[str],
    start: str,
    end: str,
    provider: str = "kbs",
    market_factory: MarketFactory | None = None,
) -> RawExtraction:
    """Fetch daily OHLCV rows for one or more stock symbols via vnstock."""

    normalized_symbols = sorted({symbol.strip().upper() for symbol in symbols if symbol.strip()})
    if not normalized_symbols:
        raise ValueError("At least one stock symbol is required.")

    normalized_provider = provider.strip().lower()
    if not normalized_provider:
        raise ValueError("The vnstock provider cannot be blank.")

    start_date, end_date, provider_end_exclusive = _inclusive_date_range(start, end)
    market = (market_factory or _default_market_factory)()
    records: list[dict[str, Any]] = []

    for symbol in normalized_symbols:
        frame = market.equity(symbol).ohlcv(
            start=start,
            end=provider_end_exclusive,
            interval="1D",
            source=normalized_provider,
        )
        symbol_records = [
            record
            for record in _frame_records(frame)
            if _within_requested_range(record, start_date, end_date)
        ]
        for record in symbol_records:
            record.setdefault("symbol", symbol)
            records.append(record)

    return RawExtraction(
        source="vnstock",
        dataset="stock_daily",
        provider=normalized_provider,
        request={
            "symbols": normalized_symbols,
            "start": start,
            "end": end,
            "provider_end_exclusive": provider_end_exclusive,
            "interval": "1D",
        },
        records=records,
        source_payload=records,
    )
