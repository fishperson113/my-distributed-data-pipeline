"""Standalone-compatible vnstock extraction client."""

from __future__ import annotations

import json
import os
from collections.abc import Callable, Iterable
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

    market = (market_factory or _default_market_factory)()
    records: list[dict[str, Any]] = []

    for symbol in normalized_symbols:
        frame = market.equity(symbol).ohlcv(
            start=start,
            end=end,
            interval="1D",
            source=normalized_provider,
        )
        symbol_records = _frame_records(frame)
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
            "interval": "1D",
        },
        records=records,
        source_payload=records,
    )

