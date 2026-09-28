"""Standalone-compatible vnstock intraday extraction client.

The Bronze dataset name stays `stock_daily`: it identifies the source feed, not
the bar width, and the Bronze CHECK constraint pins the accepted values. The
grain it now carries is intraday, at the width set in `config.yml`.
"""

from __future__ import annotations

import json
import math
import os
from collections.abc import Callable, Iterable
from datetime import date, timedelta
from typing import Any

from data_pipeline.ingestion.common.errors import (
    IntradayRetentionError,
    SourceResponseError,
)
from data_pipeline.ingestion.common.models import RawExtraction
from data_pipeline.ingestion.common.timeframe import (
    MARKET_TIMEZONE,
    bar_timestamp_fields,
    from_naive_local,
    is_outside_retention,
    resolve_window,
    within_window,
)

# A VN session spans 09:15..14:45 with a midday break, so 400 minutes covers a
# day end to end with room to spare. vnstock defaults `count` to 100 and returns
# the LAST `count` bars, silently dropping the start of the range, so every
# intraday call sizes and passes `count` itself.
SESSION_MINUTES_UPPER_BOUND = 400
MINIMUM_BAR_BUDGET = 500

# Names the source feed, not the bar width. Pinned by the Bronze CHECK
# constraint, so it stays put while the grain underneath it changed.
STOCK_DATASET = "stock_daily"

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


def _bar_budget(*, granularity_minutes: int, span_days: int) -> int:
    """Size the vnstock `count` so a whole window survives the provider's tail cut."""

    bars_per_day = math.ceil(SESSION_MINUTES_UPPER_BOUND / granularity_minutes) + 2
    return max(MINIMUM_BAR_BUDGET, bars_per_day * max(span_days, 1) * 2)


def extract_stock_intraday(
    *,
    symbols: Iterable[str],
    partition_date: str,
    granularity_minutes: int,
    provider: str = "kbs",
    start_time: str | None = None,
    end_time: str | None = None,
    retention_days: int | None = None,
    timezone_name: str = MARKET_TIMEZONE,
    market_factory: MarketFactory | None = None,
) -> RawExtraction:
    """Fetch intraday OHLCV bars for one partition day via vnstock.

    vnstock only accepts plain ``YYYY-MM-DD`` bounds -- passing a clock component
    raises ``Dữ liệu trống`` -- so the whole day is requested and any sub-day
    window is applied here, on the normalised timestamps.
    """

    normalized_symbols = sorted({symbol.strip().upper() for symbol in symbols if symbol.strip()})
    if not normalized_symbols:
        raise ValueError("At least one stock symbol is required.")

    normalized_provider = provider.strip().lower()
    if not normalized_provider:
        raise ValueError("The vnstock provider cannot be blank.")
    if granularity_minutes < 1:
        raise ValueError("granularity_minutes must be at least 1.")

    window_start, window_end = resolve_window(
        partition_date=partition_date,
        start_time=start_time,
        end_time=end_time,
        timezone_name=timezone_name,
    )
    day = date.fromisoformat(partition_date)
    provider_end_exclusive = (day + timedelta(days=1)).isoformat()
    interval = f"{granularity_minutes}m"
    count = _bar_budget(granularity_minutes=granularity_minutes, span_days=1)

    market = (market_factory or _default_market_factory)()
    records: list[dict[str, Any]] = []

    for symbol in normalized_symbols:
        frame = market.equity(symbol).ohlcv(
            start=partition_date,
            end=provider_end_exclusive,
            interval=interval,
            count=count,
            source=normalized_provider,
        )
        source_records = _frame_records(frame)
        if len(source_records) >= count:
            raise SourceResponseError(
                f"vnstock returned {len(source_records)} bars for {symbol} at the "
                f"count ceiling of {count}; the window was probably truncated."
            )
        for record in source_records:
            raw_time = record.get("time")
            if raw_time is None:
                continue
            moment = from_naive_local(raw_time, timezone_name)
            if not within_window(moment, window_start, window_end):
                continue
            bar = {
                "symbol": symbol,
                **bar_timestamp_fields(moment),
                "granularity_minutes": granularity_minutes,
                "open": record.get("open"),
                "high": record.get("high"),
                "low": record.get("low"),
                "close": record.get("close"),
                "volume": record.get("volume"),
                "source_time": raw_time,
            }
            records.append(bar)

    if not records and retention_days is not None:
        if is_outside_retention(window_start=window_start, retention_days=retention_days):
            raise IntradayRetentionError(
                f"vnstock returned no intraday bars for {partition_date}, which is older "
                f"than the {retention_days}-day retention window. The source no longer "
                "keeps this partition; it is not an empty trading day."
            )

    return RawExtraction(
        source="vnstock",
        dataset=STOCK_DATASET,
        provider=normalized_provider,
        request={
            "symbols": normalized_symbols,
            "partition_date": partition_date,
            "start_time": start_time,
            "end_time": end_time,
            "granularity_minutes": granularity_minutes,
            "interval": interval,
            "count": count,
            "provider_end_exclusive": provider_end_exclusive,
            "timezone": timezone_name,
        },
        records=records,
        source_payload=records,
    )
