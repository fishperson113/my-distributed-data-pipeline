"""Standalone-compatible SSI iBoard intraday history client.

The Bronze dataset name stays `fund_daily`: it identifies the source feed, not
the bar width, and the Bronze CHECK constraint pins the accepted values. The
grain it now carries is intraday, at the width set in `config.yml`.
"""

from __future__ import annotations

import time
from datetime import UTC, date, datetime, time as datetime_time, timedelta
from typing import Any

import httpx

from data_pipeline.ingestion.common.errors import (
    IntradayGrainError,
    IntradayRetentionError,
    SourceResponseError,
)
from data_pipeline.ingestion.common.models import RawExtraction
from data_pipeline.ingestion.common.timeframe import (
    MARKET_TIMEZONE,
    bar_timestamp_fields,
    from_epoch,
    is_outside_retention,
    resolve_window,
    to_epoch,
    within_window,
)

DEFAULT_SSI_HISTORY_URL = "https://iboard-api.ssi.com.vn/statistics/charts/history"
RETRIABLE_STATUS_CODES = {429, 500, 502, 503, 504}

# Names the source feed, not the bar width. Pinned by the Bronze CHECK
# constraint, so it stays put while the grain underneath it changed.
FUND_DATASET = "fund_daily"


def _request_payload(
    client: httpx.Client,
    *,
    endpoint_url: str,
    params: dict[str, Any],
    max_attempts: int,
) -> dict[str, Any]:
    """Fetch and decode SSI JSON with bounded retries for transient failures."""

    last_error: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            response = client.get(endpoint_url, params=params)
            if response.status_code in RETRIABLE_STATUS_CODES and attempt < max_attempts:
                time.sleep(2 ** (attempt - 1))
                continue
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict):
                raise SourceResponseError("SSI returned a non-object JSON response.")
            return payload
        except (httpx.HTTPError, ValueError, SourceResponseError) as error:
            last_error = error
            if attempt == max_attempts:
                break
            time.sleep(2 ** (attempt - 1))

    raise SourceResponseError(f"SSI request failed after {max_attempts} attempts: {last_error}")


def _ohlcv_arrays(payload: dict[str, Any]) -> dict[str, list[Any]]:
    """Validate the SSI envelope and return its parallel OHLCV arrays."""

    if str(payload.get("code", "")).upper() != "SUCCESS":
        raise SourceResponseError(f"SSI returned a non-success code: {payload.get('code')!r}")

    data = payload.get("data")
    if not isinstance(data, dict):
        raise SourceResponseError("SSI response is missing the data object.")

    arrays: dict[str, list[Any]] = {}
    for field in ("t", "o", "h", "l", "c", "v"):
        value = data.get(field)
        if not isinstance(value, list):
            raise SourceResponseError(f"SSI data field {field!r} is not an array.")
        arrays[field] = value

    lengths = {field: len(values) for field, values in arrays.items()}
    if len(set(lengths.values())) != 1:
        raise SourceResponseError(f"SSI OHLCV arrays have different lengths: {lengths}")
    return arrays


def _assert_intraday_grain(epochs: list[int]) -> None:
    """Reject a daily-grain answer to an intraday request.

    SSI serves daily candles, stamped at midnight UTC, for any `resolution` it
    does not recognise -- and still reports SUCCESS. Every intraday bar carries
    a real session clock, so an all-midnight response means the grain silently
    degraded.
    """

    if not epochs:
        return
    if all(
        datetime.fromtimestamp(epoch, tz=UTC).time() == datetime_time.min for epoch in epochs
    ):
        raise IntradayGrainError(
            "SSI answered an intraday request with daily bars; the resolution was "
            "likely rejected and silently downgraded."
        )


def extract_fund_intraday(
    *,
    symbol: str,
    partition_date: str,
    granularity_minutes: int,
    start_time: str | None = None,
    end_time: str | None = None,
    retention_days: int | None = None,
    timezone_name: str = MARKET_TIMEZONE,
    timeout_seconds: float = 30.0,
    max_attempts: int = 3,
    endpoint_url: str = DEFAULT_SSI_HISTORY_URL,
    client: httpx.Client | None = None,
) -> RawExtraction:
    """Fetch intraday ETF/fund bars for one partition day from SSI iBoard.

    SSI takes `from`/`to` as Unix seconds, so a sub-day window is expressed
    directly in the request rather than filtered afterwards.
    """

    normalized_symbol = symbol.strip().upper()
    if not normalized_symbol:
        raise ValueError("The fund symbol cannot be blank.")
    if max_attempts < 1:
        raise ValueError("max_attempts must be at least 1.")
    if granularity_minutes < 1:
        raise ValueError("granularity_minutes must be at least 1.")

    window_start, window_end = resolve_window(
        partition_date=partition_date,
        start_time=start_time,
        end_time=end_time,
        timezone_name=timezone_name,
    )
    from_timestamp = to_epoch(window_start)
    to_timestamp = to_epoch(window_end)

    params = {
        "symbol": normalized_symbol,
        "resolution": str(granularity_minutes),
        "from": from_timestamp,
        "to": to_timestamp,
    }
    headers = {
        "Accept": "application/json",
        "Origin": "https://iboard.ssi.com.vn",
        "Referer": "https://iboard.ssi.com.vn/",
        "User-Agent": "my-distributed-data-pipeline/0.1",
    }

    owns_client = client is None
    runtime_client = client or httpx.Client(
        timeout=timeout_seconds, headers=headers, follow_redirects=True
    )
    try:
        payload = _request_payload(
            runtime_client,
            endpoint_url=endpoint_url,
            params=params,
            max_attempts=max_attempts,
        )
    finally:
        if owns_client:
            runtime_client.close()

    arrays = _ohlcv_arrays(payload)
    epochs = [int(value) for value in arrays["t"]]
    _assert_intraday_grain(epochs)

    records: list[dict[str, Any]] = []
    for index, epoch in enumerate(epochs):
        moment = from_epoch(epoch, timezone_name)
        if not within_window(moment, window_start, window_end):
            continue
        records.append(
            {
                "symbol": normalized_symbol,
                **bar_timestamp_fields(moment),
                "granularity_minutes": granularity_minutes,
                "open": arrays["o"][index],
                "high": arrays["h"][index],
                "low": arrays["l"][index],
                "close": arrays["c"][index],
                "volume": arrays["v"][index],
            }
        )

    if not records and retention_days is not None:
        if is_outside_retention(window_start=window_start, retention_days=retention_days):
            raise IntradayRetentionError(
                f"SSI returned no intraday bars for {partition_date}, which is older than "
                f"the {retention_days}-day retention window. The source no longer keeps "
                "this partition; it is not an empty trading day."
            )

    return RawExtraction(
        source="ssi_iboard",
        dataset=FUND_DATASET,
        provider="SSI",
        request={
            "url": endpoint_url,
            "symbol": normalized_symbol,
            "partition_date": partition_date,
            "start_time": start_time,
            "end_time": end_time,
            "granularity_minutes": granularity_minutes,
            "resolution": str(granularity_minutes),
            "timezone": timezone_name,
            "from": from_timestamp,
            "to": to_timestamp,
        },
        records=records,
        source_payload=payload,
    )
