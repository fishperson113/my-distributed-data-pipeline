"""Standalone-compatible SSI iBoard history client."""

from __future__ import annotations

import time
from datetime import UTC, date, datetime, time as datetime_time, timedelta
from typing import Any

import httpx

from data_pipeline.ingestion.common.models import RawExtraction

DEFAULT_SSI_HISTORY_URL = "https://iboard-api.ssi.com.vn/statistics/charts/history"
RETRIABLE_STATUS_CODES = {429, 500, 502, 503, 504}


class SourceResponseError(RuntimeError):
    """Raised when the upstream response cannot form a trustworthy raw extract."""


def _unix_range(start: str, end: str) -> tuple[int, int]:
    """Convert inclusive ISO dates to the SSI Unix-second request range."""

    start_date = date.fromisoformat(start)
    end_date = date.fromisoformat(end)
    if end_date < start_date:
        raise ValueError("The end date must be on or after the start date.")

    start_at = datetime.combine(start_date, datetime_time.min, tzinfo=UTC)
    end_exclusive = datetime.combine(end_date + timedelta(days=1), datetime_time.min, tzinfo=UTC)
    return int(start_at.timestamp()), int(end_exclusive.timestamp())


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


def _daily_records(payload: dict[str, Any], symbol: str) -> list[dict[str, Any]]:
    """Convert SSI parallel OHLCV arrays into lossless logical candle records."""

    if str(payload.get("code", "")).upper() != "SUCCESS":
        raise SourceResponseError(f"SSI returned a non-success code: {payload.get('code')!r}")

    data = payload.get("data")
    if not isinstance(data, dict):
        raise SourceResponseError("SSI response is missing the data object.")

    required_fields = ("t", "o", "h", "l", "c", "v")
    arrays: dict[str, list[Any]] = {}
    for field in required_fields:
        value = data.get(field)
        if not isinstance(value, list):
            raise SourceResponseError(f"SSI data field {field!r} is not an array.")
        arrays[field] = value

    lengths = {field: len(values) for field, values in arrays.items()}
    if len(set(lengths.values())) != 1:
        raise SourceResponseError(f"SSI OHLCV arrays have different lengths: {lengths}")

    records: list[dict[str, Any]] = []
    for index in range(lengths["t"]):
        timestamp = int(arrays["t"][index])
        records.append(
            {
                "symbol": symbol,
                "timestamp": timestamp,
                "trade_date": datetime.fromtimestamp(timestamp, tz=UTC).date().isoformat(),
                "open": arrays["o"][index],
                "high": arrays["h"][index],
                "low": arrays["l"][index],
                "close": arrays["c"][index],
                "volume": arrays["v"][index],
            }
        )
    return records


def extract_fund_daily(
    *,
    symbol: str,
    start: str,
    end: str,
    timeout_seconds: float = 30.0,
    max_attempts: int = 3,
    endpoint_url: str = DEFAULT_SSI_HISTORY_URL,
    client: httpx.Client | None = None,
) -> RawExtraction:
    """Fetch daily ETF/fund OHLCV data from the SSI public-facing endpoint."""

    normalized_symbol = symbol.strip().upper()
    if not normalized_symbol:
        raise ValueError("The fund symbol cannot be blank.")
    if max_attempts < 1:
        raise ValueError("max_attempts must be at least 1.")

    from_timestamp, to_timestamp = _unix_range(start, end)
    params = {
        "symbol": normalized_symbol,
        "resolution": "D",
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
    runtime_client = client or httpx.Client(timeout=timeout_seconds, headers=headers, follow_redirects=True)
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

    records = _daily_records(payload, normalized_symbol)
    return RawExtraction(
        source="ssi_iboard",
        dataset="fund_daily",
        provider="SSI",
        request={
            "url": endpoint_url,
            "symbol": normalized_symbol,
            "start": start,
            "end": end,
            "resolution": "D",
            "from": from_timestamp,
            "to": to_timestamp,
        },
        records=records,
        source_payload=payload,
    )
