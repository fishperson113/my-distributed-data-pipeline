"""Unit tests for the intraday source adapters, without live network calls.

The guards asserted here each correspond to a behaviour observed against the
live endpoints: SSI serving daily bars for an unrecognised resolution, both
sources answering an expired window with an empty success, and vnstock capping
a response at its count argument.
"""

from __future__ import annotations

import httpx
import pandas as pd
import pytest

from data_pipeline.ingestion.common.errors import (
    IntradayGrainError,
    IntradayRetentionError,
    SourceResponseError,
)
from data_pipeline.ingestion.fund.client import extract_fund_intraday
from data_pipeline.ingestion.stock.client import extract_stock_intraday

# 2026-09-25 09:15 and 09:30 exchange time.
BAR_ONE = 1790302500
BAR_TWO = 1790303400
# The same day at midnight UTC: what SSI returns when it falls back to daily.
DAILY_FALLBACK = 1790294400


def _ssi_response(epochs: list[int]) -> dict[str, object]:
    size = len(epochs)
    return {
        "code": "SUCCESS",
        "data": {
            "t": epochs,
            "o": [34.7] * size,
            "h": [34.8] * size,
            "l": [34.6] * size,
            "c": [34.75] * size,
            "v": [1000] * size,
        },
    }


def _ssi_client(payload: dict[str, object]) -> httpx.Client:
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payload)

    return httpx.Client(transport=httpx.MockTransport(handler))


def test_fund_intraday_normalises_bars_onto_the_shared_axis() -> None:
    with _ssi_client(_ssi_response([BAR_ONE, BAR_TWO])) as client:
        extraction = extract_fund_intraday(
            symbol="e1vfvn30",
            partition_date="2026-09-25",
            granularity_minutes=15,
            client=client,
        )

    assert extraction.dataset == "fund_daily"
    assert extraction.record_count == 2
    first = extraction.records[0]
    assert first["symbol"] == "E1VFVN30"
    assert first["ts"] == "2026-09-25T09:15:00+07:00"
    assert first["ts_epoch"] == BAR_ONE
    assert first["trade_time"] == "09:15:00"
    assert first["granularity_minutes"] == 15


def test_fund_intraday_requests_the_sub_day_window_it_was_given() -> None:
    captured: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured.update(dict(request.url.params))
        return httpx.Response(200, json=_ssi_response([BAR_ONE]))

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        extract_fund_intraday(
            symbol="E1VFVN30",
            partition_date="2026-09-25",
            granularity_minutes=15,
            start_time="09:00",
            end_time="09:30",
            client=client,
        )

    assert captured["resolution"] == "15"
    assert int(captured["from"]) == 1790301600
    assert int(captured["to"]) == 1790303400


def test_fund_intraday_drops_bars_outside_the_requested_window() -> None:
    with _ssi_client(_ssi_response([BAR_ONE, BAR_TWO])) as client:
        extraction = extract_fund_intraday(
            symbol="E1VFVN30",
            partition_date="2026-09-25",
            granularity_minutes=15,
            start_time="09:00",
            end_time="09:30",
            client=client,
        )

    assert [record["ts_epoch"] for record in extraction.records] == [BAR_ONE]


def test_fund_intraday_rejects_a_silent_downgrade_to_daily_bars() -> None:
    with _ssi_client(_ssi_response([DAILY_FALLBACK])) as client:
        with pytest.raises(IntradayGrainError, match="daily bars"):
            extract_fund_intraday(
                symbol="E1VFVN30",
                partition_date="2026-09-25",
                granularity_minutes=15,
                client=client,
            )


def test_fund_intraday_flags_an_expired_partition_rather_than_landing_nothing() -> None:
    with _ssi_client(_ssi_response([])) as client:
        with pytest.raises(IntradayRetentionError, match="retention window"):
            extract_fund_intraday(
                symbol="E1VFVN30",
                partition_date="2020-01-02",
                granularity_minutes=15,
                retention_days=30,
                client=client,
            )


def test_fund_intraday_accepts_an_empty_day_inside_retention() -> None:
    """A holiday inside the window is legitimately empty, not a failure."""

    with _ssi_client(_ssi_response([])) as client:
        extraction = extract_fund_intraday(
            symbol="E1VFVN30",
            partition_date="2026-09-25",
            granularity_minutes=15,
            retention_days=100_000,
            client=client,
        )

    assert extraction.record_count == 0


class _RecordingEquity:
    """A vnstock equity stub that records how it was called."""

    def __init__(self, calls: list[dict[str, object]], frame: pd.DataFrame) -> None:
        self._calls = calls
        self._frame = frame

    def ohlcv(self, **kwargs: object) -> pd.DataFrame:
        self._calls.append(kwargs)
        return self._frame


def _market_factory(calls: list[dict[str, object]], frame: pd.DataFrame):
    class _Market:
        def equity(self, _: str) -> _RecordingEquity:
            return _RecordingEquity(calls, frame)

    return lambda: _Market()


def _stock_frame(times: list[str]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "time": pd.Timestamp(moment),
                "open": 65.0,
                "high": 65.5,
                "low": 64.8,
                "close": 65.2,
                "volume": 1000,
            }
            for moment in times
        ]
    )


def test_stock_intraday_always_sends_an_explicit_count() -> None:
    """vnstock caps a response at count, which defaults to 100 and cuts the head."""

    calls: list[dict[str, object]] = []
    frame = _stock_frame(["2026-09-25T09:15:00", "2026-09-25T09:30:00"])

    extraction = extract_stock_intraday(
        symbols=["fpt"],
        partition_date="2026-09-25",
        granularity_minutes=15,
        market_factory=_market_factory(calls, frame),
    )

    assert calls[0]["count"] > 100
    assert calls[0]["interval"] == "15m"
    assert calls[0]["end"] == "2026-09-26"
    assert extraction.request["count"] == calls[0]["count"]
    assert extraction.record_count == 2
    assert extraction.records[0]["symbol"] == "FPT"
    assert extraction.records[0]["ts"] == "2026-09-25T09:15:00+07:00"


def test_stock_intraday_raises_when_the_response_hits_the_count_ceiling() -> None:
    calls: list[dict[str, object]] = []
    frame = _stock_frame(["2026-09-25T09:15:00"] * 5_000)

    with pytest.raises(SourceResponseError, match="truncated"):
        extract_stock_intraday(
            symbols=["FPT"],
            partition_date="2026-09-25",
            granularity_minutes=15,
            market_factory=_market_factory(calls, frame),
        )


def test_stock_intraday_filters_to_the_requested_sub_day_window() -> None:
    calls: list[dict[str, object]] = []
    frame = _stock_frame(
        ["2026-09-25T09:15:00", "2026-09-25T13:00:00", "2026-09-25T13:15:00"]
    )

    extraction = extract_stock_intraday(
        symbols=["FPT"],
        partition_date="2026-09-25",
        granularity_minutes=15,
        start_time="13:00",
        end_time="13:15",
        market_factory=_market_factory(calls, frame),
    )

    assert [record["trade_time"] for record in extraction.records] == ["13:00:00"]


def test_stock_intraday_flags_an_expired_partition() -> None:
    calls: list[dict[str, object]] = []

    with pytest.raises(IntradayRetentionError, match="retention window"):
        extract_stock_intraday(
            symbols=["FPT"],
            partition_date="2020-01-02",
            granularity_minutes=15,
            retention_days=30,
            market_factory=_market_factory(calls, _stock_frame([])),
        )


def test_fund_intraday_rejects_mismatched_ohlcv_arrays() -> None:
    """SSI returns OHLCV as parallel arrays; a length mismatch is not recoverable."""

    payload = {
        "code": "SUCCESS",
        "data": {
            "t": [BAR_ONE],
            "o": [],
            "h": [34.8],
            "l": [34.6],
            "c": [34.75],
            "v": [1000],
        },
    }

    with _ssi_client(payload) as client:
        with pytest.raises(SourceResponseError, match="different lengths"):
            extract_fund_intraday(
                symbol="E1VFVN30",
                partition_date="2026-09-25",
                granularity_minutes=15,
                client=client,
            )


def test_fund_intraday_rejects_a_non_success_code() -> None:
    with _ssi_client({"code": "ERROR", "data": {}}) as client:
        with pytest.raises(SourceResponseError, match="non-success code"):
            extract_fund_intraday(
                symbol="E1VFVN30",
                partition_date="2026-09-25",
                granularity_minutes=15,
                client=client,
            )
