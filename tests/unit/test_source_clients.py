"""Unit tests for source adapters without live network calls."""

from __future__ import annotations

import httpx
import pandas as pd
import pytest

from data_pipeline.ingestion.fund.client import SourceResponseError, extract_fund_daily
from data_pipeline.ingestion.stock.client import extract_stock_daily


class _FakeEquity:
    def __init__(self, symbol: str) -> None:
        self.symbol = symbol

    def ohlcv(self, **_: object) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "time": pd.Timestamp("2026-09-18"),
                    "open": 100.0,
                    "high": 105.0,
                    "low": 99.0,
                    "close": 103.0,
                    "volume": 1_000,
                }
            ]
        )


class _FakeMarket:
    def equity(self, symbol: str) -> _FakeEquity:
        return _FakeEquity(symbol)


def test_stock_extraction_produces_json_safe_records() -> None:
    extraction = extract_stock_daily(
        symbols=["fpt"],
        start="2026-09-01",
        end="2026-09-19",
        market_factory=_FakeMarket,
    )

    assert extraction.provider == "kbs"
    assert extraction.record_count == 1
    assert extraction.records[0]["symbol"] == "FPT"
    assert extraction.records[0]["time"].startswith("2026-09-18")


def test_fund_extraction_preserves_payload_and_builds_records() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.params["symbol"] == "E1VFVN30"
        return httpx.Response(
            200,
            json={
                "code": "SUCCESS",
                "message": "ok",
                "data": {
                    "t": [1789689600],
                    "o": [35.4],
                    "h": [35.74],
                    "l": [35.4],
                    "c": [35.5],
                    "v": [542500],
                    "s": "ok",
                    "nextTime": None,
                },
            },
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        extraction = extract_fund_daily(
            symbol="E1VFVN30",
            start="2026-09-01",
            end="2026-09-19",
            client=client,
        )

    assert extraction.record_count == 1
    assert extraction.records[0]["trade_date"] == "2026-09-18"
    assert extraction.records[0]["close"] == 35.5
    assert extraction.source_payload is not None


def test_fund_extraction_rejects_mismatched_arrays() -> None:
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "code": "SUCCESS",
                "data": {
                    "t": [1789689600],
                    "o": [],
                    "h": [35.74],
                    "l": [35.4],
                    "c": [35.5],
                    "v": [542500],
                },
            },
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(SourceResponseError, match="different lengths"):
            extract_fund_daily(
                symbol="E1VFVN30",
                start="2026-09-01",
                end="2026-09-19",
                client=client,
            )

