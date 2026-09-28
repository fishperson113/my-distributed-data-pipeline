"""Unit tests for the shared intraday time axis."""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from data_pipeline.ingestion.common.timeframe import (
    MARKET_TZ,
    bar_timestamp_fields,
    from_epoch,
    from_naive_local,
    is_outside_retention,
    resolve_window,
    to_epoch,
    within_window,
)

# 2026-09-25 09:15 exchange time, the first bar of that session.
FIRST_BAR_EPOCH = 1790302500


def test_both_source_formats_resolve_to_the_same_instant() -> None:
    from_ssi = from_epoch(FIRST_BAR_EPOCH)
    from_vnstock = from_naive_local("2026-09-25T09:15:00.000")

    assert from_ssi == from_vnstock
    assert from_ssi.strftime("%H:%M") == "09:15"


def test_window_defaults_to_the_whole_partition_day() -> None:
    start, end = resolve_window(partition_date="2026-09-25")

    assert start.isoformat() == "2026-09-25T00:00:00+07:00"
    assert end.isoformat() == "2026-09-26T00:00:00+07:00"


def test_window_narrows_to_a_sub_day_slice() -> None:
    start, end = resolve_window(
        partition_date="2026-09-25", start_time="10:00", end_time="11:00"
    )

    assert to_epoch(start) == 1790305200
    assert to_epoch(end) == 1790308800
    assert within_window(from_epoch(1790306100), start, end)
    assert not within_window(from_epoch(FIRST_BAR_EPOCH), start, end)


def test_window_rejects_an_end_at_or_before_the_start() -> None:
    with pytest.raises(ValueError, match="end_time must be later"):
        resolve_window(partition_date="2026-09-25", start_time="11:00", end_time="10:00")


def test_window_rejects_a_malformed_clock() -> None:
    with pytest.raises(ValueError, match="start_time must be an ISO time"):
        resolve_window(partition_date="2026-09-25", start_time="9am")


def test_timestamp_fields_carry_both_the_instant_and_its_local_reading() -> None:
    fields = bar_timestamp_fields(from_epoch(FIRST_BAR_EPOCH))

    assert fields == {
        "ts": "2026-09-25T09:15:00+07:00",
        "ts_epoch": FIRST_BAR_EPOCH,
        "trade_date": "2026-09-25",
        "trade_time": "09:15:00",
    }


def test_retention_check_separates_a_recent_day_from_an_expired_one() -> None:
    now = datetime(2026, 9, 26, tzinfo=MARKET_TZ)
    recent, _ = resolve_window(partition_date="2026-09-20")
    expired, _ = resolve_window(partition_date="2026-06-20")

    assert not is_outside_retention(window_start=recent, retention_days=30, now=now)
    assert is_outside_retention(window_start=expired, retention_days=30, now=now)


def test_retention_boundary_is_exclusive_at_exactly_the_cutoff() -> None:
    now = datetime(2026, 9, 26, 12, 0, tzinfo=MARKET_TZ)
    start = now - timedelta(days=30)

    assert not is_outside_retention(window_start=start, retention_days=30, now=now)
    assert is_outside_retention(
        window_start=start - timedelta(seconds=1), retention_days=30, now=now
    )


def test_epoch_conversion_rejects_a_naive_datetime() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        to_epoch(datetime(2026, 9, 25, 9, 15))
