"""Shared intraday time handling for both market sources.

The two sources disagree about how they express a bar's instant: SSI returns a
Unix epoch in UTC, vnstock returns a naive local-time string with no offset.
Both describe the same instants -- a 15-minute probe of FPT returned an
identical 09:15..14:45 sequence from each -- so everything downstream is put on
one axis here: an aware Asia/Ho_Chi_Minh datetime, carried alongside its epoch.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from typing import Any
from zoneinfo import ZoneInfo

MARKET_TIMEZONE = "Asia/Ho_Chi_Minh"
MARKET_TZ = ZoneInfo(MARKET_TIMEZONE)


def market_tz(timezone_name: str = MARKET_TIMEZONE) -> ZoneInfo:
    """Return the exchange timezone used to anchor every intraday bar."""

    return ZoneInfo(timezone_name)


def _parse_clock(value: str, field: str) -> time:
    try:
        return time.fromisoformat(value)
    except ValueError as error:
        raise ValueError(f"{field} must be an ISO time such as '09:15'.") from error


def resolve_window(
    *,
    partition_date: str,
    start_time: str | None = None,
    end_time: str | None = None,
    timezone_name: str = MARKET_TIMEZONE,
) -> tuple[datetime, datetime]:
    """Resolve one partition day into a half-open ``[start, end)`` local window.

    ``start_time`` and ``end_time`` are optional clock strings that narrow the
    window inside the day, which is what makes a sub-day crawl expressible.
    Omitting both covers the whole calendar day.
    """

    tz = market_tz(timezone_name)
    day = date.fromisoformat(partition_date)

    start_clock = _parse_clock(start_time, "start_time") if start_time else time.min
    window_start = datetime.combine(day, start_clock, tzinfo=tz)

    if end_time:
        window_end = datetime.combine(day, _parse_clock(end_time, "end_time"), tzinfo=tz)
    else:
        window_end = datetime.combine(day + timedelta(days=1), time.min, tzinfo=tz)

    if window_end <= window_start:
        raise ValueError("end_time must be later than start_time.")
    return window_start, window_end


def to_epoch(moment: datetime) -> int:
    """Return the Unix second for an aware datetime."""

    if moment.tzinfo is None:
        raise ValueError("Only timezone-aware datetimes can be converted to epoch.")
    return int(moment.timestamp())


def from_epoch(epoch: int, timezone_name: str = MARKET_TIMEZONE) -> datetime:
    """Read an SSI Unix second as an aware exchange-local datetime."""

    return datetime.fromtimestamp(int(epoch), tz=market_tz(timezone_name))


def from_naive_local(value: str, timezone_name: str = MARKET_TIMEZONE) -> datetime:
    """Read a vnstock naive local timestamp string as an aware datetime."""

    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=market_tz(timezone_name))
    return parsed.astimezone(market_tz(timezone_name))


def bar_timestamp_fields(moment: datetime) -> dict[str, Any]:
    """Render the canonical timestamp columns attached to every intraday bar."""

    return {
        "ts": moment.isoformat(),
        "ts_epoch": to_epoch(moment),
        "trade_date": moment.date().isoformat(),
        "trade_time": moment.strftime("%H:%M:%S"),
    }


def within_window(moment: datetime, start: datetime, end: datetime) -> bool:
    """Keep bars inside the half-open window the caller asked for."""

    return start <= moment < end


def is_outside_retention(
    *,
    window_start: datetime,
    retention_days: int,
    now: datetime | None = None,
) -> bool:
    """Report whether a window predates the rolling intraday retention.

    Both sources answer a too-old request with an empty but successful payload,
    which is indistinguishable from a public holiday. Callers use this to turn
    that ambiguity into a loud failure instead of an empty Bronze batch.
    """

    reference = now or datetime.now(tz=window_start.tzinfo or MARKET_TZ)
    return window_start < reference - timedelta(days=retention_days)
