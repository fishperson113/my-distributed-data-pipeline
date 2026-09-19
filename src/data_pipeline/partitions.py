"""Shared partition definitions for market ingestion."""

import dagster as dg


MARKET_TIMEZONE = "Asia/Ho_Chi_Minh"

daily_market_partitions = dg.DailyPartitionsDefinition(
    start_date="2020-01-01",
    timezone=MARKET_TIMEZONE,
)
