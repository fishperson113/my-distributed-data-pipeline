"""Shared partition definitions for market ingestion."""

import dagster as dg

from data_pipeline.config import load_pipeline_config


_pipeline_config = load_pipeline_config()
MARKET_TIMEZONE = _pipeline_config.daily_partition.timezone

daily_market_partitions = dg.DailyPartitionsDefinition(
    start_date=_pipeline_config.daily_partition.start_date,
    timezone=MARKET_TIMEZONE,
)
