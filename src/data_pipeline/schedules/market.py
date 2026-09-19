"""Schedules for daily market ingestion."""

import dagster as dg

from data_pipeline.config import load_pipeline_config
from data_pipeline.jobs.market import daily_market_ingestion


_schedule_config = load_pipeline_config().daily_schedule
daily_market_ingestion_schedule = dg.build_schedule_from_partitioned_job(
    daily_market_ingestion,
    name="daily_market_ingestion_schedule",
    hour_of_day=_schedule_config.hour,
    minute_of_hour=_schedule_config.minute,
    default_status=(
        dg.DefaultScheduleStatus.RUNNING
        if _schedule_config.enabled_by_default
        else dg.DefaultScheduleStatus.STOPPED
    ),
    description="Runs the previous daily market partition using config.yml policy.",
)
