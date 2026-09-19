"""Schedules for daily market ingestion."""

import dagster as dg

from data_pipeline.jobs.market import daily_market_ingestion
daily_market_ingestion_schedule = dg.build_schedule_from_partitioned_job(
    daily_market_ingestion,
    name="daily_market_ingestion_schedule",
    hour_of_day=6,
    minute_of_hour=0,
    default_status=dg.DefaultScheduleStatus.STOPPED,
    description="Runs the previous daily partition at 06:00 Vietnam time (UTC+7).",
)
