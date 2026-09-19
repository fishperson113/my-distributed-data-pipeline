"""Canonical Dagster code-location entry point."""

import dagster as dg

from data_pipeline.assets.healthcheck import deployment_healthcheck
from data_pipeline.assets.market import raw_fund_daily, raw_stock_daily
from data_pipeline.jobs.market import daily_market_ingestion
from data_pipeline.schedules.market import daily_market_ingestion_schedule


defs = dg.Definitions(
    assets=[deployment_healthcheck, raw_stock_daily, raw_fund_daily],
    jobs=[daily_market_ingestion],
    schedules=[daily_market_ingestion_schedule],
)
