"""Canonical Dagster code-location entry point."""

import dagster as dg
from dagster_dbt import DbtCliResource

from data_pipeline.assets.bronze import bronze_fund_daily, bronze_stock_daily
from data_pipeline.assets.checks import bronze_asset_checks
from data_pipeline.assets.dbt import DBT_PROJECT_DIR, dbt_market_models
from data_pipeline.assets.healthcheck import deployment_healthcheck
from data_pipeline.jobs.market import daily_market_ingestion
from data_pipeline.schedules.market import daily_market_ingestion_schedule


defs = dg.Definitions(
    assets=[
        deployment_healthcheck,
        bronze_stock_daily,
        bronze_fund_daily,
        dbt_market_models,
    ],
    asset_checks=bronze_asset_checks,
    jobs=[daily_market_ingestion],
    schedules=[daily_market_ingestion_schedule],
    resources={
        "dbt": DbtCliResource(project_dir=DBT_PROJECT_DIR),
    },
)
