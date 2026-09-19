"""Tests for the Dagster code-location boundary."""

from datetime import datetime
from zoneinfo import ZoneInfo

import dagster as dg

from data_pipeline.assets.healthcheck import deployment_healthcheck
from data_pipeline.definitions import defs


def test_definitions_are_loadable() -> None:
    """Validate that Dagster can load the repository definitions."""

    dg.Definitions.validate_loadable(defs)


def test_healthcheck_asset_is_registered() -> None:
    """Ensure the deployment healthcheck is visible to Dagster."""

    asset_keys = defs.resolve_asset_graph().get_all_asset_keys()
    assert dg.AssetKey("deployment_healthcheck") in asset_keys


def test_daily_market_assets_job_and_schedule_are_registered() -> None:
    """Ensure the partitioned ingestion topology is visible to Dagster."""

    asset_keys = defs.resolve_asset_graph().get_all_asset_keys()
    assert dg.AssetKey(["raw", "stock_daily"]) in asset_keys
    assert dg.AssetKey(["raw", "fund_daily"]) in asset_keys

    job = defs.resolve_job_def("daily_market_ingestion")
    assert job.partitions_def is not None

    schedule = defs.resolve_schedule_def("daily_market_ingestion_schedule")
    assert schedule.cron_schedule == "0 6 * * *"
    assert schedule.execution_timezone == "Asia/Ho_Chi_Minh"

    tick = schedule.evaluate_tick(
        dg.build_schedule_context(
            scheduled_execution_time=datetime(
                2026, 9, 20, 6, 0, tzinfo=ZoneInfo("Asia/Ho_Chi_Minh")
            )
        )
    )
    assert tick.run_requests[0].partition_key == "2026-09-19"


def test_healthcheck_asset_materializes_locally() -> None:
    """Exercise the asset without external services."""

    result = dg.materialize([deployment_healthcheck])
    assert result.success
