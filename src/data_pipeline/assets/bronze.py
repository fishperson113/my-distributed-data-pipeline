"""Dagster orchestration wrappers for Postgres Bronze raw landing."""

import json

import dagster as dg

from data_pipeline.assets.market import raw_fund_daily, raw_stock_daily
from data_pipeline.config import InfrastructureSettings
from data_pipeline.ingestion.common.models import RawExtraction
from data_pipeline.partitions import daily_market_partitions
from data_pipeline.warehouse.postgres_bronze import load_extraction_to_postgres
from data_pipeline.warehouse.settings import PostgresBronzeSettings


def _load_partition(
    *,
    context: dg.AssetExecutionContext,
    raw_source: str,
    dataset: str,
) -> dg.MaterializeResult:
    infrastructure = InfrastructureSettings.from_env()
    partition_date = context.partition_key
    raw_path = (
        infrastructure.raw_storage_path
        / raw_source
        / dataset
        / f"date={partition_date}"
        / "payload.json"
    )
    envelope = json.loads(raw_path.read_text(encoding="utf-8"))
    envelope.pop("record_count", None)
    extraction = RawExtraction(**envelope)
    result = load_extraction_to_postgres(
        extraction=extraction,
        partition_date=partition_date,
        warehouse_postgres_dsn=PostgresBronzeSettings.from_env().warehouse_postgres_dsn,
        dagster_run_id=context.run_id,
        raw_path=raw_path,
    )
    return dg.MaterializeResult(
        metadata={
            "batch_id": str(result.batch_id),
            "dataset": result.dataset,
            "record_count": result.record_count,
            "raw_path": str(raw_path),
        }
    )


@dg.asset(
    key=dg.AssetKey(["bronze", "stock_daily"]),
    group_name="warehouse",
    partitions_def=daily_market_partitions,
    deps=[raw_stock_daily],
    description="Load the daily vnstock raw payload into Postgres Bronze.",
)
def bronze_stock_daily(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Persist the stock daily raw landing file as a Postgres Bronze batch."""

    return _load_partition(context=context, raw_source="vnstock", dataset="stock_daily")


@dg.asset(
    key=dg.AssetKey(["bronze", "fund_daily"]),
    group_name="warehouse",
    partitions_def=daily_market_partitions,
    deps=[raw_fund_daily],
    description="Load the daily SSI raw payload into Postgres Bronze.",
)
def bronze_fund_daily(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Persist the fund daily raw landing file as a Postgres Bronze batch."""

    return _load_partition(context=context, raw_source="ssi", dataset="fund_daily")
