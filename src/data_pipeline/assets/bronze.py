"""Direct crawl-to-Bronze assets for market data sources.

Each asset fetches one daily partition from its source and loads it straight
into Postgres Bronze as an in-memory extraction. No raw JSON file is written;
the Bronze batch is the landing layer. Use scripts/load_postgres_raw.py for
manual backfills from files under storage/raw.
"""

import dagster as dg

from data_pipeline.config import InfrastructureSettings, load_pipeline_config
from data_pipeline.ingestion.fund import extract_fund_daily
from data_pipeline.ingestion.stock import extract_stock_daily
from data_pipeline.partitions import daily_market_partitions
from data_pipeline.warehouse.postgres_bronze import load_extraction_to_postgres
from data_pipeline.warehouse.settings import PostgresBronzeSettings


@dg.asset(
    key=dg.AssetKey(["bronze", "stock_daily"]),
    group_name="warehouse",
    partitions_def=daily_market_partitions,
    description="Fetch the daily vnstock payload and load it straight into Postgres Bronze.",
)
def bronze_stock_daily(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Crawl the selected trading date from vnstock and persist it as a Bronze batch."""

    partition_date = context.partition_key
    policy = load_pipeline_config()
    extraction = extract_stock_daily(
        symbols=policy.stock.symbols,
        start=partition_date,
        end=partition_date,
        provider=policy.stock.provider,
    )
    result = load_extraction_to_postgres(
        extraction=extraction,
        partition_date=partition_date,
        warehouse_postgres_dsn=PostgresBronzeSettings.from_env().warehouse_postgres_dsn,
        dagster_run_id=context.run_id,
    )

    return dg.MaterializeResult(
        metadata={
            "partition_date": partition_date,
            "provider": extraction.provider,
            "symbols": list(policy.stock.symbols),
            "record_count": result.record_count,
            "batch_id": str(result.batch_id),
        }
    )


@dg.asset(
    key=dg.AssetKey(["bronze", "fund_daily"]),
    group_name="warehouse",
    partitions_def=daily_market_partitions,
    description="Fetch the daily SSI fund payload and load it straight into Postgres Bronze.",
)
def bronze_fund_daily(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Crawl the selected trading date from SSI and persist it as a Bronze batch."""

    partition_date = context.partition_key
    infrastructure = InfrastructureSettings.from_env()
    policy = load_pipeline_config()
    extraction = extract_fund_daily(
        symbol=policy.fund.symbol,
        start=partition_date,
        end=partition_date,
        timeout_seconds=infrastructure.http_timeout_seconds,
        max_attempts=infrastructure.http_max_attempts,
        endpoint_url=infrastructure.ssi_history_url,
    )
    result = load_extraction_to_postgres(
        extraction=extraction,
        partition_date=partition_date,
        warehouse_postgres_dsn=PostgresBronzeSettings.from_env().warehouse_postgres_dsn,
        dagster_run_id=context.run_id,
    )

    return dg.MaterializeResult(
        metadata={
            "partition_date": partition_date,
            "provider": extraction.provider,
            "symbol": policy.fund.symbol,
            "record_count": result.record_count,
            "batch_id": str(result.batch_id),
        }
    )
