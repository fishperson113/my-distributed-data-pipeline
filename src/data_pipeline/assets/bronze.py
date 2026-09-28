"""Direct crawl-to-Bronze assets for market data sources.

Each asset fetches one partition day from its source and loads it straight into
Postgres Bronze as an in-memory extraction. No raw JSON file is written; the
Bronze batch is the landing layer. Use scripts/load_postgres_raw.py for manual
backfills from files under storage/raw.

A partition is still one trading day, but the grain inside it is intraday: the
assets crawl every bar of the session at the width set in `config.yml`. The
asset keys and Bronze dataset names keep their `daily` wording because they name
the partition and the source feed, not the bar width.

Intraday history is a rolling window of roughly a month on both sources, so a
partition older than `ingestion.intraday.retention_days` fails loudly rather
than landing an empty batch.
"""

import dagster as dg

from data_pipeline.config import InfrastructureSettings, load_pipeline_config
from data_pipeline.ingestion.fund import extract_fund_intraday
from data_pipeline.ingestion.stock import extract_stock_intraday
from data_pipeline.partitions import daily_market_partitions
from data_pipeline.warehouse.postgres_bronze import load_extraction_to_postgres
from data_pipeline.warehouse.settings import PostgresBronzeSettings


class IntradayWindowConfig(dg.Config):
    """Optional sub-day window applied on top of a daily partition.

    Leaving both fields empty crawls the whole partition day, which is what the
    schedule does. Supplying them narrows the crawl to one slice of the session,
    entered as exchange-local clock strings such as ``"13:00"``.
    """

    start_time: str | None = None
    end_time: str | None = None


@dg.asset(
    key=dg.AssetKey(["bronze", "stock_daily"]),
    group_name="warehouse",
    partitions_def=daily_market_partitions,
    description="Fetch every intraday vnstock bar of the partition day into Postgres Bronze.",
)
def bronze_stock_daily(
    context: dg.AssetExecutionContext, config: IntradayWindowConfig
) -> dg.MaterializeResult:
    """Crawl the selected trading date from vnstock and persist it as a Bronze batch."""

    partition_date = context.partition_key
    policy = load_pipeline_config()
    extraction = extract_stock_intraday(
        symbols=policy.stock.symbols,
        partition_date=partition_date,
        granularity_minutes=policy.intraday.granularity_minutes,
        provider=policy.stock.provider,
        start_time=config.start_time,
        end_time=config.end_time,
        retention_days=policy.intraday.retention_days,
        timezone_name=policy.daily_partition.timezone,
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
            "granularity_minutes": policy.intraday.granularity_minutes,
            "start_time": config.start_time or "session start",
            "end_time": config.end_time or "session end",
            "record_count": result.record_count,
            "batch_id": str(result.batch_id),
        }
    )


@dg.asset(
    key=dg.AssetKey(["bronze", "fund_daily"]),
    group_name="warehouse",
    partitions_def=daily_market_partitions,
    description="Fetch every intraday SSI fund bar of the partition day into Postgres Bronze.",
)
def bronze_fund_daily(
    context: dg.AssetExecutionContext, config: IntradayWindowConfig
) -> dg.MaterializeResult:
    """Crawl the selected trading date from SSI and persist it as a Bronze batch."""

    partition_date = context.partition_key
    infrastructure = InfrastructureSettings.from_env()
    policy = load_pipeline_config()
    extraction = extract_fund_intraday(
        symbol=policy.fund.symbol,
        partition_date=partition_date,
        granularity_minutes=policy.intraday.granularity_minutes,
        start_time=config.start_time,
        end_time=config.end_time,
        retention_days=policy.intraday.retention_days,
        timezone_name=policy.daily_partition.timezone,
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
            "granularity_minutes": policy.intraday.granularity_minutes,
            "start_time": config.start_time or "session start",
            "end_time": config.end_time or "session end",
            "record_count": result.record_count,
            "batch_id": str(result.batch_id),
        }
    )
