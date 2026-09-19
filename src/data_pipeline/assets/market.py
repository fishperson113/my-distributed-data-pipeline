"""Daily raw landing assets for market data sources."""

import dagster as dg

from data_pipeline.config import InfrastructureSettings, load_pipeline_config
from data_pipeline.ingestion.common.raw_output import write_raw_extraction
from data_pipeline.ingestion.fund import extract_fund_daily
from data_pipeline.ingestion.stock import extract_stock_daily
from data_pipeline.partitions import daily_market_partitions


@dg.asset(
    key=dg.AssetKey(["raw", "stock_daily"]),
    group_name="market_ingestion",
    partitions_def=daily_market_partitions,
    description="Daily stock source payload landed from vnstock as raw JSON.",
)
def raw_stock_daily(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Fetch the selected trading date from vnstock and persist its raw envelope."""

    partition_date = context.partition_key
    infrastructure = InfrastructureSettings.from_env()
    policy = load_pipeline_config()
    extraction = extract_stock_daily(
        symbols=policy.stock.symbols,
        start=partition_date,
        end=partition_date,
        provider=policy.stock.provider,
    )
    output_path = write_raw_extraction(
        extraction,
        infrastructure.raw_storage_path
        / "vnstock"
        / "stock_daily"
        / f"date={partition_date}"
        / "payload.json",
    )

    return dg.MaterializeResult(
        metadata={
            "partition_date": partition_date,
            "provider": extraction.provider,
            "symbols": list(policy.stock.symbols),
            "record_count": extraction.record_count,
            "output_path": str(output_path),
        }
    )


@dg.asset(
    key=dg.AssetKey(["raw", "fund_daily"]),
    group_name="market_ingestion",
    partitions_def=daily_market_partitions,
    description="Daily fund/ETF source payload landed from SSI as raw JSON.",
)
def raw_fund_daily(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Fetch the selected trading date from SSI and persist its raw envelope."""

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
    output_path = write_raw_extraction(
        extraction,
        infrastructure.raw_storage_path
        / "ssi"
        / "fund_daily"
        / f"date={partition_date}"
        / "payload.json",
    )

    return dg.MaterializeResult(
        metadata={
            "partition_date": partition_date,
            "provider": extraction.provider,
            "symbol": policy.fund.symbol,
            "record_count": extraction.record_count,
            "output_path": str(output_path),
        }
    )
