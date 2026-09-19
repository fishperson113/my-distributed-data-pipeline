"""Daily raw landing assets for market data sources."""

import dagster as dg

from data_pipeline.config import SourceSettings
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
    settings = SourceSettings.from_env()
    extraction = extract_stock_daily(
        symbols=settings.stock_symbols,
        start=partition_date,
        end=partition_date,
        provider=settings.vnstock_provider,
    )
    output_path = write_raw_extraction(
        extraction,
        settings.raw_storage_path
        / "vnstock"
        / "stock_daily"
        / f"date={partition_date}"
        / "payload.json",
    )

    return dg.MaterializeResult(
        metadata={
            "partition_date": partition_date,
            "provider": extraction.provider,
            "symbols": list(settings.stock_symbols),
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
    settings = SourceSettings.from_env()
    extraction = extract_fund_daily(
        symbol=settings.fund_symbol,
        start=partition_date,
        end=partition_date,
        timeout_seconds=settings.http_timeout_seconds,
        max_attempts=settings.http_max_attempts,
        endpoint_url=settings.ssi_history_url,
    )
    output_path = write_raw_extraction(
        extraction,
        settings.raw_storage_path
        / "ssi"
        / "fund_daily"
        / f"date={partition_date}"
        / "payload.json",
    )

    return dg.MaterializeResult(
        metadata={
            "partition_date": partition_date,
            "provider": extraction.provider,
            "symbol": settings.fund_symbol,
            "record_count": extraction.record_count,
            "output_path": str(output_path),
        }
    )
