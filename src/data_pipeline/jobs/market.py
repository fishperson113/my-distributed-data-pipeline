"""Jobs for daily market source ingestion."""

import dagster as dg


daily_market_ingestion = dg.define_asset_job(
    name="daily_market_ingestion",
    selection=dg.AssetSelection.assets(
        dg.AssetKey(["raw", "stock_daily"]),
        dg.AssetKey(["raw", "fund_daily"]),
    ),
    description="Land the daily stock and fund source payloads into raw storage.",
)
