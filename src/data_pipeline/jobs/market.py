"""Jobs for daily market source ingestion."""

import dagster as dg


daily_market_ingestion = dg.define_asset_job(
    name="daily_market_ingestion",
    selection=dg.AssetSelection.assets(
        dg.AssetKey(["bronze", "stock_daily"]),
        dg.AssetKey(["bronze", "fund_daily"]),
    ).downstream(),
    description="Crawl daily source payloads into Postgres Bronze, then run dbt.",
)
