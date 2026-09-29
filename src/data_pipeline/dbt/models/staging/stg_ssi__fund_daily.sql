-- Typed, one-row-per-bar view over the SSI iBoard Bronze raw table.
--
-- The grain is intraday: one row per (symbol, instant) at the bar width set in
-- config.yml. The model keeps its `daily` name because a partition is still one
-- trading day.
--
-- Bronze is append-only: a revised payload for a partition creates a new batch
-- rather than overwriting the old one, so the newest batch wins per instant.
with ranked as (
    select
        fund.payload ->> 'symbol' as symbol,
        to_timestamp((fund.payload ->> 'ts_epoch')::bigint) as bar_ts,
        (fund.payload ->> 'ts_epoch')::bigint as ts_epoch,
        cast(fund.payload ->> 'trade_date' as date) as trade_date,
        cast(fund.payload ->> 'granularity_minutes' as integer) as granularity_minutes,
        cast(fund.payload ->> 'open' as double precision) as open,
        cast(fund.payload ->> 'high' as double precision) as high,
        cast(fund.payload ->> 'low' as double precision) as low,
        cast(fund.payload ->> 'close' as double precision) as close,
        cast(fund.payload ->> 'volume' as bigint) as volume,
        batch.provider_name as provider,
        batch.finished_at as fetched_at,
        row_number() over (
            partition by fund.payload ->> 'symbol', (fund.payload ->> 'ts_epoch')::bigint
            order by batch.finished_at desc nulls last, batch.started_at desc
        ) as version_rank
    from {{ source('bronze', 'fund') }} as fund
    inner join bronze.ingestion_batch as batch on fund.batch_id = batch.batch_id
    where batch.status = 'completed'
      -- Rows landed before the intraday conversion have no instant in their
      -- payload and cannot be typed at this grain. Skipping them keeps one
      -- stale row from failing the not_null tests and blocking the whole
      -- build; scripts/clear_legacy_bronze.py removes them for good.
      and fund.payload ? 'ts_epoch'
)

select
    symbol,
    bar_ts,
    ts_epoch,
    trade_date,
    granularity_minutes,
    open,
    high,
    low,
    close,
    volume,
    provider,
    fetched_at
from ranked
where version_rank = 1
