with ranked as (
    select
        stock.payload ->> 'symbol' as symbol,
        cast(stock.payload ->> 'time' as timestamptz)::date as trade_date,
        cast(stock.payload ->> 'open' as double precision) as open,
        cast(stock.payload ->> 'high' as double precision) as high,
        cast(stock.payload ->> 'low' as double precision) as low,
        cast(stock.payload ->> 'close' as double precision) as close,
        cast(stock.payload ->> 'volume' as bigint) as volume,
        batch.provider_name as provider,
        batch.finished_at as fetched_at,
        row_number() over (
            partition by stock.payload ->> 'symbol', cast(stock.payload ->> 'time' as timestamptz)::date
            order by batch.finished_at desc nulls last, batch.started_at desc
        ) as version_rank
    from {{ source('bronze', 'stock') }} as stock
    inner join bronze.ingestion_batch as batch on stock.batch_id = batch.batch_id
    where batch.status = 'succeeded'
)

select
    symbol,
    trade_date,
    open,
    high,
    low,
    close,
    volume,
    provider,
    fetched_at
from ranked
where version_rank = 1
