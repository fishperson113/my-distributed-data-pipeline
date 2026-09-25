with ranked as (
    select
        fund.payload ->> 'symbol' as symbol,
        cast(fund.payload ->> 'trade_date' as date) as trade_date,
        cast(fund.payload ->> 'open' as double precision) as open,
        cast(fund.payload ->> 'high' as double precision) as high,
        cast(fund.payload ->> 'low' as double precision) as low,
        cast(fund.payload ->> 'close' as double precision) as close,
        cast(fund.payload ->> 'volume' as bigint) as volume,
        batch.provider_name as provider,
        batch.finished_at as fetched_at,
        row_number() over (
            partition by fund.payload ->> 'symbol', cast(fund.payload ->> 'trade_date' as date)
            order by batch.finished_at desc nulls last, batch.started_at desc
        ) as version_rank
    from {{ source('bronze', 'fund') }} as fund
    inner join bronze.ingestion_batch as batch on fund.batch_id = batch.batch_id
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
