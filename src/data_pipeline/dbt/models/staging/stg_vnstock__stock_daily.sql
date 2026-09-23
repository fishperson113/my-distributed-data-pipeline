select
    payload ->> 'symbol' as symbol,
    cast(payload ->> 'time' as date) as trade_date,
    cast(payload ->> 'open' as double) as open,
    cast(payload ->> 'high' as double) as high,
    cast(payload ->> 'low' as double) as low,
    cast(payload ->> 'close' as double) as close,
    cast(payload ->> 'volume' as bigint) as volume,
    payload ->> 'provider' as provider,
    cast(payload ->> 'fetched_at' as timestamp) as fetched_at
from {{ source('raw', 'stock_daily') }}
