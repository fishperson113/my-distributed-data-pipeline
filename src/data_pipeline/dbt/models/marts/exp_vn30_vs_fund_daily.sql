-- Intraday comparison of each stock against the E1VFVN30 fund on a shared grid.
--
-- The two sources do not produce the same bars. A source only emits a bar for
-- an interval that actually traded, and the fund is thinner than the stocks, so
-- an inner join on the instant would silently drop rows. Instead a spine of
-- every grid bucket seen on either side is built, both sides are left joined
-- onto it, and the last known close is carried forward within the trading day.
-- `*_is_actual` marks whether a close was observed or carried, so a consumer
-- can always tell a real print from a filled one.
--
-- Buckets are cut on epoch multiples. Every supported bar width (1, 5, 15, 30,
-- 60) divides the +07:00 exchange offset evenly, so the epoch grid and the
-- exchange-local grid land on the same boundaries. Snapping is done here rather
-- than assumed, which keeps the model correct if the bar width changes or a
-- source ever emits an off-grid stamp.
with stock_grid as (
    select
        symbol,
        trade_date,
        to_timestamp(
            floor(ts_epoch::numeric / (granularity_minutes * 60)) * (granularity_minutes * 60)
        ) as bucket_ts,
        (array_agg(open order by bar_ts))[1] as open,
        max(high) as high,
        min(low) as low,
        (array_agg(close order by bar_ts desc))[1] as close,
        sum(volume) as volume
    from {{ ref('stg_vnstock__stock_daily') }}
    group by
        symbol,
        trade_date,
        to_timestamp(
            floor(ts_epoch::numeric / (granularity_minutes * 60)) * (granularity_minutes * 60)
        )
),

fund_grid as (
    select
        symbol,
        trade_date,
        to_timestamp(
            floor(ts_epoch::numeric / (granularity_minutes * 60)) * (granularity_minutes * 60)
        ) as bucket_ts,
        (array_agg(close order by bar_ts desc))[1] as close,
        sum(volume) as volume
    from {{ ref('stg_ssi__fund_daily') }}
    group by
        symbol,
        trade_date,
        to_timestamp(
            floor(ts_epoch::numeric / (granularity_minutes * 60)) * (granularity_minutes * 60)
        )
),

spine as (
    select trade_date, bucket_ts from stock_grid
    union
    select trade_date, bucket_ts from fund_grid
),

scaffold as (
    select
        spine.trade_date,
        spine.bucket_ts,
        symbols.symbol as stock_symbol
    from spine
    cross join (select distinct symbol from stock_grid) as symbols
),

joined as (
    select
        scaffold.trade_date,
        scaffold.bucket_ts,
        scaffold.stock_symbol,
        stock.open as stock_open,
        stock.high as stock_high,
        stock.low as stock_low,
        stock.close as stock_close,
        stock.volume as stock_volume,
        fund.symbol as fund_symbol,
        fund.close as fund_close,
        fund.volume as fund_volume,
        -- Running count of non-null closes: it only advances on an observed
        -- bar, so all carried rows share the group of the print before them.
        count(stock.close) over (
            partition by scaffold.stock_symbol, scaffold.trade_date
            order by scaffold.bucket_ts
            rows between unbounded preceding and current row
        ) as stock_fill_group,
        count(fund.close) over (
            partition by scaffold.stock_symbol, scaffold.trade_date
            order by scaffold.bucket_ts
            rows between unbounded preceding and current row
        ) as fund_fill_group
    from scaffold
    left join stock_grid as stock
        on stock.symbol = scaffold.stock_symbol
        and stock.bucket_ts = scaffold.bucket_ts
    left join fund_grid as fund
        on fund.bucket_ts = scaffold.bucket_ts
),

filled as (
    select
        trade_date,
        bucket_ts,
        stock_symbol,
        stock_open,
        stock_high,
        stock_low,
        stock_close,
        stock_volume,
        fund_symbol,
        fund_close,
        fund_volume,
        first_value(stock_close) over (
            partition by stock_symbol, trade_date, stock_fill_group
            order by bucket_ts
        ) as stock_close_filled,
        first_value(fund_close) over (
            partition by stock_symbol, trade_date, fund_fill_group
            order by bucket_ts
        ) as fund_close_filled
    from joined
)

select
    trade_date,
    bucket_ts,
    -- bucket_ts is a timestamptz, so it renders in whatever the session
    -- timezone happens to be. The exchange-local clock is what a reader of a
    -- VN market mart expects, so it is spelled out rather than inherited.
    bucket_ts at time zone '{{ var("market_timezone") }}' as bucket_ts_local,
    to_char(bucket_ts at time zone '{{ var("market_timezone") }}', 'HH24:MI') as bucket_time,
    stock_symbol,
    stock_open,
    stock_high,
    stock_low,
    stock_close,
    stock_close_filled,
    stock_volume,
    stock_close is not null as stock_is_actual,
    max(fund_symbol) over () as fund_symbol,
    fund_close,
    fund_close_filled,
    fund_volume,
    fund_close is not null as fund_is_actual,
    case
        when fund_close_filled is null or fund_close_filled = 0 then null
        else stock_close_filled / fund_close_filled
    end as stock_to_fund_ratio
from filled
