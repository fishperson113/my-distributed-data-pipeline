-- Idempotency after transform: staging must hold one row per (symbol, trade_date).
-- Fails (returns rows) if de-duplication by version_rank ever leaks duplicates.
select
    symbol,
    trade_date,
    count(*) as row_count
from {{ ref('stg_vnstock__stock_daily') }}
group by symbol, trade_date
having count(*) > 1
