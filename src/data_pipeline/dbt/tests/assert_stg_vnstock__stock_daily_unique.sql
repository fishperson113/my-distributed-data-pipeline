-- Idempotency after transform: staging must hold one row per (symbol, instant).
-- Fails (returns rows) if de-duplication by version_rank ever leaks duplicates.
select
    symbol,
    ts_epoch,
    count(*) as row_count
from {{ ref('stg_vnstock__stock_daily') }}
group by symbol, ts_epoch
having count(*) > 1
