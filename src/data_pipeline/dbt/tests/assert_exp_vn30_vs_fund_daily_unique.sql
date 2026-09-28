-- The spine must yield exactly one row per (stock_symbol, bucket_ts).
select
    stock_symbol,
    bucket_ts,
    count(*) as row_count
from {{ ref('exp_vn30_vs_fund_daily') }}
group by stock_symbol, bucket_ts
having count(*) > 1
