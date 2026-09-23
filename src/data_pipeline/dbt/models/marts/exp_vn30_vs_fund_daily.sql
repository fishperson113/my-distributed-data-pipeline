select
    stock.trade_date,
    stock.symbol as stock_symbol,
    stock.close as stock_close,
    stock.volume as stock_volume,
    fund.symbol as fund_symbol,
    fund.close as fund_close,
    fund.volume as fund_volume
from {{ ref('stg_vnstock__stock_daily') }} as stock
inner join {{ ref('stg_ssi__fund_daily') }} as fund
    on stock.trade_date = fund.trade_date
