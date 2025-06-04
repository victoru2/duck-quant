{{
  config(
    materialized='table',
    schema='gold',
    tags=['crypto_invest', 'portfolio']
  )
}}

with
prices as (
    select
        symbol,
        price_usd as current_price_usd
    from {{ ref('binance_prices_current') }}
),

cost_basis as (
    select
        coin,
        sum(net_amount) as total_amount,
        sum(net_amount * usd_price) as total_cost,
        min(usd_price) as min_entry_price,
        max(usd_price) as max_entry_price,
        avg(usd_price) as avg_entry_price
    from {{ ref('crypto_invest_current') }}
    where
        transaction_type = 'buy'
        and (notes is null or lower(notes) != 'closed')
        and not is_stablecoin
    group by 1
)

select
    cb.coin,
    cb.total_amount,
    p.current_price_usd,
    cb.min_entry_price,
    cb.max_entry_price,
    cb.avg_entry_price,
    cb.total_amount * p.current_price_usd as total_amount_usd,
    coalesce(cb.total_cost, 0) as total_cost,
    total_amount_usd - coalesce(cb.total_cost, 0) as pnl,
    case
        when cb.total_cost = 0 then null
        else (total_amount_usd - cb.total_cost) / cb.total_cost * 100
    end as roi_pct
from cost_basis as cb
left join prices as p
    on lower(cb.coin) = lower(p.symbol)
