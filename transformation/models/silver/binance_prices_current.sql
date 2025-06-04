{{
  config(
    materialized='table',
    schema='silver',
    tags=['crypto_invest', 'binance_api']
  )
}}

select
    symbol,
    price_usd
from {{ ref('binance_prices') }}
qualify row_number() over (
    partition by symbol
    order by updated_at desc
) = 1
