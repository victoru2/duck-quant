{{ config(
    materialized='table',
    schema='gold',
    tags=['crypto_invest']
) }}

select
    transaction_date,
    coin,
    usd_price,
    fee,
    transaction_type,
    exchange_name,
    is_stablecoin,
    trade_amount,
    net_amount
from
    {{ ref('crypto_invest_current') }}
