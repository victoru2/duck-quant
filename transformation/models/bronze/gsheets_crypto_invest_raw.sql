{{ config(
    materialized='table',
    schema='bronze',
    tags=['crypto_invest']
) }}

select
    date,
    coin,
    "usd price" as usd_price,
    amount,
    fee,
    "fee coin" as fee_coin,
    "tx id" as tx_id,
    "total amount" as total_amount,
    "fiat amount" as fiat_amount,
    "fiat price" as fiat_price,
    "fiat currency" as fiat_currency,
    type as transaction_type,
    exchange,
    total,
    notes
from
    {{ source('landing_zone', 'crypto_invest') }}
where
    coin <> 'coin'
    and coin is not null
    and coin <> ''
