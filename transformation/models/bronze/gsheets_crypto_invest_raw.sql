{{ config(
    materialized='table',
    schema='bronze'
) }}

SELECT
    date,
    coin,
    "usd price" AS usd_price,
    amount, 
    fee,
    "fee coin" AS fee_coin,
    "tx id" AS tx_id,
    "total amount" AS total_amount, 
    "fiat amount" AS fiat_amount, 
    "fiat price" AS fiat_price, 
    "fiat currency" AS fiat_currency, 
    "type" AS transaction_type, 
    exchange, 
    total, 
    notes
FROM 
    {{ source('landing_zone', 'crypto_invest') }}
WHERE
    coin <> 'coin'
    AND coin IS NOT NULL
    AND coin <> ''
