{{ config(
    materialized='table',
    schema='gold'
) }}

SELECT
    transaction_date,
    coin,
    usd_price,
    fee,
    CASE 
        WHEN transaction_type IN ('sell', 'withdrawal', 'spot to earn') THEN amount*-1 ELSE amount
    END AS trade_amount,
    CASE 
        WHEN transaction_type IN ('sell', 'withdrawal', 'spot to earn') THEN (amount+coalesce(fee,0))*-1 ELSE amount-coalesce(fee,0)
    END AS net_amount,
    transaction_type,
    exchange_name
FROM 
    {{ ref('gsheets_crypto_invest') }}
