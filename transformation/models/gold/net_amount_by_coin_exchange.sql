{{ config(
    materialized='table',
    schema='gold'
) }}

SELECT
    coin,
    exchange_name,
    SUM(net_amount) AS net_amount,
    COUNT(*) AS transaction_count
FROM 
    {{ ref('net_amount_per_transaction') }}
GROUP BY coin, exchange_name
ORDER BY net_amount DESC
