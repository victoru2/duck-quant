{{ config(
    materialized='table',
    schema='silver'
) }}

SELECT
    strptime(date, '%d/%m/%Y')::DATE AS transaction_date,
    TRIM(coin) AS coin,
    CASE 
        WHEN usd_price = '' OR usd_price IS NULL THEN NULL
        ELSE CAST(REPLACE(REPLACE(usd_price, '.', ''), ',', '.') AS DECIMAL(20, 8))
    END AS usd_price,
    CASE 
        WHEN amount = '' OR amount IS NULL THEN NULL
        ELSE ABS(CAST(REPLACE(REPLACE(amount, '.', ''), ',', '.') AS DECIMAL(20, 8)))
    END AS amount,
    CASE 
        WHEN fee = '' OR fee IS NULL THEN NULL
        ELSE ABS(CAST(REPLACE(REPLACE(fee, '.', ''), ',', '.') AS DECIMAL(20, 8)))
    END AS fee,
    NULLIF(TRIM(fee_coin), '') AS fee_coin,
    NULLIF(TRIM(tx_id), '') AS tx_id,
    LOWER(TRIM(transaction_type)) AS transaction_type,
    LOWER(TRIM(exchange)) AS exchange_name
FROM 
    {{ ref('gsheets_crypto_invest_raw') }}
