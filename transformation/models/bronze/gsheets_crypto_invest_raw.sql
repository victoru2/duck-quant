{{ config(
    materialized='table',
    schema='bronze'
) }}

WITH source_values AS (
    SELECT
        _dlt_parent_id AS transaction_id,
        MAX(CASE WHEN data_values._dlt_list_idx = 0 THEN value END) AS date,
        MAX(CASE WHEN data_values._dlt_list_idx = 1 THEN value END) AS coin,
        MAX(CASE WHEN data_values._dlt_list_idx = 2 THEN value END) AS usd_price,
        MAX(CASE WHEN data_values._dlt_list_idx = 3 THEN value END) AS amount,
        MAX(CASE WHEN data_values._dlt_list_idx = 4 THEN value END) AS fee,
        MAX(CASE WHEN data_values._dlt_list_idx = 5 THEN value END) AS fee_coin,
        MAX(CASE WHEN data_values._dlt_list_idx = 6 THEN value END) AS tx_id,
        MAX(CASE WHEN data_values._dlt_list_idx = 7 THEN value END) AS total_amount,
        MAX(CASE WHEN data_values._dlt_list_idx = 8 THEN value END) AS fiat_amount,
        MAX(CASE WHEN data_values._dlt_list_idx = 9 THEN value END) AS fiat_price,
        MAX(CASE WHEN data_values._dlt_list_idx = 10 THEN value END) AS fiat_currency,
        MAX(CASE WHEN data_values._dlt_list_idx = 11 THEN value END) AS type,
        MAX(CASE WHEN data_values._dlt_list_idx = 12 THEN value END) AS exchange,
        MAX(CASE WHEN data_values._dlt_list_idx = 14 THEN value END) AS notes
    FROM 
        {{ source('landing_zone', 'load_gsheets_data__value') }} data_values
    GROUP BY 
        _dlt_parent_id
)
SELECT
    date,
    coin,
    usd_price,
    amount,
    fee,
    fee_coin,
    tx_id,
    type,
    exchange,
    notes
FROM 
    source_values
WHERE
    coin <> 'coin'
    AND coin IS NOT NULL
    AND coin <> ''
