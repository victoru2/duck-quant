{{ config(
    materialized='table',
    schema='silver'
) }}

SELECT
    expense_type,
    CASE
        WHEN amount = '' OR amount IS NULL THEN NULL
        ELSE ABS(CAST(REPLACE(REPLACE(amount, '.', ''), ',', '.') AS DECIMAL(20, 8)))
    END AS amount,
    CASE
        WHEN month = '' OR month IS NULL THEN NULL
        ELSE month::INT
    END AS month
FROM
    {{ ref('gsheets_expense_raw') }}
