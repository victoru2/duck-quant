{{ config(
    materialized='table',
    schema='gold'
) }}

WITH expenses AS (
    SELECT
        expense_type,
        amount,
        month
    FROM {{ ref('gsheets_expense') }}
    WHERE month <= 12
),

monthly_total AS (
    SELECT
        month,
        SUM(amount) AS total_amount
    FROM expenses
    GROUP BY month
),

summary AS (
    SELECT
        e.month,
        e.expense_type,
        SUM(e.amount) AS total_by_category,
        mt.total_amount,
        ROUND(100 * SUM(e.amount) / mt.total_amount, 2) AS percent_of_month
    FROM expenses e
    JOIN monthly_total mt
        ON e.month = mt.month
    GROUP BY e.month, e.expense_type, mt.total_amount
)

SELECT * FROM summary
ORDER BY month, percent_of_month DESC
