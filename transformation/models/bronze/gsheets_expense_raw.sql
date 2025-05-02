{{ config(
    materialized='table',
    schema='bronze'
) }}

WITH card_1 AS (
    SELECT
        "CARD 1" AS expense_type,
        "C1" AS amount,
        "col_2" AS month
    FROM
        {{ source('landing_zone', 'expense') }}
    WHERE
        expense_type <> 'expense type'
),

card_2 AS (
    SELECT
        "CARD 2" AS expense_type,
        "col_4" AS amount,
        "col_5" AS month
    FROM
        {{ source('landing_zone', 'expense') }}
    WHERE
        expense_type <> 'expense type'
),

card_3 AS (
    SELECT
        "CARD 3" AS expense_type,
        "col_7" AS amount,
        "col_8" AS month
    FROM
        {{ source('landing_zone', 'expense') }}
    WHERE
        expense_type <> 'expense type'
),

cash AS (
    SELECT
        "CASH" AS expense_type,
        "col_10" AS amount,
        "col_11" AS month
    FROM
        {{ source('landing_zone', 'expense') }}
    WHERE
        expense_type <> 'expense type'
)

SELECT * FROM card_1
UNION
SELECT * FROM card_2
UNION
SELECT * FROM card_3
UNION
SELECT * FROM cash
