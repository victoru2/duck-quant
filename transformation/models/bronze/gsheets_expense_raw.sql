{{ config(
    materialized='table',
    schema='bronze',
    tags=['expense']
) }}

select
    payment_method,
    "expense type" as expense_type,
    amount,
    month as period_month
from
    {{ source('landing_zone', 'expense') }}
