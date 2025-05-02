{{ config(
    materialized='table',
    schema='silver'
) }}

select
    expense_type,
    case
        when amount = '' or amount is null then null
        else
            ABS(
                CAST(
                    REPLACE(REPLACE(amount, '.', ''), ',', '.') as DECIMAL(
                        20, 8
                    )
                )
            )
    end as amount,
    case
        when period_month = '' or period_month is null then null
        else CAST(period_month as INT)
    end as period_month
from
    {{ ref('gsheets_expense_raw') }}
