{{ config(
    materialized='table',
    schema='silver',
    tags=['expense']
) }}

select
    expense_type,
    case
        when amount = '' or amount is null then null
        else
            abs(
                cast(
                    replace(replace(amount, '.', ''), ',', '.') as DECIMAL(
                        20, 8
                    )
                )
            )
    end as amount,
    case
        when period_month = '' or period_month is null then null
        else cast(period_month as INT)
    end as period_month
from
    {{ ref('gsheets_expense_raw') }}
