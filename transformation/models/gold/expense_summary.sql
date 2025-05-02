{{ config(
    materialized='table',
    schema='gold'
) }}

with expenses as (
    select
        expense_type,
        amount,
        period_month
    from {{ ref('gsheets_expense') }}
    where period_month <= 12
),

monthly_total as (
    select
        period_month,
        sum(amount) as total_amount
    from expenses
    group by 1
),

summary as (
    select
        e.period_month,
        e.expense_type,
        mt.total_amount,
        sum(e.amount) as total_by_category,
        round(100 * sum(e.amount) / mt.total_amount, 2) as percent_of_month
    from expenses as e
    inner join monthly_total as mt
        on e.period_month = mt.period_month
    group by 1, 2, 3
)

select * from summary
