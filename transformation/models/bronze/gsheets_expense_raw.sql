{{ config(
    materialized='table',
    schema='bronze'
) }}

with card_1 as (
    select
        "CARD 1" as expense_type,
        c1 as amount,
        col_2 as period_month
    from
        {{ source('landing_zone', 'expense') }}
    where
        expense_type <> 'expense type'
),

card_2 as (
    select
        "CARD 2" as expense_type,
        col_4 as amount,
        col_5 as period_month
    from
        {{ source('landing_zone', 'expense') }}
    where
        expense_type <> 'expense type'
),

card_3 as (
    select
        "CARD 3" as expense_type,
        col_7 as amount,
        col_8 as period_month
    from
        {{ source('landing_zone', 'expense') }}
    where
        expense_type <> 'expense type'
),

cash as (
    select
        cash as expense_type,
        col_10 as amount,
        col_11 as period_month
    from
        {{ source('landing_zone', 'expense') }}
    where
        expense_type <> 'expense type'
)

select * from card_1
union
select * from card_2
union
select * from card_3
union
select * from cash
