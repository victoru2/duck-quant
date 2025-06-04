{{ config(
    materialized='table',
    schema='gold',
    tags=['crypto_invest']
) }}

with base as (
    select
        fiat_currency,
        amount,
        fee_usd,
        fiat_amount,
        fiat_price,
        date_trunc('month', transaction_date) as transaction_month,
        case
            when transaction_type in ('p2p', 'deposit') then 'in'
            when transaction_type in ('withdrawal') then 'out'
            else 'other'
        end as direction
    from {{ ref('crypto_invest_current') }}
),

filtered as (
    select
        transaction_month,
        direction,
        amount,
        fee_usd,
        fiat_amount,
        fiat_price,
        coalesce(nullif(fiat_currency, ''), 'brl') as fiat_currency
    from base
    where direction in ('in', 'out')
)

select
    transaction_month,
    fiat_currency,
    direction,
    sum(amount) as amount,
    sum(fee_usd) as fee_usd,
    sum(fiat_amount) as total_fiat,
    mean(fiat_price) as fiat_price
from filtered
group by 1, 2, 3
order by 1, 2, 3
