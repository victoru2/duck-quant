{{
  config(
    materialized='table',
    schema='silver',
    tags=['crypto_invest']
  )
}}

with
latest_prices as (
    select
        symbol,
        price_usd
    from {{ ref('binance_prices_current') }}
),

transactions_with_fees as (
    select
        transaction_date,
        coin,
        usd_price,
        amount,
        fee,
        fee_coin,
        tx_id,
        fiat_amount,
        fiat_price,
        fiat_currency,
        transaction_type,
        exchange_name,
        notes,
        is_stablecoin
    from {{ ref('gsheets_crypto_invest') }}
)

select
    t.transaction_date,
    t.coin,
    t.usd_price,
    t.amount,
    t.fee,
    t.fee_coin,
    t.tx_id,
    t.fiat_amount,
    t.fiat_price,
    t.fiat_currency,
    t.transaction_type,
    t.exchange_name,
    t.notes,
    t.is_stablecoin,
    case
        when
            t.transaction_type in ('sell', 'withdrawal', 'spot to earn')
            then t.amount * -1
        else t.amount
    end as trade_amount,
    case
        when
            t.transaction_type in ('sell', 'withdrawal', 'spot to earn')
            then (t.amount + coalesce(t.fee, 0)) * -1 else
            t.amount - coalesce(t.fee, 0)
    end as net_amount,
    case
        when t.is_stablecoin then t.fee
        else t.fee * coalesce(p.price_usd, 0)
    end as fee_usd,
    current_timestamp as calculated_at
from transactions_with_fees as t
left join latest_prices as p
    on lower(t.fee_coin) = p.symbol
