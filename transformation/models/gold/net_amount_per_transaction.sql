{{ config(
    materialized='table',
    schema='gold'
) }}

{% set stablecoins = ('usdt', 'usdc', 'dai') %}

select
    transaction_date,
    coin,
    usd_price,
    fee,
    transaction_type,
    exchange_name,
    case
        when
            transaction_type in ('sell', 'withdrawal', 'spot to earn')
            then amount * -1
        else amount
    end as trade_amount,
    case
        when
            transaction_type in ('sell', 'withdrawal', 'spot to earn')
            then (amount + coalesce(fee, 0)) * -1 else
            amount - coalesce(fee, 0)
    end as net_amount,
    case
        when lower(coin) in {{ stablecoins }} then true
        else false
    end as is_stablecoin
from
    {{ ref('gsheets_crypto_invest') }}
