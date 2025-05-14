{{ config(
    materialized='table',
    schema='gold',
    tags=['crypto_invest']
) }}

select
    transaction_date,
    coin,
    usd_price,
    fee,
    transaction_type,
    exchange_name,
    is_stablecoin,
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
    end as net_amount
from
    {{ ref('gsheets_crypto_invest') }}
