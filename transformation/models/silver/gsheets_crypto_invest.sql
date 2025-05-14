{{ config(
    materialized='table',
    schema='silver',
    tags=['crypto_invest']
) }}

{% set stablecoins = ('usdt', 'usdc', 'dai') %}

select
    strptime(date, '%d/%m/%Y')::DATE as transaction_date,
    trim(coin) as coin,
    case
        when usd_price = '' or usd_price is null then null
        else (replace(replace(usd_price, '.', ''), ',', '.'))::DECIMAL(20, 8)
    end as usd_price,
    case
        when amount = '' or amount is null then null
        else abs((replace(replace(amount, '.', ''), ',', '.'))::DECIMAL(20, 8))
    end as amount,
    case
        when fee = '' or fee is null then null
        else abs((replace(replace(fee, '.', ''), ',', '.'))::DECIMAL(20, 8))
    end as fee,
    nullif(trim(fee_coin), '') as fee_coin,
    nullif(trim(tx_id), '') as tx_id,
    lower(trim(transaction_type)) as transaction_type,
    lower(trim(exchange)) as exchange_name,
    lower(trim(notes)) as notes,
    case
        when lower(trim(coin)) in {{ stablecoins }} then true
        else false
    end as is_stablecoin
from
    {{ ref('gsheets_crypto_invest_raw') }}
