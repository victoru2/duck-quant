{{ config(
    materialized='table',
    schema='gold',
    tags=['crypto_invest']
) }}

select
    coin,
    exchange_name,
    is_stablecoin,
    sum(net_amount) as net_amount,
    count(*) as transaction_count
from
    {{ ref('crypto_invest_current') }}
group by 1, 2, 3
order by 3 desc
