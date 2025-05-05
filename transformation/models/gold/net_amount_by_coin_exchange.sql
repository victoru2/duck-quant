{{ config(
    materialized='table',
    schema='gold'
) }}

select
    coin,
    exchange_name,
    is_stablecoin,
    sum(net_amount) as net_amount,
    count(*) as transaction_count
from
    {{ ref('net_amount_per_transaction') }}
group by 1, 2, 3
order by 3 desc
