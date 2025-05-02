{{ config(
    materialized='table',
    schema='gold'
) }}

select
    coin,
    exchange_name,
    SUM(net_amount) as net_amount,
    COUNT(*) as transaction_count
from
    {{ ref('net_amount_per_transaction') }}
group by 1, 2
order by 3 desc
