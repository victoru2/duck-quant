{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    schema='bronze',
    tags=['crypto_invest', 'binance_api']
  )
}}

{{ get_binance_prices() }}
