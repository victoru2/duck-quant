{% macro get_binance_prices() %}
    {% set coins_query %}
        select distinct trim(lower(coin)) as symbol
        from {{ ref('gsheets_crypto_invest') }}
        where
            coin is not null
            and coin != ''
            and exchange_name = 'binance'
            and not is_stablecoin
    {% endset %}

    {% set results = run_query(coins_query) %}

    {% if execute %}
        {% set coins = results.columns[0].values() if results and results.columns else [] %}
    {% else %}
        {% set coins = [] %}
    {% endif %}

    {% if not coins %}
        {{ log("No coins found, using default set", info=True) }}
        {% set coins = ['btc','eth'] %}
    {% endif %}

    {% for coin in coins %}
        {% set pair =
            coin.upper() ~ 'USDT'
        %}
        select
            '{{ coin }}' as symbol,
            try_cast(
                r.price as decimal(20,8)
            ) as price_usd,
            CURRENT_TIMESTAMP AS updated_at
        from read_json(
            'https://api.binance.com/api/v3/ticker/price?symbol={{ pair }}'
        ) as r
        {% if not loop.last %} union all {% endif %}
    {% endfor %}
{% endmacro %}
