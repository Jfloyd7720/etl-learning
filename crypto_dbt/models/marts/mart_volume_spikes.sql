SELECT *
FROM {{ ref('stg_coin_prices') }}
WHERE volume_spike = true