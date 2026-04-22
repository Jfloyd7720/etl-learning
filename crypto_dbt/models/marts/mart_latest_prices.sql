SELECT *
FROM (
    SELECT *, 
        ROW_NUMBER() OVER (PARTITION BY coin_id ORDER BY ingested_at DESC) as rn
    FROM {{ ref('stg_coin_prices') }}
) as ranked
WHERE rn = 1