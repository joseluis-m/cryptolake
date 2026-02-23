-- Staging: stg_prices
-- Clean interface over silver.daily_prices.
-- Adds prev_day_price (via LAG) and price_change_pct_1d (day-over-day %).

WITH source AS (
    SELECT * FROM {{ source('silver', 'daily_prices') }}
),

with_lag AS (
    SELECT
        coin_id,
        price_date,
        price_usd,
        market_cap_usd,
        volume_24h_usd,
        _processed_at,
        -- Previous day's price for the same coin
        LAG(price_usd) OVER (
            PARTITION BY coin_id ORDER BY price_date
        ) AS prev_day_price
    FROM source
    WHERE price_usd > 0
)

SELECT
    *,
    -- Day-over-day change: ((new - prev) / prev) * 100
    CASE
        WHEN prev_day_price IS NOT NULL AND prev_day_price > 0
        THEN ROUND(((price_usd - prev_day_price) / prev_day_price) * 100, 4)
        ELSE NULL
    END AS price_change_pct_1d
FROM with_lag
