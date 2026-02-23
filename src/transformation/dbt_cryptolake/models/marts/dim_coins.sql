-- Dimension: dim_coins
-- One row per cryptocurrency with aggregate statistics.
-- SCD Type 1: overwritten on each run, no history kept.

{{ config(
    materialized='table',
    unique_key='coin_id'
) }}

WITH coin_stats AS (
    SELECT
        coin_id,
        MIN(price_date) AS first_tracked_date,
        MAX(price_date) AS last_tracked_date,
        COUNT(DISTINCT price_date) AS total_days_tracked,
        MIN(price_usd) AS all_time_low,
        MAX(price_usd) AS all_time_high,
        ROUND(AVG(price_usd), 2) AS avg_price,
        ROUND(AVG(volume_24h_usd), 2) AS avg_daily_volume
    FROM {{ ref('stg_prices') }}
    GROUP BY coin_id
)

SELECT
    coin_id,
    first_tracked_date,
    last_tracked_date,
    total_days_tracked,
    all_time_low,
    all_time_high,
    avg_price,
    avg_daily_volume,
    -- Price range as percentage: ((max - min) / min) * 100
    ROUND(((all_time_high - all_time_low) / all_time_low) * 100, 2) AS price_range_pct,
    CURRENT_TIMESTAMP() AS _loaded_at
FROM coin_stats
