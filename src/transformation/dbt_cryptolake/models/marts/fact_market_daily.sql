-- Fact table: fact_market_daily
-- Central fact table of the star schema.
-- Granularity: 1 row = 1 coin x 1 day.
--
-- Contains base metrics (price, market cap, volume) enriched with:
--   - Moving averages (7d, 30d)
--   - 7-day rolling volatility (standard deviation)
--   - MA30 trend signal (ABOVE/BELOW)
--   - Fear & Greed sentiment data (via LEFT JOIN)

{{ config(
    materialized='table',
    unique_key=['coin_id', 'price_date']
) }}

WITH prices AS (
    SELECT * FROM {{ ref('stg_prices') }}
),

fear_greed AS (
    SELECT * FROM {{ ref('stg_fear_greed') }}
),

enriched AS (
    SELECT
        p.coin_id,
        p.price_date,
        p.price_usd,
        p.market_cap_usd,
        p.volume_24h_usd,
        p.price_change_pct_1d,

        -- 7-day moving average
        ROUND(AVG(p.price_usd) OVER (
            PARTITION BY p.coin_id
            ORDER BY p.price_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 2) AS moving_avg_7d,

        -- 30-day moving average
        ROUND(AVG(p.price_usd) OVER (
            PARTITION BY p.coin_id
            ORDER BY p.price_date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ), 2) AS moving_avg_30d,

        -- 7-day rolling volatility (price standard deviation)
        ROUND(STDDEV(p.price_usd) OVER (
            PARTITION BY p.coin_id
            ORDER BY p.price_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 2) AS volatility_7d,

        -- 7-day average volume
        ROUND(AVG(p.volume_24h_usd) OVER (
            PARTITION BY p.coin_id
            ORDER BY p.price_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 2) AS avg_volume_7d,

        -- Market sentiment (LEFT JOIN: not every day has F&G data)
        fg.fear_greed_value,
        fg.classification AS market_sentiment,
        fg.sentiment_score,

        -- MA30 trend signal: price above or below 30-day average
        CASE
            WHEN p.price_usd > AVG(p.price_usd) OVER (
                PARTITION BY p.coin_id
                ORDER BY p.price_date
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ) THEN 'ABOVE_MA30'
            ELSE 'BELOW_MA30'
        END AS ma30_signal

    FROM prices p
    LEFT JOIN fear_greed fg
        ON p.price_date = fg.index_date
)

SELECT
    coin_id,
    price_date,
    price_usd,
    market_cap_usd,
    volume_24h_usd,
    price_change_pct_1d,
    moving_avg_7d,
    moving_avg_30d,
    volatility_7d,
    avg_volume_7d,
    fear_greed_value,
    market_sentiment,
    sentiment_score,
    ma30_signal,
    CURRENT_TIMESTAMP() AS _loaded_at
FROM enriched
