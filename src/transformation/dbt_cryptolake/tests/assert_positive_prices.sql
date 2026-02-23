-- Singular test: assert_positive_prices
-- Verifies no negative or zero prices exist in the fact table.
-- A price <= 0 would indicate corrupted data or an ingestion error.
-- Test FAILS if this query returns any rows.

SELECT
    coin_id,
    price_date,
    price_usd
FROM {{ ref('fact_market_daily') }}
WHERE price_usd <= 0
