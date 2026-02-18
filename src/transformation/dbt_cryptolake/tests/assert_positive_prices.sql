-- ============================================================
-- Test singular: assert_positive_prices
-- ============================================================
-- Verifica que no hay precios negativos o cero en la fact table.
-- Un precio <= 0 indicaría datos corruptos o un error en la ingesta.
--
-- Si esta consulta devuelve filas, el test FALLA.
-- ============================================================

SELECT
    coin_id,
    price_date,
    price_usd
FROM {{ ref('fact_market_daily') }}
WHERE price_usd <= 0
