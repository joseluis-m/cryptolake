-- ============================================================
-- Staging: stg_prices
-- ============================================================
-- Interfaz limpia sobre silver.daily_prices.
--
-- Añade dos campos calculados:
--   - prev_day_price: precio del día anterior (con LAG window function)
--   - price_change_pct_1d: cambio porcentual respecto al día anterior
--
-- LAG() es una window function que "mira hacia atrás" en los datos:
--   LAG(columna) OVER (PARTITION BY grupo ORDER BY orden)
--   = "dame el valor de 'columna' en la fila anterior dentro del mismo 'grupo'"
--
-- Ejemplo con Bitcoin:
--   Fecha       Precio    LAG(precio)  Change %
--   2024-01-01  42000     NULL         NULL (no hay día anterior)
--   2024-01-02  43000     42000        +2.38%
--   2024-01-03  41500     43000        -3.49%
-- ============================================================

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

        -- LAG: precio del día anterior para esta misma moneda
        LAG(price_usd) OVER (
            PARTITION BY coin_id ORDER BY price_date
        ) AS prev_day_price

    FROM source
    WHERE price_usd > 0
)

SELECT
    *,
    -- Cálculo del cambio porcentual día a día
    -- Fórmula: ((nuevo - anterior) / anterior) * 100
    CASE
        WHEN prev_day_price IS NOT NULL AND prev_day_price > 0
        THEN ROUND(((price_usd - prev_day_price) / prev_day_price) * 100, 4)
        ELSE NULL
    END AS price_change_pct_1d
FROM with_lag
