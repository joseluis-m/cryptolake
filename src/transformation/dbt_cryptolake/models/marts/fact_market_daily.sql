-- ============================================================
-- Fact Table: fact_market_daily
-- ============================================================
-- Tabla de hechos central del star schema.
-- Granularidad: 1 fila = 1 moneda × 1 día
--
-- Contiene:
--   - Datos base: precio, market cap, volumen
--   - Métricas calculadas con window functions:
--     · Moving averages (7d, 30d)
--     · Volatilidad 7 días
--     · Señal MA30 (above/below)
--   - Datos de sentimiento del mercado (Fear & Greed)
--
-- Window Functions usadas:
-- ──────────────────────────────────────────────────────────
-- AVG(...) OVER (PARTITION BY coin ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
--
--   PARTITION BY coin: calcula por separado para cada moneda
--   ORDER BY date: ordena cronológicamente
--   ROWS BETWEEN 6 PRECEDING AND CURRENT ROW: ventana de 7 días
--                                              (6 anteriores + el actual)
--
-- Ejemplo visual para Bitcoin, ventana de 3 días:
--   Fecha       Precio    AVG(ventana 3d)
--   2024-01-01  42000     42000        ← solo 1 día disponible
--   2024-01-02  43000     42500        ← promedio de 2 días
--   2024-01-03  41500     42166.67     ← promedio de 3 días (42000+43000+41500)/3
--   2024-01-04  44000     42833.33     ← ventana se "desliza" (43000+41500+44000)/3
-- ============================================================

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

-- Calculamos todas las métricas con window functions
enriched AS (
    SELECT
        p.coin_id,
        p.price_date,
        p.price_usd,
        p.market_cap_usd,
        p.volume_24h_usd,
        p.price_change_pct_1d,

        -- ═══════════════════════════════════════════════════
        -- MEDIAS MÓVILES (Moving Averages)
        -- ═══════════════════════════════════════════════════
        -- Media de los últimos 7 días. Suaviza el ruido diario.
        -- Si el precio está por encima de la MA7, tendencia alcista a corto plazo.
        ROUND(AVG(p.price_usd) OVER (
            PARTITION BY p.coin_id
            ORDER BY p.price_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 2) AS moving_avg_7d,

        -- Media de los últimos 30 días. Indica tendencia de medio plazo.
        ROUND(AVG(p.price_usd) OVER (
            PARTITION BY p.coin_id
            ORDER BY p.price_date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ), 2) AS moving_avg_30d,

        -- ═══════════════════════════════════════════════════
        -- VOLATILIDAD (desviación estándar 7 días)
        -- ═══════════════════════════════════════════════════
        -- Mide cuánto fluctúa el precio. Alta volatilidad = más riesgo.
        -- STDDEV calcula la dispersión estadística del precio.
        ROUND(STDDEV(p.price_usd) OVER (
            PARTITION BY p.coin_id
            ORDER BY p.price_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 2) AS volatility_7d,

        -- Media de volumen 7 días (tendencia de actividad)
        ROUND(AVG(p.volume_24h_usd) OVER (
            PARTITION BY p.coin_id
            ORDER BY p.price_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 2) AS avg_volume_7d,

        -- ═══════════════════════════════════════════════════
        -- SENTIMIENTO DEL MERCADO (Fear & Greed Index)
        -- ═══════════════════════════════════════════════════
        -- LEFT JOIN porque no todos los días tienen dato de F&G
        fg.fear_greed_value,
        fg.classification AS market_sentiment,
        fg.sentiment_score,

        -- ═══════════════════════════════════════════════════
        -- SEÑAL MA30: ¿Precio por encima o debajo de la media 30d?
        -- ═══════════════════════════════════════════════════
        -- Señal técnica básica:
        --   ABOVE_MA30 = tendencia alcista (precio > media)
        --   BELOW_MA30 = tendencia bajista (precio < media)
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
