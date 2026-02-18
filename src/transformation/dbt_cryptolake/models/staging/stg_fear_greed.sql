-- ============================================================
-- Staging: stg_fear_greed
-- ============================================================
-- Interfaz limpia sobre silver.fear_greed.
--
-- Añade un sentiment_score numérico para facilitar el análisis.
-- Es más fácil hacer AVG(sentiment_score) que contar strings.
--
-- Clasificación:
--   "Extreme Fear"  → 1  (pánico en el mercado)
--   "Fear"          → 2
--   "Neutral"       → 3
--   "Greed"         → 4
--   "Extreme Greed" → 5  (euforia, posible burbuja)
-- ============================================================

WITH source AS (
    SELECT * FROM {{ source('silver', 'fear_greed') }}
)

SELECT
    index_date,
    fear_greed_value,
    classification,

    -- Convertimos la clasificación textual a un score numérico
    CASE classification
        WHEN 'Extreme Fear' THEN 1
        WHEN 'Fear' THEN 2
        WHEN 'Neutral' THEN 3
        WHEN 'Greed' THEN 4
        WHEN 'Extreme Greed' THEN 5
    END AS sentiment_score,

    _processed_at

FROM source
