-- Staging: stg_fear_greed
-- Clean interface over silver.fear_greed.
-- Adds a numeric sentiment_score for easier aggregation:
--   Extreme Fear -> 1, Fear -> 2, Neutral -> 3, Greed -> 4, Extreme Greed -> 5

WITH source AS (
    SELECT * FROM {{ source('silver', 'fear_greed') }}
)

SELECT
    index_date,
    fear_greed_value,
    classification,
    CASE classification
        WHEN 'Extreme Fear' THEN 1
        WHEN 'Fear' THEN 2
        WHEN 'Neutral' THEN 3
        WHEN 'Greed' THEN 4
        WHEN 'Extreme Greed' THEN 5
    END AS sentiment_score,
    _processed_at
FROM source
