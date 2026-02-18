-- ============================================================
-- Dimensión: dim_coins
-- ============================================================
-- Contiene una fila por criptomoneda con estadísticas agregadas.
--
-- Tipo: SCD Type 1 (Slowly Changing Dimension Type 1)
-- → Cuando los datos cambian, sobrescribimos. No guardamos historial.
-- → Se recrea completa en cada ejecución (materialized: table).
--
-- Ejemplo de resultado:
--   coin_id  | first_tracked_date | all_time_high | avg_price | ...
--   bitcoin  | 2024-11-14         | 106000.0      | 95234.5   | ...
--   ethereum | 2024-11-14         | 3800.0        | 3256.7    | ...
-- ============================================================

{{ config(
    materialized='table',
    unique_key='coin_id'
) }}

WITH coin_stats AS (
    SELECT
        coin_id,

        -- Rango de fechas en que tenemos datos de este coin
        MIN(price_date) AS first_tracked_date,
        MAX(price_date) AS last_tracked_date,
        COUNT(DISTINCT price_date) AS total_days_tracked,

        -- Estadísticas de precio
        MIN(price_usd) AS all_time_low,
        MAX(price_usd) AS all_time_high,
        ROUND(AVG(price_usd), 2) AS avg_price,

        -- Estadísticas de volumen
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

    -- Rango de precio en porcentaje: cuánto varió entre mínimo y máximo
    ROUND(((all_time_high - all_time_low) / all_time_low) * 100, 2) AS price_range_pct,

    CURRENT_TIMESTAMP() AS _loaded_at

FROM coin_stats
