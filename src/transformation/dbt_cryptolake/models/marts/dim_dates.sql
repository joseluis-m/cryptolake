-- ============================================================
-- Dimensión: dim_dates
-- ============================================================
-- Calendario con atributos útiles para análisis temporal.
--
-- ¿Por qué una tabla de fechas separada?
-- En un star schema, las dimensiones de fecha permiten filtrar y agrupar
-- fácilmente: "ventas del Q1", "solo días laborables", "por mes", etc.
-- Sin esta tabla, tendrías que calcular YEAR(), MONTH(), etc. en cada
-- consulta. Con ella, solo haces un JOIN y filtras.
--
-- Ejemplo de resultado:
--   date_day   | year | month | quarter | is_weekend | day_name
--   2024-11-14 | 2024 | 11    | 4       | false      | Thursday
--   2024-11-15 | 2024 | 11    | 4       | false      | Friday
--   2024-11-16 | 2024 | 11    | 4       | true       | Saturday
-- ============================================================

{{ config(materialized='table') }}

WITH date_spine AS (
    -- Extraemos todas las fechas únicas de nuestros datos de precios
    SELECT DISTINCT price_date AS date_day
    FROM {{ ref('stg_prices') }}
)

SELECT
    date_day,
    YEAR(date_day) AS year,
    MONTH(date_day) AS month,
    DAY(date_day) AS day_of_month,
    DAYOFWEEK(date_day) AS day_of_week,
    WEEKOFYEAR(date_day) AS week_of_year,
    QUARTER(date_day) AS quarter,

    -- Flag de fin de semana (útil para análisis de volumen)
    -- Crypto opera 24/7, pero el volumen suele bajar los fines de semana
    CASE WHEN DAYOFWEEK(date_day) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend,

    -- Nombres legibles para dashboards
    DATE_FORMAT(date_day, 'EEEE') AS day_name,
    DATE_FORMAT(date_day, 'MMMM') AS month_name

FROM date_spine
