-- Dimension: dim_dates
-- Calendar dimension with temporal attributes derived from observed price dates.
-- Enables easy filtering and grouping by quarter, weekday, weekend, etc.

{{ config(materialized='table') }}

WITH date_spine AS (
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
    -- Crypto trades 24/7 but volume typically drops on weekends
    CASE WHEN DAYOFWEEK(date_day) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend,
    DATE_FORMAT(date_day, 'EEEE') AS day_name,
    DATE_FORMAT(date_day, 'MMMM') AS month_name
FROM date_spine
