from __future__ import annotations

"""
Spark Batch Job: Silver -> Gold (Dimensional Model)

Builds a star schema with:
- dim_coins: Dimension with aggregate statistics per cryptocurrency
- dim_dates: Calendar dimension with date attributes for analytics
- fact_market_daily: Fact table with daily metrics and technical signals

Computed metrics:
- Price change % (day-over-day)
- Moving averages (7d, 30d)
- Volatility (7d standard deviation)
- MA30 signal (price above/below 30-day moving average)
- Market sentiment (Fear & Greed Index)

Usage:
    docker exec cryptolake-spark-master \
        /opt/spark/bin/spark-submit /opt/spark/work/src/processing/batch/silver_to_gold.py
"""
from pyspark.sql import SparkSession


def build_dim_coins(spark: SparkSession):
    """Build dim_coins dimension (SCD Type 1 -- overwrite on change)."""
    print("\nBuilding dim_coins...")

    spark.sql("""
        CREATE OR REPLACE TABLE cryptolake.gold.dim_coins
        USING iceberg
        LOCATION 's3://cryptolake-gold/dim_coins'
        AS
        SELECT
            coin_id,

            -- Tracking
            MIN(price_date)                     AS first_tracked_date,
            MAX(price_date)                     AS last_tracked_date,
            COUNT(DISTINCT price_date)          AS total_days_tracked,

            -- Price stats
            ROUND(MIN(price_usd), 6)            AS all_time_low,
            ROUND(MAX(price_usd), 2)            AS all_time_high,
            ROUND(AVG(price_usd), 6)            AS avg_price,

            -- Volume stats
            ROUND(AVG(volume_24h_usd), 2)       AS avg_daily_volume,
            ROUND(MAX(volume_24h_usd), 2)       AS max_daily_volume,

            -- Market cap (latest known value)
            ROUND(MAX(market_cap_usd), 2)       AS max_market_cap,

            -- Historical price range (simplified volatility)
            ROUND(
                ((MAX(price_usd) - MIN(price_usd)) / MIN(price_usd)) * 100,
                2
            )                                   AS price_range_pct,

            current_timestamp()                 AS _loaded_at

        FROM cryptolake.silver.daily_prices
        GROUP BY coin_id
    """)

    count = spark.table("cryptolake.gold.dim_coins").count()
    print(f"  dim_coins: {count} coins")


def build_dim_dates(spark: SparkSession):
    """Build dim_dates calendar dimension from observed dates."""
    print("\nBuilding dim_dates...")

    spark.sql("""
        CREATE OR REPLACE TABLE cryptolake.gold.dim_dates
        USING iceberg
        LOCATION 's3://cryptolake-gold/dim_dates'
        AS
        SELECT DISTINCT
            price_date                              AS date_day,
            YEAR(price_date)                        AS year,
            MONTH(price_date)                       AS month,
            DAYOFMONTH(price_date)                  AS day_of_month,
            DAYOFWEEK(price_date)                   AS day_of_week,
            WEEKOFYEAR(price_date)                  AS week_of_year,
            QUARTER(price_date)                     AS quarter,

            CASE
                WHEN DAYOFWEEK(price_date) IN (1, 7)
                THEN true ELSE false
            END                                     AS is_weekend,

            DATE_FORMAT(price_date, 'EEEE')         AS day_name,
            DATE_FORMAT(price_date, 'MMMM')         AS month_name

        FROM cryptolake.silver.daily_prices
        ORDER BY date_day
    """)

    count = spark.table("cryptolake.gold.dim_dates").count()
    print(f"  dim_dates: {count} dates")


def build_fact_market_daily(spark: SparkSession):
    """Build fact_market_daily with technical indicators and sentiment signals.

    Window functions used:
    - LAG(): previous day price for day-over-day change
    - AVG() OVER (ROWS BETWEEN N PRECEDING AND CURRENT ROW): moving averages
    - STDDEV() OVER (...): rolling volatility

    All windows partitioned by coin_id, ordered by price_date.
    """
    print("\nBuilding fact_market_daily...")

    prices = spark.table("cryptolake.silver.daily_prices")
    fear_greed = spark.table("cryptolake.silver.fear_greed")

    prices.createOrReplaceTempView("s_prices")
    fear_greed.createOrReplaceTempView("s_fear_greed")

    spark.sql("""
        CREATE OR REPLACE TABLE cryptolake.gold.fact_market_daily
        USING iceberg
        PARTITIONED BY (coin_id)
        LOCATION 's3://cryptolake-gold/fact_market_daily'
        AS
        WITH price_metrics AS (
            SELECT
                p.coin_id,
                p.price_date,
                p.price_usd,
                p.market_cap_usd,
                p.volume_24h_usd,

                -- Day-over-day price change %
                ROUND(
                    (p.price_usd - LAG(p.price_usd, 1) OVER w_coin)
                    / LAG(p.price_usd, 1) OVER w_coin * 100,
                    4
                ) AS price_change_pct_1d,

                -- 7-day moving average (short-term trend)
                ROUND(
                    AVG(p.price_usd) OVER (
                        PARTITION BY p.coin_id ORDER BY p.price_date
                        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                    ),
                    6
                ) AS moving_avg_7d,

                -- 30-day moving average (medium-term trend)
                ROUND(
                    AVG(p.price_usd) OVER (
                        PARTITION BY p.coin_id ORDER BY p.price_date
                        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
                    ),
                    6
                ) AS moving_avg_30d,

                -- 7-day volatility (rolling standard deviation)
                ROUND(
                    STDDEV(p.price_usd) OVER (
                        PARTITION BY p.coin_id ORDER BY p.price_date
                        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                    ),
                    6
                ) AS volatility_7d,

                -- 7-day average volume
                ROUND(
                    AVG(p.volume_24h_usd) OVER (
                        PARTITION BY p.coin_id ORDER BY p.price_date
                        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                    ),
                    2
                ) AS avg_volume_7d

            FROM s_prices p
            WINDOW w_coin AS (PARTITION BY p.coin_id ORDER BY p.price_date)
        )

        -- JOIN with Fear & Greed and add trading signals
        SELECT
            pm.coin_id,
            pm.price_date,
            pm.price_usd,
            pm.market_cap_usd,
            pm.volume_24h_usd,
            pm.price_change_pct_1d,
            pm.moving_avg_7d,
            pm.moving_avg_30d,
            pm.volatility_7d,
            pm.avg_volume_7d,

            fg.fear_greed_value,
            fg.classification AS market_sentiment,

            -- MA30 trend signal
            CASE
                WHEN pm.price_usd > pm.moving_avg_30d THEN 'ABOVE_MA30'
                WHEN pm.price_usd < pm.moving_avg_30d THEN 'BELOW_MA30'
                ELSE 'AT_MA30'
            END AS ma30_signal,

            -- Combined signal (technical + sentiment, contrarian approach)
            CASE
                WHEN pm.price_usd < pm.moving_avg_30d
                     AND fg.fear_greed_value < 25
                THEN 'POTENTIAL_BUY'
                WHEN pm.price_usd > pm.moving_avg_30d
                     AND fg.fear_greed_value > 75
                THEN 'POTENTIAL_SELL'
                ELSE 'HOLD'
            END AS combined_signal,

            current_timestamp() AS _loaded_at

        FROM price_metrics pm
        LEFT JOIN s_fear_greed fg
            ON pm.price_date = fg.index_date
    """)

    count = spark.table("cryptolake.gold.fact_market_daily").count()
    print(f"  fact_market_daily: {count} records")


if __name__ == "__main__":
    print("=" * 60)
    print("CryptoLake -- Silver to Gold (Star Schema)")
    print("=" * 60)

    spark = SparkSession.builder.appName("CryptoLake-SilverToGold").getOrCreate()

    try:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS cryptolake.gold LOCATION 's3://cryptolake-gold/'")

        build_dim_coins(spark)
        build_dim_dates(spark)
        build_fact_market_daily(spark)

        # -- Star schema verification -----------------------
        print("\n" + "=" * 60)
        print("GOLD VERIFICATION -- Star Schema")
        print("=" * 60)

        print("\n-- dim_coins --")
        spark.sql("""
            SELECT coin_id, total_days_tracked,
                   all_time_low, all_time_high, price_range_pct
            FROM cryptolake.gold.dim_coins
            ORDER BY price_range_pct DESC
        """).show(truncate=False)

        print("-- dim_dates (sample) --")
        spark.sql("""
            SELECT date_day, day_name, month_name, quarter, is_weekend
            FROM cryptolake.gold.dim_dates
            ORDER BY date_day DESC
            LIMIT 5
        """).show(truncate=False)

        print("-- fact_market_daily: Bitcoin last 7 days --")
        spark.sql("""
            SELECT price_date,
                   ROUND(price_usd, 2) as price,
                   price_change_pct_1d as change_pct,
                   ROUND(moving_avg_7d, 2) as ma7,
                   ROUND(moving_avg_30d, 2) as ma30,
                   ma30_signal,
                   market_sentiment,
                   combined_signal
            FROM cryptolake.gold.fact_market_daily
            WHERE coin_id = 'bitcoin'
            ORDER BY price_date DESC
            LIMIT 7
        """).show(truncate=False)

        print("-- Potential buy signals (latest data) --")
        spark.sql("""
            WITH latest AS (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY coin_id ORDER BY price_date DESC
                ) as rn
                FROM cryptolake.gold.fact_market_daily
            )
            SELECT coin_id, price_date,
                   ROUND(price_usd, 4) as price,
                   price_change_pct_1d,
                   ma30_signal,
                   market_sentiment,
                   combined_signal
            FROM latest
            WHERE rn = 1
            ORDER BY combined_signal, coin_id
        """).show(truncate=False)

    finally:
        spark.stop()

    print("\nGold (Star Schema) completed.")
