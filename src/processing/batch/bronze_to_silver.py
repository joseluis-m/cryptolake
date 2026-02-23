from __future__ import annotations

"""
Spark Batch Job: Bronze -> Silver

Transformations applied:
1. Deduplication via ROW_NUMBER window function (keep latest per coin+date)
2. Type casting: timestamp_ms -> DATE
3. Null handling: filter price_usd <= 0, explicit nulls for optional fields
4. MERGE INTO: incremental upsert to avoid full reprocessing

Usage:
    docker exec cryptolake-spark-master \
        /opt/spark/bin/spark-submit /opt/spark/work/src/processing/batch/bronze_to_silver.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    from_unixtime,
    row_number,
    when,
)
from pyspark.sql.window import Window


def create_silver_tables(spark: SparkSession):
    """Create Silver tables if they don't exist."""
    print("\nCreating Silver tables...")

    spark.sql("CREATE NAMESPACE IF NOT EXISTS cryptolake.silver LOCATION 's3://cryptolake-silver/'")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS cryptolake.silver.daily_prices (
            coin_id         STRING      NOT NULL,
            price_date      DATE        NOT NULL,
            price_usd       DOUBLE      NOT NULL,
            market_cap_usd  DOUBLE,
            volume_24h_usd  DOUBLE,
            _processed_at   TIMESTAMP   NOT NULL
        )
        USING iceberg
        PARTITIONED BY (coin_id)
        LOCATION 's3://cryptolake-silver/daily_prices'
    """)
    print("  cryptolake.silver.daily_prices OK")

    spark.sql("""
        CREATE TABLE IF NOT EXISTS cryptolake.silver.fear_greed (
            index_date          DATE        NOT NULL,
            fear_greed_value    INT         NOT NULL,
            classification      STRING      NOT NULL,
            _processed_at       TIMESTAMP   NOT NULL
        )
        USING iceberg
        LOCATION 's3://cryptolake-silver/fear_greed'
    """)
    print("  cryptolake.silver.fear_greed OK")


def process_prices(spark: SparkSession):
    """Transform historical prices from Bronze -> Silver via MERGE INTO."""
    print("\nPROCESSING PRICES: Bronze -> Silver")
    print("=" * 50)

    bronze_df = spark.table("cryptolake.bronze.historical_prices")
    total_bronze = bronze_df.count()
    print(f"  Bronze records: {total_bronze}")

    # Convert timestamp_ms (milliseconds) to DATE
    typed_df = bronze_df.withColumn(
        "price_date", from_unixtime(col("timestamp_ms") / 1000).cast("date")
    )

    # Deduplicate: keep most recent record per (coin_id, price_date)
    dedup_window = Window.partitionBy("coin_id", "price_date").orderBy(col("_loaded_at").desc())
    deduped_df = (
        typed_df.withColumn("_row_num", row_number().over(dedup_window))
        .filter(col("_row_num") == 1)
        .drop("_row_num")
    )

    # Filter invalid prices and clean nulls
    cleaned_df = (
        deduped_df.filter(col("price_usd") > 0)
        .withColumn("market_cap_usd", when(col("market_cap_usd") > 0, col("market_cap_usd")))
        .withColumn("volume_24h_usd", when(col("volume_24h_usd") > 0, col("volume_24h_usd")))
        .withColumn("_processed_at", current_timestamp())
        .select("coin_id", "price_date", "price_usd", "market_cap_usd", "volume_24h_usd", "_processed_at")
    )

    total_clean = cleaned_df.count()
    print(f"  After dedup + cleaning: {total_clean}")
    print(f"  Duplicates/invalid removed: {total_bronze - total_clean}")

    # MERGE INTO Silver (upsert)
    cleaned_df.createOrReplaceTempView("price_updates")
    spark.sql("""
        MERGE INTO cryptolake.silver.daily_prices AS target
        USING price_updates AS source
        ON target.coin_id = source.coin_id
           AND target.price_date = source.price_date
        WHEN MATCHED THEN UPDATE SET
            price_usd = source.price_usd,
            market_cap_usd = source.market_cap_usd,
            volume_24h_usd = source.volume_24h_usd,
            _processed_at = source._processed_at
        WHEN NOT MATCHED THEN INSERT *
    """)

    silver_count = spark.table("cryptolake.silver.daily_prices").count()
    print(f"  Silver daily_prices: {silver_count} total records")


def process_fear_greed(spark: SparkSession):
    """Transform Fear & Greed from Bronze -> Silver."""
    print("\nPROCESSING FEAR & GREED: Bronze -> Silver")
    print("=" * 50)

    bronze_df = spark.table("cryptolake.bronze.fear_greed")
    print(f"  Bronze records: {bronze_df.count()}")

    dedup_window = Window.partitionBy("index_date").orderBy(col("_loaded_at").desc())

    silver_df = (
        bronze_df.withColumn("index_date", from_unixtime(col("timestamp")).cast("date"))
        .withColumn("_row_num", row_number().over(dedup_window))
        .filter(col("_row_num") == 1)
        .withColumn("_processed_at", current_timestamp())
        .select("index_date", col("value").alias("fear_greed_value"), "classification", "_processed_at")
    )

    silver_df.createOrReplaceTempView("fg_updates")
    spark.sql("""
        MERGE INTO cryptolake.silver.fear_greed AS target
        USING fg_updates AS source
        ON target.index_date = source.index_date
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    count = spark.table("cryptolake.silver.fear_greed").count()
    print(f"  Silver fear_greed: {count} total records")


if __name__ == "__main__":
    print("=" * 60)
    print("CryptoLake -- Bronze to Silver")
    print("=" * 60)

    spark = SparkSession.builder.appName("CryptoLake-BronzeToSilver").getOrCreate()

    try:
        create_silver_tables(spark)
        process_prices(spark)
        process_fear_greed(spark)

        print("\n" + "=" * 60)
        print("SILVER VERIFICATION")
        print("=" * 60)

        spark.sql("""
            SELECT coin_id, COUNT(*) as days,
                   MIN(price_date) as from_date,
                   MAX(price_date) as to_date,
                   ROUND(AVG(price_usd), 2) as avg_price
            FROM cryptolake.silver.daily_prices
            GROUP BY coin_id ORDER BY coin_id
        """).show(truncate=False)

        spark.sql("""
            SELECT classification, COUNT(*) as days
            FROM cryptolake.silver.fear_greed
            GROUP BY classification ORDER BY days DESC
        """).show(truncate=False)

    finally:
        spark.stop()

    print("\nSilver transform completed.")
