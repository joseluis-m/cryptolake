from __future__ import annotations

"""
Spark Batch Job: APIs -> Iceberg Bronze

Extracts data from CoinGecko and Fear & Greed Index APIs,
then loads it into Iceberg tables in the Bronze layer.

Bronze = raw, untransformed, append-only data.
Everything is stored as-is to preserve the original source of truth.

Usage:
    docker exec cryptolake-spark-master \
        /opt/spark/bin/spark-submit /opt/spark/work/src/processing/batch/api_to_bronze.py
"""

import time
from datetime import datetime, timezone

import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# ================================================================
# CONFIGURATION
# ================================================================

COINGECKO_BASE_URL = "https://api.coingecko.com/api/v3"
FEAR_GREED_URL = "https://api.alternative.me/fng/"
DAYS_TO_EXTRACT = 90

# Canonical list of tracked coins (CoinGecko IDs).
# Must match TRACKED_COINS in .env.example and settings.py.
TRACKED_COINS = [
    "bitcoin",
    "ethereum",
    "solana",
    "hyperliquid",
    "chainlink",
    "uniswap",
    "aave",
    "bittensor",
    "ondo",
]


# ================================================================
# SCHEMAS
# ================================================================

BRONZE_HISTORICAL_SCHEMA = StructType(
    [
        StructField("coin_id", StringType(), nullable=False),
        StructField("timestamp_ms", LongType(), nullable=False),
        StructField("price_usd", DoubleType(), nullable=False),
        StructField("market_cap_usd", DoubleType(), nullable=True),
        StructField("volume_24h_usd", DoubleType(), nullable=True),
        StructField("_ingested_at", StringType(), nullable=False),
        StructField("_source", StringType(), nullable=False),
    ]
)

BRONZE_FEAR_GREED_SCHEMA = StructType(
    [
        StructField("value", IntegerType(), nullable=False),
        StructField("classification", StringType(), nullable=False),
        StructField("timestamp", LongType(), nullable=False),
        StructField("_ingested_at", StringType(), nullable=False),
        StructField("_source", StringType(), nullable=False),
    ]
)


# ================================================================
# EXTRACTION FUNCTIONS
# ================================================================


def extract_coingecko(days: int = 90) -> list[dict]:
    """Extract historical prices from CoinGecko with retry and backoff."""
    session = requests.Session()
    session.headers.update({"User-Agent": "CryptoLake/1.0"})

    all_records = []
    now = datetime.now(timezone.utc).isoformat()

    for i, coin_id in enumerate(TRACKED_COINS):
        try:
            print(f"  Extracting {coin_id} ({i + 1}/{len(TRACKED_COINS)})...")

            max_retries = 3
            response = None

            for attempt in range(max_retries + 1):
                response = session.get(
                    f"{COINGECKO_BASE_URL}/coins/{coin_id}/market_chart",
                    params={
                        "vs_currency": "usd",
                        "days": str(days),
                        "interval": "daily",
                    },
                    timeout=30,
                )

                if response.status_code == 429:
                    if attempt < max_retries:
                        wait = 30 * (2**attempt)
                        print(f"  Rate limited, waiting {wait}s (attempt {attempt + 1})...")
                        time.sleep(wait)
                    else:
                        response.raise_for_status()
                else:
                    break

            response.raise_for_status()
            data = response.json()

            prices = data.get("prices", [])
            market_caps = data.get("market_caps", [])
            volumes = data.get("total_volumes", [])

            for idx, (ts, price) in enumerate(prices):
                if price and price > 0:
                    all_records.append(
                        {
                            "coin_id": coin_id,
                            "timestamp_ms": int(ts),
                            "price_usd": float(price),
                            "market_cap_usd": (
                                float(market_caps[idx][1])
                                if idx < len(market_caps) and market_caps[idx][1]
                                else None
                            ),
                            "volume_24h_usd": (
                                float(volumes[idx][1])
                                if idx < len(volumes) and volumes[idx][1]
                                else None
                            ),
                            "_ingested_at": now,
                            "_source": "coingecko",
                        }
                    )

            print(f"  {coin_id}: {len(prices)} datapoints OK")

            # Rate limit: CoinGecko free tier allows ~10 req/min
            if i < len(TRACKED_COINS) - 1:
                time.sleep(6)

        except Exception as e:
            print(f"  {coin_id} failed: {e}")
            continue

    return all_records


def extract_fear_greed(days: int = 90) -> list[dict]:
    """Extract historical Fear & Greed Index."""
    session = requests.Session()
    session.headers.update({"User-Agent": "CryptoLake/1.0"})
    now = datetime.now(timezone.utc).isoformat()

    print(f"  Extracting Fear & Greed Index ({days} days)...")

    response = session.get(
        FEAR_GREED_URL,
        params={"limit": str(days), "format": "json"},
        timeout=30,
    )
    response.raise_for_status()
    data = response.json()

    records = []
    for entry in data.get("data", []):
        value = int(entry["value"])
        if 0 <= value <= 100:
            records.append(
                {
                    "value": value,
                    "classification": entry["value_classification"],
                    "timestamp": int(entry["timestamp"]),
                    "_ingested_at": now,
                    "_source": "fear_greed_index",
                }
            )

    print(f"  Fear & Greed: {len(records)} datapoints OK")
    return records


# ================================================================
# LOAD FUNCTIONS
# ================================================================


def create_bronze_tables(spark: SparkSession):
    """Create Iceberg Bronze tables if they don't exist."""
    print("\nCreating Bronze tables (if not exist)...")

    spark.sql("CREATE NAMESPACE IF NOT EXISTS cryptolake.bronze LOCATION 's3://cryptolake-bronze/'")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS cryptolake.bronze.historical_prices (
            coin_id         STRING      NOT NULL,
            timestamp_ms    BIGINT      NOT NULL,
            price_usd       DOUBLE      NOT NULL,
            market_cap_usd  DOUBLE,
            volume_24h_usd  DOUBLE,
            _ingested_at    STRING      NOT NULL,
            _source         STRING      NOT NULL,
            _loaded_at      TIMESTAMP   NOT NULL
        )
        USING iceberg
        PARTITIONED BY (coin_id)
        LOCATION 's3://cryptolake-bronze/historical_prices'
        TBLPROPERTIES (
            'write.format.default' = 'parquet',
            'write.parquet.compression-codec' = 'zstd'
        )
    """)
    print("  cryptolake.bronze.historical_prices OK")

    spark.sql("""
        CREATE TABLE IF NOT EXISTS cryptolake.bronze.fear_greed (
            value           INT         NOT NULL,
            classification  STRING      NOT NULL,
            timestamp       BIGINT      NOT NULL,
            _ingested_at    STRING      NOT NULL,
            _source         STRING      NOT NULL,
            _loaded_at      TIMESTAMP   NOT NULL
        )
        USING iceberg
        LOCATION 's3://cryptolake-bronze/fear_greed'
        TBLPROPERTIES (
            'write.format.default' = 'parquet',
            'write.parquet.compression-codec' = 'zstd'
        )
    """)
    print("  cryptolake.bronze.fear_greed OK")


def load_to_bronze(spark: SparkSession):
    """Extract from APIs and append to Iceberg Bronze tables."""
    create_bronze_tables(spark)

    # -- Historical prices ------------------------------------
    print("\nLOADING HISTORICAL PRICES")
    print("=" * 50)

    price_records = extract_coingecko(days=DAYS_TO_EXTRACT)

    if price_records:
        prices_df = spark.createDataFrame(price_records, schema=BRONZE_HISTORICAL_SCHEMA)
        prices_df = prices_df.withColumn("_loaded_at", current_timestamp())
        prices_df.writeTo("cryptolake.bronze.historical_prices").append()

        count = prices_df.count()
        print(f"\n  {count} records loaded into bronze.historical_prices")
    else:
        print("  WARNING: No price records extracted")

    # -- Fear & Greed Index -----------------------------------
    print("\nLOADING FEAR & GREED INDEX")
    print("=" * 50)

    fg_records = extract_fear_greed(days=DAYS_TO_EXTRACT)

    if fg_records:
        fg_df = spark.createDataFrame(fg_records, schema=BRONZE_FEAR_GREED_SCHEMA)
        fg_df = fg_df.withColumn("_loaded_at", current_timestamp())
        fg_df.writeTo("cryptolake.bronze.fear_greed").append()

        count = fg_df.count()
        print(f"\n  {count} records loaded into bronze.fear_greed")
    else:
        print("  WARNING: No Fear & Greed records extracted")


# ================================================================
# MAIN
# ================================================================

if __name__ == "__main__":
    print("=" * 60)
    print("CryptoLake -- API to Bronze")
    print("=" * 60)

    spark = SparkSession.builder.appName("CryptoLake-APIToBronze").getOrCreate()

    try:
        load_to_bronze(spark)

        # -- Verification ------------------------------------
        print("\n" + "=" * 60)
        print("VERIFICATION")
        print("=" * 60)

        print("\n  bronze.historical_prices:")
        spark.sql("""
            SELECT coin_id, COUNT(*) as records,
                   ROUND(MIN(price_usd), 2) as min_price,
                   ROUND(MAX(price_usd), 2) as max_price
            FROM cryptolake.bronze.historical_prices
            GROUP BY coin_id
            ORDER BY coin_id
        """).show(truncate=False)

        print("  bronze.fear_greed:")
        spark.sql("""
            SELECT classification, COUNT(*) as days
            FROM cryptolake.bronze.fear_greed
            GROUP BY classification
            ORDER BY days DESC
        """).show(truncate=False)

        print("  Iceberg snapshots (version history):")
        spark.sql("""
            SELECT snapshot_id, committed_at, operation
            FROM cryptolake.bronze.historical_prices.snapshots
        """).show(truncate=False)

    finally:
        spark.stop()

    print("\nBronze load completed.")
