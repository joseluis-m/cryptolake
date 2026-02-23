"""
Kafka to Bronze: Spark Structured Streaming consumer.

Reads real-time trade events from Kafka topic 'prices.realtime',
parses the JSON payload, and writes micro-batches to the Bronze
Iceberg table 'cryptolake.bronze.realtime_trades'.

Usage (inside Spark container):
    spark-submit --master local[2] kafka_to_bronze.py

The job runs continuously until stopped (Ctrl+C / SIGTERM).
It checkpoints progress to MinIO so it can resume without data loss.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# Schema matching the JSON produced by binance_producer.py
TRADE_SCHEMA = StructType(
    [
        StructField("coin_id", StringType(), False),
        StructField("symbol", StringType(), True),
        StructField("price_usd", DoubleType(), False),
        StructField("quantity", DoubleType(), True),
        StructField("trade_time_ms", LongType(), True),
        StructField("event_time_ms", LongType(), True),
        StructField("ingested_at", StringType(), True),
        StructField("source", StringType(), True),
        StructField("is_buyer_maker", BooleanType(), True),
    ]
)

KAFKA_SERVERS = "kafka:29092"
KAFKA_TOPIC = "prices.realtime"
CHECKPOINT_PATH = "s3://cryptolake-checkpoints/kafka_to_bronze"
TABLE_NAME = "cryptolake.bronze.realtime_trades"


def main():
    spark = (
        SparkSession.builder.appName("CryptoLake-KafkaToBronze").master("local[2]").getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Create Bronze namespace and table if they don't exist
    spark.sql("CREATE NAMESPACE IF NOT EXISTS cryptolake.bronze")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            coin_id STRING,
            symbol STRING,
            price_usd DOUBLE,
            quantity DOUBLE,
            trade_time_ms BIGINT,
            event_time_ms BIGINT,
            ingested_at STRING,
            source STRING,
            is_buyer_maker BOOLEAN,
            _kafka_partition INT,
            _kafka_offset BIGINT,
            _loaded_at TIMESTAMP
        )
        USING iceberg
        LOCATION 's3://cryptolake-bronze/realtime_trades'
    """
    )

    print(f"Reading from Kafka topic '{KAFKA_TOPIC}' at {KAFKA_SERVERS}...")
    print("Waiting for messages (run the Binance producer to generate data)...")
    print("Press Ctrl+C to stop.\n")

    # Read stream from Kafka
    stream_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Parse JSON value and add metadata
    parsed_df = (
        stream_df.select(
            F.from_json(F.col("value").cast("string"), TRADE_SCHEMA).alias("data"),
            F.col("partition").alias("_kafka_partition"),
            F.col("offset").alias("_kafka_offset"),
        )
        .select("data.*", "_kafka_partition", "_kafka_offset")
        .withColumn("_loaded_at", F.current_timestamp())
    )

    # Write micro-batches to Bronze Iceberg table
    query = (
        parsed_df.writeStream.format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .option("fanout-enabled", "true")
        .trigger(processingTime="10 seconds")
        .toTable(TABLE_NAME)
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
