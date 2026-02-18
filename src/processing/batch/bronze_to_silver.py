from __future__ import annotations

"""
Spark Batch Job: Bronze → Silver

Transformaciones aplicadas:
1. Deduplicación: Si un coin+fecha aparece varias veces (por múltiples
   ejecuciones de bronze-load), nos quedamos con el registro más reciente.
2. Type casting: Convertimos timestamp_ms a DATE.
3. Null handling: Filtramos precios <= 0, dejamos nulls explícitos en campos
   opcionales como market_cap y volume.
4. MERGE INTO: Upsert incremental para no reprocesar todo cada vez.

Ejecución:
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
    """Crea las tablas Silver si no existen."""
    print("\n🏗️  Creando tablas Silver...")

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
    print("  ✅ cryptolake.silver.daily_prices")

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
    print("  ✅ cryptolake.silver.fear_greed")


def process_prices(spark: SparkSession):
    """
    Transforma precios históricos de Bronze → Silver.

    Pasos detallados:
    1. Leer de Bronze
    2. Convertir timestamp_ms → price_date (DATE)
    3. Deduplicar: window function ROW_NUMBER para quedarnos con
       el registro más reciente por (coin_id, price_date)
    4. Filtrar precios inválidos (<=0)
    5. MERGE INTO Silver (upsert)
    """
    print("\n📊 PROCESANDO PRECIOS: Bronze → Silver")
    print("=" * 50)

    # 1. Leer de Bronze
    bronze_df = spark.table("cryptolake.bronze.historical_prices")
    total_bronze = bronze_df.count()
    print(f"  📥 Registros en Bronze: {total_bronze}")

    # 2. Convertir timestamp a fecha
    # from_unixtime espera segundos, pero timestamp_ms está en milisegundos
    # por eso dividimos entre 1000
    typed_df = bronze_df.withColumn(
        "price_date", from_unixtime(col("timestamp_ms") / 1000).cast("date")
    )

    # 3. Deduplicar
    # Window function: para cada grupo (coin_id, price_date),
    # ordena por _loaded_at descendente (más reciente primero)
    # y asigna row_number. Nos quedamos solo con row_number = 1.
    dedup_window = Window.partitionBy("coin_id", "price_date").orderBy(col("_loaded_at").desc())

    deduped_df = (
        typed_df.withColumn("_row_num", row_number().over(dedup_window))
        .filter(col("_row_num") == 1)
        .drop("_row_num")
    )

    # 4. Filtrar precios inválidos y limpiar nulls
    cleaned_df = (
        deduped_df.filter(col("price_usd") > 0)
        .withColumn("market_cap_usd", when(col("market_cap_usd") > 0, col("market_cap_usd")))
        .withColumn("volume_24h_usd", when(col("volume_24h_usd") > 0, col("volume_24h_usd")))
        .withColumn("_processed_at", current_timestamp())
        .select(
            "coin_id",
            "price_date",
            "price_usd",
            "market_cap_usd",
            "volume_24h_usd",
            "_processed_at",
        )
    )

    total_clean = cleaned_df.count()
    duplicates_removed = total_bronze - total_clean
    print(f"  🧹 Registros tras dedup + limpieza: {total_clean}")
    print(f"  🗑️  Duplicados/inválidos eliminados: {duplicates_removed}")

    # 5. MERGE INTO Silver
    # Primero registramos el DataFrame como vista temporal (tabla en memoria).
    # Luego usamos MERGE INTO para hacer upsert.
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

    # Verificar resultado
    silver_count = spark.table("cryptolake.silver.daily_prices").count()
    print(f"  ✅ Silver daily_prices: {silver_count} registros totales")


def process_fear_greed(spark: SparkSession):
    """Transforma Fear & Greed de Bronze → Silver."""
    print("\n📊 PROCESANDO FEAR & GREED: Bronze → Silver")
    print("=" * 50)

    bronze_df = spark.table("cryptolake.bronze.fear_greed")
    print(f"  📥 Registros en Bronze: {bronze_df.count()}")

    # Convertir timestamp (segundos) a fecha y deduplicar
    dedup_window = Window.partitionBy("index_date").orderBy(col("_loaded_at").desc())

    silver_df = (
        bronze_df.withColumn("index_date", from_unixtime(col("timestamp")).cast("date"))
        .withColumn("_row_num", row_number().over(dedup_window))
        .filter(col("_row_num") == 1)
        .withColumn("_processed_at", current_timestamp())
        .select(
            "index_date",
            col("value").alias("fear_greed_value"),
            "classification",
            "_processed_at",
        )
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
    print(f"  ✅ Silver fear_greed: {count} registros totales")


if __name__ == "__main__":
    print("=" * 60)
    print("🚀 CryptoLake — Bronze to Silver")
    print("=" * 60)

    spark = SparkSession.builder.appName("CryptoLake-BronzeToSilver").getOrCreate()

    try:
        create_silver_tables(spark)
        process_prices(spark)
        process_fear_greed(spark)

        # Verificación final
        print("\n" + "=" * 60)
        print("📋 VERIFICACIÓN SILVER")
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

    print("\n✅ Silver transform completado!")
