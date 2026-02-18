from __future__ import annotations

"""
Spark Batch Job: APIs → Iceberg Bronze

Extrae datos de CoinGecko y Fear & Greed Index, y los carga
en tablas Iceberg en la capa Bronze del Lakehouse.

Bronze = datos crudos, sin transformar, append-only.
Guardamos todo tal cual llega para tener la "fuente de verdad" original.

Ejecución:
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
# CONFIGURACIÓN
# ================================================================
# Definimos todo aquí para que el script sea autocontenido.
# En producción usarías variables de entorno o un config service.

COINGECKO_BASE_URL = "https://api.coingecko.com/api/v3"
FEAR_GREED_URL = "https://api.alternative.me/fng/"
DAYS_TO_EXTRACT = 90

TRACKED_COINS = [
    "bitcoin",
    "ethereum",
    "solana",
    "cardano",
    "polkadot",
    "chainlink",
    "avalanche-2",
    "polygon-ecosystem-token",
]


# ================================================================
# SCHEMAS
# ================================================================
# StructType define la estructura de un DataFrame, como una CREATE TABLE.
# Es obligatorio definir schemas explícitos en data engineering — nunca
# dejes que Spark "infiera" el schema porque puede equivocarse y además
# no documentas qué esperas recibir.

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
# FUNCIONES DE EXTRACCIÓN
# ================================================================


def extract_coingecko(days: int = 90) -> list[dict]:
    """
    Extrae precios históricos de CoinGecko.
    Incluye retry con backoff exponencial para manejar rate limits.
    """
    session = requests.Session()
    session.headers.update({"User-Agent": "CryptoLake/1.0"})

    all_records = []
    now = datetime.now(timezone.utc).isoformat()

    for i, coin_id in enumerate(TRACKED_COINS):
        try:
            print(f"  📥 Extrayendo {coin_id} ({i + 1}/{len(TRACKED_COINS)})...")

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
                        print(f"  ⏳ Rate limited, esperando {wait}s (intento {attempt + 1})...")
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

            print(f"  ✅ {coin_id}: {len(prices)} datapoints")

            if i < len(TRACKED_COINS) - 1:
                time.sleep(6)

        except Exception as e:
            print(f"  ❌ {coin_id} falló: {e}")
            continue

    return all_records


def extract_fear_greed(days: int = 90) -> list[dict]:
    """Extrae el Fear & Greed Index histórico."""
    session = requests.Session()
    session.headers.update({"User-Agent": "CryptoLake/1.0"})
    now = datetime.now(timezone.utc).isoformat()

    print(f"  📥 Extrayendo Fear & Greed Index ({days} días)...")

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

    print(f"  ✅ Fear & Greed: {len(records)} datapoints")
    return records


# ================================================================
# FUNCIONES DE CARGA A ICEBERG
# ================================================================


def create_bronze_tables(spark: SparkSession):
    """
    Crea las tablas Iceberg en Bronze si no existen.

    USING iceberg: Le dice a Spark que use el formato Iceberg.
    PARTITIONED BY: Organiza los archivos Parquet por coin_id.
        Esto acelera las queries que filtran por coin (que serán la mayoría).
    TBLPROPERTIES: Configuración de la tabla Iceberg:
        - write.format.default = parquet: Formato de archivos subyacente
        - write.parquet.compression-codec = zstd: Compresión moderna y eficiente
    """
    print("\n🏗️  Creando tablas Bronze (si no existen)...")

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
    print("  ✅ cryptolake.bronze.historical_prices")

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
    print("  ✅ cryptolake.bronze.fear_greed")


def load_to_bronze(spark: SparkSession):
    """
    Extrae datos de las APIs y los carga en Iceberg Bronze.

    Flujo por tabla:
    1. Extraer datos de la API → lista de dicts
    2. Convertir a Spark DataFrame con schema tipado
    3. Añadir _loaded_at (timestamp de carga en el Lakehouse)
    4. Append a la tabla Iceberg (nunca sobrescribimos Bronze)
    """
    create_bronze_tables(spark)

    # ── Precios históricos ──────────────────────────────────
    print("\n📊 CARGANDO PRECIOS HISTÓRICOS")
    print("=" * 50)

    price_records = extract_coingecko(days=DAYS_TO_EXTRACT)

    if price_records:
        # Crear DataFrame con schema explícito
        prices_df = spark.createDataFrame(price_records, schema=BRONZE_HISTORICAL_SCHEMA)

        # Añadir timestamp de carga (cuándo entró al Lakehouse)
        prices_df = prices_df.withColumn("_loaded_at", current_timestamp())

        # Append a Bronze — NUNCA hacemos overwrite en Bronze.
        # Bronze es append-only: cada ejecución añade datos nuevos.
        # Si hay duplicados, los resolveremos en Silver.
        prices_df.writeTo("cryptolake.bronze.historical_prices").append()

        count = prices_df.count()
        print(f"\n  ✅ {count} registros cargados en bronze.historical_prices")
    else:
        print("  ⚠️  No se extrajeron precios")

    # ── Fear & Greed Index ──────────────────────────────────
    print("\n📊 CARGANDO FEAR & GREED INDEX")
    print("=" * 50)

    fg_records = extract_fear_greed(days=DAYS_TO_EXTRACT)

    if fg_records:
        fg_df = spark.createDataFrame(fg_records, schema=BRONZE_FEAR_GREED_SCHEMA)
        fg_df = fg_df.withColumn("_loaded_at", current_timestamp())
        fg_df.writeTo("cryptolake.bronze.fear_greed").append()

        count = fg_df.count()
        print(f"\n  ✅ {count} registros cargados en bronze.fear_greed")
    else:
        print("  ⚠️  No se extrajeron datos de Fear & Greed")


# ================================================================
# MAIN
# ================================================================

if __name__ == "__main__":
    print("=" * 60)
    print("🚀 CryptoLake — API to Bronze")
    print("=" * 60)

    # Crear SparkSession
    # SparkSession es el punto de entrada a toda la funcionalidad de Spark.
    # .appName() aparece en el Spark UI para identificar este job.
    # Las configuraciones de Iceberg y S3 ya están en spark-defaults.conf
    # (se cargan automáticamente), así que no necesitamos repetirlas aquí.
    spark = SparkSession.builder.appName("CryptoLake-APIToBronze").getOrCreate()

    try:
        load_to_bronze(spark)

        # ── Verificación ─────────────────────────────────────
        print("\n" + "=" * 60)
        print("📋 VERIFICACIÓN")
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

        # Verificar time travel de Iceberg (una de sus superpoderes)
        print("  📸 Snapshots de Iceberg (historial de versiones):")
        spark.sql("""
            SELECT snapshot_id, committed_at, operation
            FROM cryptolake.bronze.historical_prices.snapshots
        """).show(truncate=False)

    finally:
        spark.stop()

    print("\n✅ Bronze load completado!")
