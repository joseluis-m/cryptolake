# CryptoLake — Fase 3 y Fase 4: Bronze → Silver → Gold

> **Punto de partida**: Tienes toda la infraestructura corriendo (Fase 1)
> y los extractores de datos funcionando (Fase 2).
>
> **Al terminar esta guía**: Tendrás datos reales almacenados en un Lakehouse
> con tres capas (Bronze → Silver → Gold), modelado dimensional con star schema,
> y métricas calculadas listas para servir a una API o dashboard.

---

## PARTE 5 (FASE 3): Capa Bronze — Datos crudos en Iceberg

### 5.1 — Conceptos: ¿Qué hacemos aquí y por qué?

En la Fase 2 extrajimos datos de CoinGecko y Fear & Greed, pero solo los
imprimimos por consola. Ahora necesitamos **persistirlos** en nuestro Lakehouse.

**El flujo es:**

```
CoinGecko API ──▶ Python Extractor ──▶ Spark ──▶ Iceberg Bronze (MinIO)
Fear & Greed  ──▶ Python Extractor ──▶ Spark ──▶ Iceberg Bronze (MinIO)
```

**¿Por qué usar Spark para cargar datos a Bronze?**

Podrías pensar: "¿No puedo guardar los datos directamente con Python?"
Sí, podrías usar PyIceberg, pero usar Spark tiene ventajas:

- Spark **valida el schema** automáticamente (rechaza datos malformados)
- Spark **gestiona las transacciones ACID** de Iceberg (no datos corruptos)
- Spark puede **particionar** los datos automáticamente
- Es el mismo motor que usarás para Silver y Gold (stack consistente)
- Es lo que usarás en producción real

**¿Qué es spark-submit?**

Es el comando para enviar un job a Spark. En vez de ejecutar `python script.py`
directamente, le dices a Spark: "ejecuta este script con toda tu infraestructura
distribuida". Spark se encarga de distribuir el trabajo, gestionar memoria, etc.

```bash
spark-submit mi_script.py
#             └── Spark ejecuta esto con acceso a todo el cluster
```

### 5.2 — Preparar el entorno Spark

Primero, necesitamos añadir los JARs de Kafka al contenedor de Spark
para poder leer de Kafka más adelante. Además necesitamos configurar
PYTHONPATH para que los scripts puedan importar módulos de `src/`.

Actualiza `docker/spark/Dockerfile`, **reemplazando todo el contenido**:

```dockerfile
FROM apache/spark:3.5.3-python3

USER root

RUN pip install --no-cache-dir \
    pyiceberg[s3fs]==0.7.1 \
    pyarrow==15.0.1 \
    kafka-python==2.0.2 \
    requests==2.31.0 \
    pydantic==2.5.0 \
    pydantic-settings==2.1.0 \
    structlog==24.1.0

# JARs de Iceberg
ENV ICEBERG_VERSION=1.5.2
RUN curl -L -o /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar \
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar" \
 && curl -L -o /opt/spark/jars/iceberg-aws-bundle-${ICEBERG_VERSION}.jar \
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar"

# JARs de Kafka para Spark Structured Streaming
# Estos permiten que Spark lea/escriba de Kafka directamente.
ENV SPARK_VERSION=3.5.3
ENV SCALA_VERSION=2.12
RUN curl -L -o /opt/spark/jars/spark-sql-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar \
    "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar" \
 && curl -L -o /opt/spark/jars/kafka-clients-3.6.1.jar \
    "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.6.1/kafka-clients-3.6.1.jar" \
 && curl -L -o /opt/spark/jars/spark-token-provider-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar \
    "https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-token-provider-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar" \
 && curl -L -o /opt/spark/jars/commons-pool2-2.12.0.jar \
    "https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar"

COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf

# PYTHONPATH: permite que los scripts hagan "from src.config import settings"
# /opt/spark/work es donde montamos el código del proyecto
ENV PYTHONPATH="/opt/spark/work:${PYTHONPATH}"

USER spark
```

Ahora reconstruye los contenedores de Spark:

```bash
cd ~/Projects/cryptolake
docker compose down
docker compose up -d --build
# Espera ~60s a que todo arranque
make status
```

Verifica que Spark sigue sano:

```bash
make spark-shell
```

```python
spark.sql("SHOW NAMESPACES IN cryptolake").show()
# Deberías ver bronze, silver, gold (los creaste en la Fase 1)
# Si no los ves:
spark.sql("CREATE NAMESPACE IF NOT EXISTS cryptolake.bronze")
spark.sql("CREATE NAMESPACE IF NOT EXISTS cryptolake.silver")
spark.sql("CREATE NAMESPACE IF NOT EXISTS cryptolake.gold")
exit()
```

### 5.3 — Script: Cargar datos de APIs en Iceberg Bronze

Este script hace todo el trabajo: extrae de las APIs, crea las tablas Iceberg
si no existen, y carga los datos. Es **autocontenido** — no depende de imports
externos para poder ejecutarse con `spark-submit` dentro de Docker.

```bash
cat > src/processing/batch/api_to_bronze.py << 'PYEOF'
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
import json
from datetime import datetime, timezone

import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.functions import current_timestamp, lit


# ================================================================
# CONFIGURACIÓN
# ================================================================
# Definimos todo aquí para que el script sea autocontenido.
# En producción usarías variables de entorno o un config service.

COINGECKO_BASE_URL = "https://api.coingecko.com/api/v3"
FEAR_GREED_URL = "https://api.alternative.me/fng/"
DAYS_TO_EXTRACT = 90

TRACKED_COINS = [
    "bitcoin", "ethereum", "solana", "cardano",
    "polkadot", "chainlink", "avalanche-2", "polygon-ecosystem-token",
]


# ================================================================
# SCHEMAS
# ================================================================
# StructType define la estructura de un DataFrame, como una CREATE TABLE.
# Es obligatorio definir schemas explícitos en data engineering — nunca
# dejes que Spark "infiera" el schema porque puede equivocarse y además
# no documentas qué esperas recibir.

BRONZE_HISTORICAL_SCHEMA = StructType([
    StructField("coin_id", StringType(), nullable=False),
    StructField("timestamp_ms", LongType(), nullable=False),
    StructField("price_usd", DoubleType(), nullable=False),
    StructField("market_cap_usd", DoubleType(), nullable=True),
    StructField("volume_24h_usd", DoubleType(), nullable=True),
    StructField("_ingested_at", StringType(), nullable=False),
    StructField("_source", StringType(), nullable=False),
])

BRONZE_FEAR_GREED_SCHEMA = StructType([
    StructField("value", IntegerType(), nullable=False),
    StructField("classification", StringType(), nullable=False),
    StructField("timestamp", LongType(), nullable=False),
    StructField("_ingested_at", StringType(), nullable=False),
    StructField("_source", StringType(), nullable=False),
])


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
            print(f"  📥 Extrayendo {coin_id} ({i+1}/{len(TRACKED_COINS)})...")
            
            max_retries = 3
            response = None
            
            for attempt in range(max_retries + 1):
                response = session.get(
                    f"{COINGECKO_BASE_URL}/coins/{coin_id}/market_chart",
                    params={"vs_currency": "usd", "days": str(days), "interval": "daily"},
                    timeout=30,
                )
                
                if response.status_code == 429:
                    if attempt < max_retries:
                        wait = 30 * (2 ** attempt)
                        print(f"  ⏳ Rate limited, esperando {wait}s (intento {attempt+1})...")
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
                    all_records.append({
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
                    })
            
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
            records.append({
                "value": value,
                "classification": entry["value_classification"],
                "timestamp": int(entry["timestamp"]),
                "_ingested_at": now,
                "_source": "fear_greed_index",
            })
    
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
    spark = (
        SparkSession.builder
        .appName("CryptoLake-APIToBronze")
        .getOrCreate()
    )
    
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
PYEOF
```

### 5.4 — Ejecutar la carga a Bronze

```bash
docker exec cryptolake-spark-master \
    /opt/spark/bin/spark-submit \
    /opt/spark/work/src/processing/batch/api_to_bronze.py
```

Esto tardará ~1-2 minutos (la mayor parte es extraer datos de CoinGecko con
los delays de rate limiting). Al final verás una verificación como:

```
📋 VERIFICACIÓN

  bronze.historical_prices:
+-------------------------+---------+-----------+-----------+
|coin_id                  |records  |min_price  |max_price  |
+-------------------------+---------+-----------+-----------+
|avalanche-2              |91       |20.15      |45.32      |
|bitcoin                  |91       |62100.0    |106000.0   |
|cardano                  |91       |0.32       |1.15       |
...

  bronze.fear_greed:
+----------------+----+
|classification  |days|
+----------------+----+
|Extreme Fear    |62  |
|Fear            |23  |
...
```

**¡Verifica en MinIO!** Abre http://localhost:9001, navega al bucket
`cryptolake-bronze`. Deberías ver directorios con archivos `.parquet`
dentro. Esos son los datos crudos de Bronze, gestionados por Iceberg.

### 5.5 — Añadir comandos al Makefile

Añade estas reglas al final de tu `Makefile`:

```makefile
bronze-load: ## Cargar datos de APIs a Bronze
	docker exec cryptolake-spark-master \
	    /opt/spark/bin/spark-submit \
	    /opt/spark/work/src/processing/batch/api_to_bronze.py

silver-transform: ## Transformar Bronze → Silver
	docker exec cryptolake-spark-master \
	    /opt/spark/bin/spark-submit \
	    /opt/spark/work/src/processing/batch/bronze_to_silver.py

gold-transform: ## Transformar Silver → Gold
	docker exec cryptolake-spark-master \
	    /opt/spark/bin/spark-submit \
	    /opt/spark/work/src/processing/batch/silver_to_gold.py

pipeline: ## Ejecutar pipeline completo: Bronze → Silver → Gold
	@echo "🚀 Ejecutando pipeline completo..."
	$(MAKE) bronze-load
	$(MAKE) silver-transform
	$(MAKE) gold-transform
	@echo "✅ Pipeline completado!"
```

### 5.6 — Commit de la Fase 3

```bash
cd ~/Projects/cryptolake
git add .
git commit -m "feat: Bronze layer - load API data into Iceberg tables

- Spark job to extract from CoinGecko and Fear & Greed APIs
- Iceberg tables with explicit schemas and zstd compression
- Partitioned by coin_id for query performance
- Append-only Bronze (raw data preservation)
- Kafka JARs added to Spark image for future streaming
- PYTHONPATH configured for module imports in Spark containers"
```

---

## PARTE 6 (FASE 4): Capa Silver y Capa Gold

### 6.1 — Conceptos: ¿Qué es Silver y por qué lo necesitamos?

Bronze tiene los datos crudos, pero tienen problemas:

- **Duplicados**: Si ejecutas `bronze-load` dos veces, tendrás datos duplicados
- **Tipos incorrectos**: El timestamp está en milisegundos (número), no como DATE
- **Nulls sin tratar**: Algunos campos pueden ser null sin que sepamos por qué
- **Sin métricas calculadas**: No tenemos price change %, moving averages, etc.

**Silver resuelve todo esto**:

```
BRONZE (crudo)                    SILVER (limpio)
───────────────                   ────────────────
timestamp_ms: 1708819200000  ──▶  price_date: 2024-02-25
Duplicados posibles          ──▶  Deduplicado (1 fila por coin+fecha)
Nulls sin control            ──▶  Nulls documentados y controlados
Append-only                  ──▶  MERGE INTO (upsert incremental)
```

**¿Qué es MERGE INTO?**

Es la operación SQL más importante de data engineering moderno. Combina
INSERT y UPDATE en una sola operación atómica:

```sql
MERGE INTO silver_table AS target
USING new_data AS source
ON target.id = source.id
WHEN MATCHED THEN UPDATE SET ...    -- Si ya existe: actualizar
WHEN NOT MATCHED THEN INSERT ...    -- Si es nuevo: insertar
```

Esto permite **procesamiento incremental**: cada ejecución solo procesa
datos nuevos o actualizados, sin reescribir toda la tabla.

### 6.2 — Conceptos: ¿Qué es Gold y el modelado dimensional?

Gold es la capa de **consumo** — datos listos para dashboards y APIs.
Aquí aplicamos **modelado dimensional** (metodología Kimball), que es
una de las habilidades más valoradas en data engineering.

**Star Schema** (esquema estrella):

```
                    ┌───────────────┐
                    │  dim_dates    │
                    │  ─────────    │
                    │  date_day     │◄──┐
                    │  year         │   │
                    │  month        │   │
                    │  is_weekend   │   │
                    └───────────────┘   │
                                       │
┌───────────────┐   ┌─────────────────────────────────┐
│  dim_coins    │   │       fact_market_daily          │
│  ──────────   │   │       ──────────────────         │
│  coin_id (PK) │◄──┤  coin_id (FK)                   │
│  first_date   │   │  price_date (FK)                 │
│  all_time_high│   │  price_usd                       │
│  avg_price    │   │  market_cap_usd                  │
└───────────────┘   │  volume_24h_usd                  │
                    │  price_change_pct_1d              │
                    │  moving_avg_7d                    │
                    │  moving_avg_30d                   │
                    │  volatility_7d                    │
                    │  fear_greed_value                 │
                    │  market_sentiment                 │
                    │  ma30_signal                      │
                    └─────────────────────────────────┘
```

**¿Por qué star schema?**

- **Fact table** (hechos): Contiene las métricas medibles (precio, volumen).
  Cada fila = 1 coin × 1 día. Es la tabla grande.
- **Dimension tables** (dimensiones): Contienen atributos descriptivos
  (nombre del coin, datos del calendario). Son tablas pequeñas de lookup.

Este modelo permite queries muy rápidas e intuitivas:

```sql
-- "Dame el precio medio de Bitcoin los fines de semana cuando hay Extreme Fear"
SELECT AVG(f.price_usd)
FROM fact_market_daily f
JOIN dim_dates d ON f.price_date = d.date_day
WHERE f.coin_id = 'bitcoin'
  AND d.is_weekend = true
  AND f.market_sentiment = 'Extreme Fear'
```

### 6.3 — Script: Bronze → Silver

```bash
cat > src/processing/batch/bronze_to_silver.py << 'PYEOF'
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
    count,
    current_timestamp,
    from_unixtime,
    lag,
    round as spark_round,
    row_number,
    when,
)
from pyspark.sql.window import Window


def create_silver_tables(spark: SparkSession):
    """Crea las tablas Silver si no existen."""
    print("\n🏗️  Creando tablas Silver...")
    
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
        "price_date",
        from_unixtime(col("timestamp_ms") / 1000).cast("date")
    )
    
    # 3. Deduplicar
    # Window function: para cada grupo (coin_id, price_date),
    # ordena por _loaded_at descendente (más reciente primero)
    # y asigna row_number. Nos quedamos solo con row_number = 1.
    dedup_window = Window.partitionBy("coin_id", "price_date").orderBy(
        col("_loaded_at").desc()
    )
    
    deduped_df = (
        typed_df
        .withColumn("_row_num", row_number().over(dedup_window))
        .filter(col("_row_num") == 1)
        .drop("_row_num")
    )
    
    # 4. Filtrar precios inválidos y limpiar nulls
    cleaned_df = (
        deduped_df
        .filter(col("price_usd") > 0)
        .withColumn(
            "market_cap_usd",
            when(col("market_cap_usd") > 0, col("market_cap_usd"))
        )
        .withColumn(
            "volume_24h_usd",
            when(col("volume_24h_usd") > 0, col("volume_24h_usd"))
        )
        .withColumn("_processed_at", current_timestamp())
        .select(
            "coin_id", "price_date", "price_usd",
            "market_cap_usd", "volume_24h_usd", "_processed_at"
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
        bronze_df
        .withColumn("index_date", from_unixtime(col("timestamp")).cast("date"))
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
PYEOF
```

### 6.4 — Ejecutar Bronze → Silver

```bash
make silver-transform
```

### 6.5 — Script: Silver → Gold (Modelado Dimensional)

Este es el script más complejo e importante del proyecto. Aquí aplicamos
modelado dimensional real y calculamos métricas avanzadas con window functions.

```bash
cat > src/processing/batch/silver_to_gold.py << 'PYEOF'
"""
Spark Batch Job: Silver → Gold (Modelo Dimensional)

Crea un star schema con:
- dim_coins: Dimensión con estadísticas de cada criptomoneda
- dim_dates: Dimensión calendario con atributos útiles para análisis
- fact_market_daily: Tabla de hechos con métricas diarias y señales técnicas

Las métricas calculadas incluyen:
- Price change % (day-over-day)
- Moving averages (7d, 30d)
- Volatilidad (desviación estándar 7d)
- Señal MA30 (precio por encima/debajo de media 30d)
- Sentimiento de mercado (Fear & Greed)

Ejecución:
    docker exec cryptolake-spark-master \
        /opt/spark/bin/spark-submit /opt/spark/work/src/processing/batch/silver_to_gold.py
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    current_timestamp,
    date_format,
    dayofmonth,
    dayofweek,
    lag,
    max as spark_max,
    min as spark_min,
    month,
    quarter,
    round as spark_round,
    stddev,
    weekofyear,
    when,
    year,
)
from pyspark.sql.window import Window


def build_dim_coins(spark: SparkSession):
    """
    Construye la dimensión dim_coins.
    
    Tipo: SCD Type 1 (Slowly Changing Dimension Type 1)
    Esto significa que cuando los datos cambian, simplemente sobrescribimos.
    No guardamos historial de cambios en la dimensión.
    
    ¿Cuándo usarías Type 2? Cuando necesitas saber el valor histórico.
    Por ejemplo, si un coin cambia de nombre, querrías saber cómo se
    llamaba cuando hiciste cierto análisis. Para nuestro caso, Type 1
    es suficiente porque los stats se recalculan cada día.
    """
    print("\n📐 Construyendo dim_coins...")
    
    spark.sql("""
        CREATE OR REPLACE TABLE cryptolake.gold.dim_coins
        USING iceberg
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
            
            -- Market cap (último valor conocido)
            ROUND(MAX(market_cap_usd), 2)       AS max_market_cap,
            
            -- Rango de precio (volatilidad histórica simplificada)
            ROUND(
                ((MAX(price_usd) - MIN(price_usd)) / MIN(price_usd)) * 100,
                2
            )                                   AS price_range_pct,
            
            current_timestamp()                 AS _loaded_at
            
        FROM cryptolake.silver.daily_prices
        GROUP BY coin_id
    """)
    
    count = spark.table("cryptolake.gold.dim_coins").count()
    print(f"  ✅ dim_coins: {count} coins")


def build_dim_dates(spark: SparkSession):
    """
    Construye la dimensión dim_dates (calendario).
    
    ¿Por qué una tabla de fechas?
    Porque "2024-02-25" es solo un dato. Pero para análisis necesitas
    saber: ¿es fin de semana? ¿qué trimestre? ¿qué mes?
    
    En producción, esta tabla se carga una vez y cubre varios años.
    Aquí la generamos dinámicamente desde las fechas que tenemos.
    """
    print("\n📅 Construyendo dim_dates...")
    
    spark.sql("""
        CREATE OR REPLACE TABLE cryptolake.gold.dim_dates
        USING iceberg
        AS
        SELECT DISTINCT
            price_date                              AS date_day,
            YEAR(price_date)                        AS year,
            MONTH(price_date)                       AS month,
            DAYOFMONTH(price_date)                  AS day_of_month,
            DAYOFWEEK(price_date)                   AS day_of_week,
            WEEKOFYEAR(price_date)                  AS week_of_year,
            QUARTER(price_date)                     AS quarter,
            
            -- Flags booleanos para análisis
            CASE
                WHEN DAYOFWEEK(price_date) IN (1, 7)
                THEN true ELSE false
            END                                     AS is_weekend,
            
            -- Nombres legibles
            DATE_FORMAT(price_date, 'EEEE')         AS day_name,
            DATE_FORMAT(price_date, 'MMMM')         AS month_name
            
        FROM cryptolake.silver.daily_prices
        ORDER BY date_day
    """)
    
    count = spark.table("cryptolake.gold.dim_dates").count()
    print(f"  ✅ dim_dates: {count} fechas")


def build_fact_market_daily(spark: SparkSession):
    """
    Construye la tabla de hechos fact_market_daily.
    
    Esta es la tabla más importante y compleja del star schema.
    Cada fila = 1 criptomoneda × 1 día, con todas las métricas.
    
    Window Functions utilizadas:
    
    LAG(): Accede a la fila anterior en la ventana.
        Uso: obtener el precio del día anterior para calcular % cambio.
    
    AVG() OVER (ROWS BETWEEN N PRECEDING AND CURRENT ROW):
        Media móvil de los últimos N+1 días.
        Uso: Moving Average 7d y 30d (indicadores técnicos clásicos).
    
    STDDEV() OVER (...):
        Desviación estándar sobre la ventana.
        Uso: Volatilidad — cuánto varía el precio.
    
    Todas las ventanas se particionan por coin_id y ordenan por price_date.
    Esto significa que los cálculos son INDEPENDIENTES por cada moneda.
    """
    print("\n📊 Construyendo fact_market_daily...")
    
    # Primero necesitamos leer Silver y registrar como vista temporal
    prices = spark.table("cryptolake.silver.daily_prices")
    fear_greed = spark.table("cryptolake.silver.fear_greed")
    
    prices.createOrReplaceTempView("s_prices")
    fear_greed.createOrReplaceTempView("s_fear_greed")
    
    spark.sql("""
        CREATE OR REPLACE TABLE cryptolake.gold.fact_market_daily
        USING iceberg
        PARTITIONED BY (coin_id)
        AS
        WITH price_metrics AS (
            SELECT
                p.coin_id,
                p.price_date,
                p.price_usd,
                p.market_cap_usd,
                p.volume_24h_usd,
                
                -- ══════════════════════════════════════════════
                -- PRICE CHANGE % (día sobre día)
                -- ══════════════════════════════════════════════
                -- LAG(price_usd, 1) devuelve el precio del día anterior.
                -- Fórmula: ((precio_hoy - precio_ayer) / precio_ayer) × 100
                ROUND(
                    (p.price_usd - LAG(p.price_usd, 1) OVER w_coin)
                    / LAG(p.price_usd, 1) OVER w_coin * 100,
                    4
                ) AS price_change_pct_1d,
                
                -- ══════════════════════════════════════════════
                -- MOVING AVERAGES (medias móviles)
                -- ══════════════════════════════════════════════
                -- MA7: Media de los últimos 7 días (6 anteriores + hoy)
                -- Se usa como indicador de tendencia a corto plazo.
                ROUND(
                    AVG(p.price_usd) OVER (
                        PARTITION BY p.coin_id ORDER BY p.price_date
                        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                    ),
                    6
                ) AS moving_avg_7d,
                
                -- MA30: Media de los últimos 30 días
                -- Indicador de tendencia a medio plazo.
                -- Cuando el precio cruza por encima de MA30 = señal alcista.
                ROUND(
                    AVG(p.price_usd) OVER (
                        PARTITION BY p.coin_id ORDER BY p.price_date
                        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
                    ),
                    6
                ) AS moving_avg_30d,
                
                -- ══════════════════════════════════════════════
                -- VOLATILIDAD (desviación estándar 7d)
                -- ══════════════════════════════════════════════
                -- Alta volatilidad = mucho riesgo/oportunidad.
                -- Bitcoin típicamente: 2-5% diario.
                -- Altcoins: 5-15% diario.
                ROUND(
                    STDDEV(p.price_usd) OVER (
                        PARTITION BY p.coin_id ORDER BY p.price_date
                        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                    ),
                    6
                ) AS volatility_7d,
                
                -- ══════════════════════════════════════════════
                -- VOLUME TREND (media de volumen 7d)
                -- ══════════════════════════════════════════════
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
        
        -- JOIN con Fear & Greed y añadir señales
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
            
            -- Fear & Greed del día
            fg.fear_greed_value,
            fg.classification AS market_sentiment,
            
            -- ══════════════════════════════════════════════
            -- SEÑAL MA30
            -- ══════════════════════════════════════════════
            -- Si el precio está por encima de la media de 30 días,
            -- la tendencia general es alcista. Si está por debajo,
            -- es bajista. Es uno de los indicadores más básicos
            -- pero más usados en trading.
            CASE
                WHEN pm.price_usd > pm.moving_avg_30d THEN 'ABOVE_MA30'
                WHEN pm.price_usd < pm.moving_avg_30d THEN 'BELOW_MA30'
                ELSE 'AT_MA30'
            END AS ma30_signal,
            
            -- ══════════════════════════════════════════════
            -- SEÑAL COMBINADA (precio + sentimiento)
            -- ══════════════════════════════════════════════
            -- Combina el indicador técnico (MA30) con el sentimiento
            -- del mercado. "Extreme Fear + BELOW_MA30" podría ser
            -- oportunidad de compra según la filosofía contrarian.
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
    print(f"  ✅ fact_market_daily: {count} registros")


if __name__ == "__main__":
    print("=" * 60)
    print("🚀 CryptoLake — Silver to Gold (Star Schema)")
    print("=" * 60)
    
    spark = SparkSession.builder.appName("CryptoLake-SilverToGold").getOrCreate()
    
    try:
        build_dim_coins(spark)
        build_dim_dates(spark)
        build_fact_market_daily(spark)
        
        # ════════════════════════════════════════════════════════
        # VERIFICACIÓN DEL STAR SCHEMA
        # ════════════════════════════════════════════════════════
        print("\n" + "=" * 60)
        print("📋 VERIFICACIÓN GOLD — Star Schema")
        print("=" * 60)
        
        # dim_coins
        print("\n── dim_coins ──")
        spark.sql("""
            SELECT coin_id, total_days_tracked,
                   all_time_low, all_time_high, price_range_pct
            FROM cryptolake.gold.dim_coins
            ORDER BY price_range_pct DESC
        """).show(truncate=False)
        
        # dim_dates sample
        print("── dim_dates (muestra) ──")
        spark.sql("""
            SELECT date_day, day_name, month_name, quarter, is_weekend
            FROM cryptolake.gold.dim_dates
            ORDER BY date_day DESC
            LIMIT 5
        """).show(truncate=False)
        
        # fact_market_daily — query analítica de ejemplo
        print("── fact_market_daily: Bitcoin últimos 7 días ──")
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
        
        # Query analítica avanzada: ¿Qué coins tienen señal de compra?
        print("── Señales de compra potenciales (últimos datos) ──")
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
    
    print("\n✅ Gold (Star Schema) completado!")
PYEOF
```

### 6.6 — Ejecutar Silver → Gold

```bash
make gold-transform
```

### 6.7 — Ejecutar el pipeline completo

Ahora prueba el pipeline entero de una vez:

```bash
make pipeline
```

Esto ejecuta secuencialmente: Bronze load → Silver transform → Gold transform.

### 6.8 — Explorar tu Lakehouse interactivamente

Abre PySpark y haz queries analíticas sobre tu star schema:

```bash
make spark-shell
```

```python
# Ver todas las tablas que has creado
spark.sql("SHOW TABLES IN cryptolake.bronze").show()
spark.sql("SHOW TABLES IN cryptolake.silver").show()
spark.sql("SHOW TABLES IN cryptolake.gold").show()

# ── Queries analíticas sobre el Star Schema ─────────────

# ¿Cuál es el coin con mayor volatilidad en los últimos 30 días?
spark.sql("""
    SELECT coin_id,
           ROUND(AVG(volatility_7d), 4) as avg_volatility,
           ROUND(AVG(price_change_pct_1d), 4) as avg_daily_change
    FROM cryptolake.gold.fact_market_daily
    WHERE price_date >= date_sub(current_date(), 30)
    GROUP BY coin_id
    ORDER BY avg_volatility DESC
""").show()

# ¿Cómo se comporta Bitcoin los fines de semana vs laborables?
spark.sql("""
    SELECT d.is_weekend,
           ROUND(AVG(f.price_change_pct_1d), 4) as avg_change_pct,
           COUNT(*) as num_days
    FROM cryptolake.gold.fact_market_daily f
    JOIN cryptolake.gold.dim_dates d ON f.price_date = d.date_day
    WHERE f.coin_id = 'bitcoin'
    GROUP BY d.is_weekend
""").show()

# Time travel: ver la tabla tal como era en un snapshot anterior
spark.sql("""
    SELECT snapshot_id, committed_at, operation, summary
    FROM cryptolake.gold.fact_market_daily.snapshots
""").show(truncate=False)

exit()
```

### 6.9 — Commit de la Fase 4

```bash
cd ~/Projects/cryptolake
git add .
git commit -m "feat: Silver + Gold layers with dimensional modeling

Silver layer:
- Bronze to Silver deduplication with ROW_NUMBER window function
- Type casting (timestamp_ms → DATE)
- Null handling and data validation
- Incremental MERGE INTO (upsert)

Gold layer (Star Schema):
- dim_coins: SCD Type 1 with price stats and ranges
- dim_dates: Calendar dimension with weekend flags
- fact_market_daily: Fact table with:
  - Day-over-day price change %
  - Moving averages (7d, 30d)
  - 7-day volatility (stddev)
  - MA30 signal (above/below trend)
  - Combined signal (price trend + market sentiment)
  - Fear & Greed integration

Pipeline automation via Makefile (make pipeline)"
```

---

## PARTE 7: Recapitulación

Tu Lakehouse ahora tiene tres capas funcionando con datos reales:

```
APIs (CoinGecko, Fear & Greed)
    │
    ▼
┌─────────────────────────────────────────────────────┐
│ BRONZE (cryptolake.bronze.*)                        │
│   historical_prices: ~728 registros (8 coins × 91d)│
│   fear_greed: ~90 registros                         │
│   Formato: Iceberg + Parquet + zstd                 │
│   Política: Append-only, datos crudos               │
└──────────────────┬──────────────────────────────────┘
                   │  Spark: dedup + clean + MERGE INTO
                   ▼
┌─────────────────────────────────────────────────────┐
│ SILVER (cryptolake.silver.*)                        │
│   daily_prices: ~728 registros (deduplicados)       │
│   fear_greed: ~90 registros (deduplicados)          │
│   Formato: Iceberg, particionado por coin_id        │
│   Política: MERGE (upsert incremental)              │
└──────────────────┬──────────────────────────────────┘
                   │  Spark SQL: window functions + JOINs
                   ▼
┌─────────────────────────────────────────────────────┐
│ GOLD (cryptolake.gold.*) — Star Schema              │
│   dim_coins: 8 registros (1 por coin)               │
│   dim_dates: ~91 registros (calendario)             │
│   fact_market_daily: ~728 registros con:            │
│     - price_change_pct, MA7, MA30, volatility       │
│     - Fear & Greed, señales técnicas                │
│   Formato: Iceberg, particionado por coin_id        │
│   Listo para: API REST, Dashboard, análisis         │
└─────────────────────────────────────────────────────┘
```

### Comandos rápidos

```bash
make bronze-load        # APIs → Bronze
make silver-transform   # Bronze → Silver
make gold-transform     # Silver → Gold
make pipeline           # Todo secuencial
make spark-shell        # Explorar datos interactivamente
```

### Próximos pasos: Fase 5 y 6

En las siguientes fases implementaremos:

1. **Fase 5**: Orquestación con Airflow (programar el pipeline para que
   se ejecute automáticamente cada día) y data quality con validaciones.

2. **Fase 6**: Serving — API REST con FastAPI y dashboard con Streamlit
   para visualizar las métricas del star schema.
