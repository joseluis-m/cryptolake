# CryptoLake — Fase 5 y Fase 6

## Guía paso a paso: dbt + Airflow

> **Resumen**: En la Fase 5 añadimos dbt (data build tool) para crear la capa Gold de forma
> declarativa, testable y documentada. En la Fase 6 orquestamos todo el pipeline con Apache
> Airflow para que se ejecute automáticamente cada día.

---

## PARTE 7 (FASE 5): Gold Layer con dbt

### 7.1 — Conceptos fundamentales

**¿Qué es dbt?**

dbt (data build tool) es una herramienta que te permite transformar datos escribiendo solo
SQL. En lugar de programar un script PySpark completo con imports, SparkSession, funciones,
etc., simplemente escribes la consulta SQL y dbt se encarga de:

- Crear las tablas/vistas por ti (ejecuta `CREATE TABLE ... AS SELECT ...`)
- Gestionar dependencias entre modelos (si `fact_market_daily` necesita datos de `stg_prices`,
  dbt se asegura de ejecutar `stg_prices` primero)
- Ejecutar tests automáticos sobre los datos (¿hay nulls? ¿hay duplicados?)
- Generar documentación automática de tu data warehouse

**¿Por qué dbt si ya tenemos Spark?**

En la Fase 4 construiste la Gold layer con PySpark puro (`silver_to_gold.py`). Funcionaba, pero
tenías que escribir mucho código Python para cosas que son simplemente SQL. dbt es el estándar
de la industria para la "T" de ELT (Extract-Load-Transform). En entrevistas, saber dbt es un
diferenciador enorme.

La clave: **Spark procesa** (Bronze → Silver, deduplicación, limpieza pesada), **dbt modela**
(Silver → Gold, star schema, métricas de negocio). Cada herramienta para lo que hace mejor.

**¿Cómo conecta dbt con Spark?**

dbt necesita una forma de enviar SQL a Spark. Para eso usamos el **Spark Thrift Server**
(también llamado HiveServer2). Es un servicio que expone Spark SQL como un servidor JDBC al que
dbt se conecta, envía consultas SQL, y Spark las ejecuta sobre las tablas Iceberg.

```
dbt (SQL) ──JDBC──▶ Spark Thrift Server ──▶ Spark SQL ──▶ Iceberg Tables (MinIO)
```

**Estructura de un proyecto dbt:**

```
dbt_cryptolake/
├── dbt_project.yml       ← Configuración del proyecto (nombre, carpetas, materialización)
├── profiles.yml          ← Conexión al servidor (host, puerto, schema)
├── models/
│   ├── sources.yml       ← Define de dónde lee dbt (tablas Silver en Iceberg)
│   ├── staging/          ← Capa de interfaz: renombra, limpia, calcula campos básicos
│   │   ├── stg_prices.sql
│   │   └── stg_fear_greed.sql
│   └── marts/            ← Capa de negocio: star schema (dimensiones + facts)
│       ├── dim_coins.sql
│       ├── dim_dates.sql
│       ├── fact_market_daily.sql
│       └── schema.yml    ← Tests y documentación de los modelos
├── macros/               ← Funciones SQL reutilizables (personalizaciones)
│   ├── generate_schema_name.sql
│   └── create_table_as.sql
└── tests/                ← Tests SQL personalizados
    └── assert_positive_prices.sql
```

**Materialización** — cómo dbt crea cada modelo:

- `view` (staging): Crea una vista SQL. No almacena datos, es solo una consulta guardada.
  Ideal para staging porque se actualiza siempre con los datos más recientes.
- `table` (dimensiones): Crea una tabla física. Se recrea completa en cada ejecución.
  Ideal para dimensiones que son pequeñas.
- `incremental` (facts): Solo procesa los datos nuevos y los añade/actualiza. Mucho más
  eficiente para tablas grandes que crecen cada día.
- `ephemeral`: No crea nada en la base de datos. Se convierte en un CTE que se incrusta
  en los modelos que lo usan. Útil para transformaciones intermedias.

---

### 7.2 — Añadir Spark Thrift Server al Docker Compose

El Thrift Server es un proceso Spark que escucha conexiones JDBC en el puerto 10000.
Usa la misma configuración de Iceberg que el Spark Master, así que puede leer las tablas
Bronze y Silver directamente.

Abre `docker-compose.yml` y añade este servicio **después del bloque `spark-worker`** y
**antes de `airflow-postgres`**:

```yaml
  # ============================================================
  # Spark Thrift Server: Punto de entrada JDBC para dbt y SQL tools.
  # Permite ejecutar consultas SQL sobre Iceberg desde fuera de Spark.
  # Es como un "puente" entre herramientas SQL (dbt, DBeaver, etc.)
  # y el motor de procesamiento Spark.
  # ============================================================
  spark-thrift:
    image: cryptolake-spark
    container_name: cryptolake-spark-thrift
    ports:
      - "10000:10000"      # Puerto JDBC — dbt se conecta aquí
    environment:
      # Sin este flag, el proceso arranca en background y Docker lo mata
      SPARK_NO_DAEMONIZE: "true"
    volumes:
      # Montamos el código fuente para que Spark pueda acceder a los scripts
      - ./src:/opt/spark/work/src:ro
      # Configuración de Spark con Iceberg
      - ./spark-defaults.conf:/opt/spark/conf/spark-defaults.conf:ro
    depends_on:
      spark-master:
        condition: service_healthy
    # Arranca el Thrift Server conectado al Spark Master.
    # --hiveconf configura el puerto y la dirección de escucha.
    command: >
      /opt/spark/sbin/start-thriftserver.sh
        --master spark://spark-master:7077
        --hiveconf hive.server2.thrift.port=10000
        --hiveconf hive.server2.thrift.bind.host=0.0.0.0
    networks:
      - default
```

**Importante**: Este servicio usa la imagen `cryptolake-spark` (la que ya construyes con tu
Dockerfile de Spark). Pero para que Docker la encuentre por nombre, necesitas añadir
`image: cryptolake-spark` al servicio `spark-master` también. Busca el bloque del `spark-master`
en tu docker-compose.yml y añade la línea `image`:

```yaml
  spark-master:
    build:
      context: ./docker/spark
      dockerfile: Dockerfile
    image: cryptolake-spark          # ← AÑADE esta línea
    container_name: cryptolake-spark-master
    # ... resto igual ...
```

Esto hace que cuando Docker construya la imagen del spark-master, la etiquete como
`cryptolake-spark`, y el servicio `spark-thrift` pueda reutilizarla sin reconstruir.

Ahora reconstruye y arranca:

```bash
docker compose down
docker compose up -d --build
```

Verifica que el Thrift Server está corriendo:

```bash
docker logs cryptolake-spark-thrift 2>&1 | tail -5
```

Deberías ver algo como `ThriftCLIService started on port 10000`. También puedes verificar
que el puerto está escuchando:

```bash
# Desde tu Mac, comprueba que el puerto 10000 está abierto
nc -zv localhost 10000
```

---

### 7.3 — Instalar dbt-spark en tu entorno local

dbt necesita el paquete `dbt-spark` con el conector PyHive (que habla el protocolo
Thrift/JDBC). Activa tu entorno virtual e instala:

```bash
cd ~/Projects/cryptolake

# Activa el entorno virtual
source .venv/bin/activate

# Instala dbt-spark con el conector PyHive
pip install "dbt-spark[PyHive]==1.8.0"
```

Esto instala `dbt-core`, `dbt-spark`, `PyHive`, y todas sus dependencias.

Verifica la instalación:

```bash
dbt --version
```

Deberías ver algo como:
```
Core:
  - installed: 1.8.x
Plugins:
  - spark: 1.8.0
```

---

### 7.4 — Crear la estructura del proyecto dbt

```bash
# Crear la carpeta del proyecto dbt dentro de src/transformation/
mkdir -p src/transformation/dbt_cryptolake/{models/{staging,marts},macros,tests}
```

Tu estructura debería quedar:

```
src/transformation/dbt_cryptolake/
├── dbt_project.yml         (lo creamos ahora)
├── profiles.yml            (lo creamos ahora)
├── models/
│   ├── sources.yml         (lo creamos ahora)
│   ├── staging/
│   │   ├── stg_prices.sql
│   │   └── stg_fear_greed.sql
│   └── marts/
│       ├── dim_coins.sql
│       ├── dim_dates.sql
│       ├── fact_market_daily.sql
│       └── schema.yml
├── macros/
│   ├── generate_schema_name.sql
│   └── create_table_as.sql
└── tests/
    └── assert_positive_prices.sql
```

---

### 7.5 — Configurar dbt_project.yml y profiles.yml

**dbt_project.yml** — la configuración central del proyecto:

```bash
cat > src/transformation/dbt_cryptolake/dbt_project.yml << 'EOF'
# ============================================================
# dbt_project.yml — Configuración del proyecto dbt
# ============================================================
# Este archivo define:
# - Nombre y versión del proyecto
# - Dónde buscar modelos, tests, macros, etc.
# - Cómo materializar cada carpeta de modelos
#
# Documentación: https://docs.getdbt.com/reference/dbt_project.yml
# ============================================================

name: cryptolake
version: '1.0.0'
config-version: 2

# "profile" conecta este proyecto con una configuración de conexión
# en profiles.yml. Debe coincidir exactamente.
profile: cryptolake

# Rutas donde dbt busca cada tipo de archivo
model-paths: ["models"]
test-paths: ["tests"]
macro-paths: ["macros"]
seed-paths: ["seeds"]
analysis-paths: ["analyses"]

# Carpetas que dbt limpia con "dbt clean"
clean-targets:
  - target        # Carpeta donde dbt genera los SQL compilados
  - dbt_packages  # Paquetes externos instalados

# ============================================================
# Configuración de materialización por carpeta
# ============================================================
# Cada carpeta dentro de models/ tiene su propia estrategia.
# El prefijo "+" significa "aplica a todos los modelos en esta carpeta".
models:
  cryptolake:
    # staging/ → Vistas SQL (no almacenan datos, siempre frescas)
    staging:
      +materialized: view
      +schema: staging

    # marts/ → Tablas físicas (star schema para análisis)
    marts:
      +materialized: table
      +schema: gold

# ============================================================
# Hooks que se ejecutan al inicio de cada "dbt run"
# ============================================================
# Creamos los namespaces de Iceberg si no existen.
# Sin esto, dbt fallaría al intentar crear tablas en un namespace
# que no existe.
on-run-start:
  - "CREATE NAMESPACE IF NOT EXISTS staging"
  - "CREATE NAMESPACE IF NOT EXISTS gold"
EOF
```

**profiles.yml** — la conexión al Spark Thrift Server:

```bash
cat > src/transformation/dbt_cryptolake/profiles.yml << 'EOF'
# ============================================================
# profiles.yml — Configuración de conexión de dbt
# ============================================================
# Define CÓMO dbt se conecta a Spark Thrift Server.
# Hay dos "targets" (entornos):
#   - dev:  desde tu Mac (localhost:10000)
#   - prod: desde dentro de Docker (spark-thrift:10000)
#
# El target "dev" es el que usarás ahora para desarrollo.
# El target "prod" lo usará Airflow en la Fase 6.
# ============================================================

cryptolake:
  target: dev

  outputs:
    # Desarrollo: ejecutas dbt desde tu Mac
    dev:
      type: spark
      method: thrift
      host: localhost
      port: 10000
      schema: gold
      # threads: cuántas consultas SQL ejecuta dbt en paralelo.
      # 1 es seguro para desarrollo local.
      threads: 1

    # Producción: Airflow ejecuta dbt desde dentro de Docker
    prod:
      type: spark
      method: thrift
      host: spark-thrift
      port: 10000
      schema: gold
      threads: 2
EOF
```

Verifica que dbt puede conectar:

```bash
cd src/transformation/dbt_cryptolake
dbt debug --profiles-dir .
```

Deberías ver `All checks passed!` al final. Si falla la conexión, asegúrate de que
`cryptolake-spark-thrift` está corriendo (`docker ps | grep thrift`).

---

### 7.6 — Crear sources.yml (origen de datos)

Los "sources" le dicen a dbt dónde están los datos de entrada. En nuestro caso, las tablas
Silver de Iceberg que creamos en la Fase 4.

```bash
cat > src/transformation/dbt_cryptolake/models/sources.yml << 'EOF'
# ============================================================
# sources.yml — Definición de datos de entrada para dbt
# ============================================================
# Un "source" en dbt es una tabla que ya existe en tu base de datos
# y que dbt NO crea ni gestiona. Solo la lee.
#
# En nuestro caso, las tablas Silver son creadas por los scripts
# de Spark (bronze_to_silver.py). dbt las lee para construir Gold.
#
# En los modelos SQL, las referenciamos así:
#   {{ source('silver', 'daily_prices') }}
#   → se traduce a: silver.daily_prices
#   → que Spark resuelve como: cryptolake.silver.daily_prices
# ============================================================

version: 2

sources:
  - name: silver
    description: "Capa Silver del Lakehouse — datos limpios y deduplicados"
    schema: silver
    tables:
      - name: daily_prices
        description: "Precios diarios por criptomoneda, deduplicados y validados"
        columns:
          - name: coin_id
            description: "Identificador único de la criptomoneda (ej: bitcoin, ethereum)"
          - name: price_date
            description: "Fecha del precio (tipo DATE)"
          - name: price_usd
            description: "Precio en dólares estadounidenses"
          - name: market_cap_usd
            description: "Capitalización de mercado en USD"
          - name: volume_24h_usd
            description: "Volumen de trading en las últimas 24 horas"
          - name: _processed_at
            description: "Timestamp de cuándo se procesó este registro en Silver"

      - name: fear_greed
        description: "Índice Fear & Greed del mercado crypto (0-100)"
        columns:
          - name: index_date
            description: "Fecha del índice"
          - name: fear_greed_value
            description: "Valor numérico (0=Extreme Fear, 100=Extreme Greed)"
          - name: classification
            description: "Clasificación textual del sentimiento"
          - name: _processed_at
            description: "Timestamp de procesamiento"
EOF
```

---

### 7.7 — Crear macros personalizadas

Necesitamos dos macros. La primera controla cómo dbt nombra los schemas. La segunda
resuelve el problema de LOCATION que vimos en la Fase 4 (que cada tabla acabe en su
bucket correcto).

**Macro 1: generate_schema_name**

Por defecto, dbt genera schemas como `{target_schema}_{custom_schema}`, es decir, si tu
target es `gold` y el modelo tiene `+schema: gold`, generaría `gold_gold`. Queremos que
use solo el `custom_schema` directamente.

```bash
cat > src/transformation/dbt_cryptolake/macros/generate_schema_name.sql << 'SQLEOF'
{#
  ============================================================
  Macro: generate_schema_name
  ============================================================
  Sobreescribe el comportamiento por defecto de dbt para
  nombrar schemas.

  Comportamiento por defecto de dbt:
    target.schema = "gold", custom_schema = "staging"
    → genera: "gold_staging"  (¡no es lo que queremos!)

  Nuestro comportamiento:
    custom_schema = "staging" → genera: "staging"
    custom_schema = null      → genera: target.schema ("gold")

  Esto hace que las tablas de staging vayan al namespace
  "cryptolake.staging" y las de marts al namespace "cryptolake.gold".
  ============================================================
#}

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
SQLEOF
```

**Macro 2: create_table_as (LOCATION para Iceberg)**

Esta macro sobreescribe cómo dbt crea tablas en Spark. Añade `LOCATION` para que
cada tabla aterrice en su bucket correcto de MinIO.

```bash
cat > src/transformation/dbt_cryptolake/macros/create_table_as.sql << 'SQLEOF'
{#
  ============================================================
  Macro: create_table_as (override para Spark + Iceberg)
  ============================================================
  Problema: El catálogo REST de Iceberg (tabulario) ignora el
  LOCATION del namespace. Sin esta macro, todas las tablas que
  dbt crea irían al bucket por defecto (cryptolake-bronze).

  Solución: Inyectamos LOCATION explícito en cada CREATE TABLE
  apuntando al bucket correcto basándonos en el schema del modelo.

  Ejemplo para un modelo en schema "gold":
    CREATE OR REPLACE TABLE gold.dim_coins
    USING iceberg
    LOCATION 's3://cryptolake-gold/dim_coins'
    AS SELECT ...
  ============================================================
#}

{% macro spark__create_table_as(temporary, relation, compiled_code) -%}
  {# Construimos el nombre del bucket a partir del schema #}
  {%- set bucket = 'cryptolake-' ~ relation.schema -%}

  create or replace table {{ relation }}
  using iceberg
  location 's3://{{ bucket }}/{{ relation.identifier }}'
  as
  {{ compiled_code }}
{%- endmacro %}
SQLEOF
```

---

### 7.8 — Crear modelos Staging

Los modelos staging son la "interfaz limpia" sobre los datos Silver. Renombran columnas
si es necesario, aplican lógica de negocio mínima, y calculan campos básicos. Son vistas,
no tablas, así que no almacenan datos.

**stg_prices.sql:**

```bash
cat > src/transformation/dbt_cryptolake/models/staging/stg_prices.sql << 'SQLEOF'
-- ============================================================
-- Staging: stg_prices
-- ============================================================
-- Interfaz limpia sobre silver.daily_prices.
--
-- Añade dos campos calculados:
--   - prev_day_price: precio del día anterior (con LAG window function)
--   - price_change_pct_1d: cambio porcentual respecto al día anterior
--
-- LAG() es una window function que "mira hacia atrás" en los datos:
--   LAG(columna) OVER (PARTITION BY grupo ORDER BY orden)
--   = "dame el valor de 'columna' en la fila anterior dentro del mismo 'grupo'"
--
-- Ejemplo con Bitcoin:
--   Fecha       Precio    LAG(precio)  Change %
--   2024-01-01  42000     NULL         NULL (no hay día anterior)
--   2024-01-02  43000     42000        +2.38%
--   2024-01-03  41500     43000        -3.49%
-- ============================================================

WITH source AS (
    SELECT * FROM {{ source('silver', 'daily_prices') }}
),

with_lag AS (
    SELECT
        coin_id,
        price_date,
        price_usd,
        market_cap_usd,
        volume_24h_usd,
        _processed_at,

        -- LAG: precio del día anterior para esta misma moneda
        LAG(price_usd) OVER (
            PARTITION BY coin_id ORDER BY price_date
        ) AS prev_day_price

    FROM source
    WHERE price_usd > 0
)

SELECT
    *,
    -- Cálculo del cambio porcentual día a día
    -- Fórmula: ((nuevo - anterior) / anterior) * 100
    CASE
        WHEN prev_day_price IS NOT NULL AND prev_day_price > 0
        THEN ROUND(((price_usd - prev_day_price) / prev_day_price) * 100, 4)
        ELSE NULL
    END AS price_change_pct_1d
FROM with_lag
SQLEOF
```

**stg_fear_greed.sql:**

```bash
cat > src/transformation/dbt_cryptolake/models/staging/stg_fear_greed.sql << 'SQLEOF'
-- ============================================================
-- Staging: stg_fear_greed
-- ============================================================
-- Interfaz limpia sobre silver.fear_greed.
--
-- Añade un sentiment_score numérico para facilitar el análisis.
-- Es más fácil hacer AVG(sentiment_score) que contar strings.
--
-- Clasificación:
--   "Extreme Fear"  → 1  (pánico en el mercado)
--   "Fear"          → 2
--   "Neutral"       → 3
--   "Greed"         → 4
--   "Extreme Greed" → 5  (euforia, posible burbuja)
-- ============================================================

WITH source AS (
    SELECT * FROM {{ source('silver', 'fear_greed') }}
)

SELECT
    index_date,
    fear_greed_value,
    classification,

    -- Convertimos la clasificación textual a un score numérico
    CASE classification
        WHEN 'Extreme Fear' THEN 1
        WHEN 'Fear' THEN 2
        WHEN 'Neutral' THEN 3
        WHEN 'Greed' THEN 4
        WHEN 'Extreme Greed' THEN 5
    END AS sentiment_score,

    _processed_at

FROM source
SQLEOF
```

---

### 7.9 — Crear modelos Marts (Star Schema)

Aquí creamos el star schema dimensional (Kimball). Este es el modelo que los analistas
y dashboards consultarán. Tiene tres tablas: dos dimensiones y una fact table.

**dim_coins.sql** — Dimensión con estadísticas de cada criptomoneda:

```bash
cat > src/transformation/dbt_cryptolake/models/marts/dim_coins.sql << 'SQLEOF'
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
SQLEOF
```

**dim_dates.sql** — Dimensión calendario:

```bash
cat > src/transformation/dbt_cryptolake/models/marts/dim_dates.sql << 'SQLEOF'
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
SQLEOF
```

**fact_market_daily.sql** — Tabla de hechos central:

```bash
cat > src/transformation/dbt_cryptolake/models/marts/fact_market_daily.sql << 'SQLEOF'
-- ============================================================
-- Fact Table: fact_market_daily
-- ============================================================
-- Tabla de hechos central del star schema.
-- Granularidad: 1 fila = 1 moneda × 1 día
--
-- Contiene:
--   - Datos base: precio, market cap, volumen
--   - Métricas calculadas con window functions:
--     · Moving averages (7d, 30d)
--     · Volatilidad 7 días
--     · Señal MA30 (above/below)
--   - Datos de sentimiento del mercado (Fear & Greed)
--
-- Window Functions usadas:
-- ──────────────────────────────────────────────────────────
-- AVG(...) OVER (PARTITION BY coin ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
--
--   PARTITION BY coin: calcula por separado para cada moneda
--   ORDER BY date: ordena cronológicamente
--   ROWS BETWEEN 6 PRECEDING AND CURRENT ROW: ventana de 7 días
--                                              (6 anteriores + el actual)
--
-- Ejemplo visual para Bitcoin, ventana de 3 días:
--   Fecha       Precio    AVG(ventana 3d)
--   2024-01-01  42000     42000        ← solo 1 día disponible
--   2024-01-02  43000     42500        ← promedio de 2 días
--   2024-01-03  41500     42166.67     ← promedio de 3 días (42000+43000+41500)/3
--   2024-01-04  44000     42833.33     ← ventana se "desliza" (43000+41500+44000)/3
-- ============================================================

{{ config(
    materialized='table',
    unique_key=['coin_id', 'price_date']
) }}

WITH prices AS (
    SELECT * FROM {{ ref('stg_prices') }}
),

fear_greed AS (
    SELECT * FROM {{ ref('stg_fear_greed') }}
),

-- Calculamos todas las métricas con window functions
enriched AS (
    SELECT
        p.coin_id,
        p.price_date,
        p.price_usd,
        p.market_cap_usd,
        p.volume_24h_usd,
        p.price_change_pct_1d,

        -- ═══════════════════════════════════════════════════
        -- MEDIAS MÓVILES (Moving Averages)
        -- ═══════════════════════════════════════════════════
        -- Media de los últimos 7 días. Suaviza el ruido diario.
        -- Si el precio está por encima de la MA7, tendencia alcista a corto plazo.
        ROUND(AVG(p.price_usd) OVER (
            PARTITION BY p.coin_id
            ORDER BY p.price_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 2) AS moving_avg_7d,

        -- Media de los últimos 30 días. Indica tendencia de medio plazo.
        ROUND(AVG(p.price_usd) OVER (
            PARTITION BY p.coin_id
            ORDER BY p.price_date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ), 2) AS moving_avg_30d,

        -- ═══════════════════════════════════════════════════
        -- VOLATILIDAD (desviación estándar 7 días)
        -- ═══════════════════════════════════════════════════
        -- Mide cuánto fluctúa el precio. Alta volatilidad = más riesgo.
        -- STDDEV calcula la dispersión estadística del precio.
        ROUND(STDDEV(p.price_usd) OVER (
            PARTITION BY p.coin_id
            ORDER BY p.price_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 2) AS volatility_7d,

        -- Media de volumen 7 días (tendencia de actividad)
        ROUND(AVG(p.volume_24h_usd) OVER (
            PARTITION BY p.coin_id
            ORDER BY p.price_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 2) AS avg_volume_7d,

        -- ═══════════════════════════════════════════════════
        -- SENTIMIENTO DEL MERCADO (Fear & Greed Index)
        -- ═══════════════════════════════════════════════════
        -- LEFT JOIN porque no todos los días tienen dato de F&G
        fg.fear_greed_value,
        fg.classification AS market_sentiment,
        fg.sentiment_score,

        -- ═══════════════════════════════════════════════════
        -- SEÑAL MA30: ¿Precio por encima o debajo de la media 30d?
        -- ═══════════════════════════════════════════════════
        -- Señal técnica básica:
        --   ABOVE_MA30 = tendencia alcista (precio > media)
        --   BELOW_MA30 = tendencia bajista (precio < media)
        CASE
            WHEN p.price_usd > AVG(p.price_usd) OVER (
                PARTITION BY p.coin_id
                ORDER BY p.price_date
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ) THEN 'ABOVE_MA30'
            ELSE 'BELOW_MA30'
        END AS ma30_signal

    FROM prices p
    LEFT JOIN fear_greed fg
        ON p.price_date = fg.index_date
)

SELECT
    coin_id,
    price_date,
    price_usd,
    market_cap_usd,
    volume_24h_usd,
    price_change_pct_1d,
    moving_avg_7d,
    moving_avg_30d,
    volatility_7d,
    avg_volume_7d,
    fear_greed_value,
    market_sentiment,
    sentiment_score,
    ma30_signal,
    CURRENT_TIMESTAMP() AS _loaded_at
FROM enriched
SQLEOF
```

---

### 7.10 — Crear tests de datos

Los tests de dbt validan la calidad de los datos después de cada ejecución. Si un test
falla, dbt te lo dice y puedes investigar. Hay dos tipos de tests: genéricos (declarados
en YAML) y singulares (consultas SQL en la carpeta tests/).

**schema.yml** — Tests genéricos (declarativos):

```bash
cat > src/transformation/dbt_cryptolake/models/marts/schema.yml << 'SQLEOF'
# ============================================================
# schema.yml — Tests y documentación de los modelos marts
# ============================================================
# Los tests genéricos de dbt se declaran junto a cada columna.
# dbt genera y ejecuta la consulta SQL automáticamente.
#
# Tipos de tests genéricos:
#   - not_null: Verifica que la columna no tiene valores NULL
#   - unique: Verifica que no hay duplicados
#   - accepted_values: Verifica que los valores están en una lista
#   - relationships: Verifica integridad referencial (FK)
#
# Un test PASA si la consulta devuelve 0 filas.
# Un test FALLA si la consulta devuelve 1 o más filas.
# ============================================================

version: 2

models:
  - name: fact_market_daily
    description: "Tabla de hechos con métricas de mercado crypto diarias. Granularidad: 1 coin × 1 día."
    columns:
      - name: coin_id
        description: "Identificador de la criptomoneda"
        tests:
          - not_null
      - name: price_date
        description: "Fecha del registro"
        tests:
          - not_null
      - name: price_usd
        description: "Precio en USD"
        tests:
          - not_null

  - name: dim_coins
    description: "Dimensión con estadísticas agregadas por criptomoneda (SCD Type 1)"
    columns:
      - name: coin_id
        description: "Identificador único de la criptomoneda"
        tests:
          - unique
          - not_null
      - name: all_time_high
        tests:
          - not_null
      - name: total_days_tracked
        tests:
          - not_null

  - name: dim_dates
    description: "Dimensión calendario con atributos temporales"
    columns:
      - name: date_day
        description: "Fecha (clave primaria)"
        tests:
          - unique
          - not_null
SQLEOF
```

**Test singular — assert_positive_prices.sql:**

```bash
cat > src/transformation/dbt_cryptolake/tests/assert_positive_prices.sql << 'SQLEOF'
-- ============================================================
-- Test singular: assert_positive_prices
-- ============================================================
-- Verifica que no hay precios negativos o cero en la fact table.
-- Un precio <= 0 indicaría datos corruptos o un error en la ingesta.
--
-- Si esta consulta devuelve filas, el test FALLA.
-- ============================================================

SELECT
    coin_id,
    price_date,
    price_usd
FROM {{ ref('fact_market_daily') }}
WHERE price_usd <= 0
SQLEOF
```

---

### 7.11 — Ejecutar dbt

Ahora vamos a ejecutar dbt y ver cómo crea las tablas Gold.

```bash
# Asegúrate de estar en la carpeta del proyecto dbt
cd ~/Projects/cryptolake/src/transformation/dbt_cryptolake

# 1. Verificar que la conexión funciona
dbt debug --profiles-dir .

# 2. Ejecutar todos los modelos (staging → marts)
dbt run --profiles-dir .

# 3. Ejecutar los tests
dbt test --profiles-dir .
```

**¿Qué hace `dbt run`?**

1. Lee todos los archivos .sql en models/
2. Detecta las dependencias (fact_market_daily depende de stg_prices y stg_fear_greed)
3. Ejecuta en el orden correcto:
   - Primero: stg_prices y stg_fear_greed (vistas en namespace staging)
   - Después: dim_coins, dim_dates, fact_market_daily (tablas en namespace gold)
4. Cada modelo se ejecuta como SQL en el Spark Thrift Server

Deberías ver algo como:

```
Running with dbt=1.8.x
Found 5 models, 7 tests, 2 sources

Concurrency: 1 threads (target='dev')

1 of 5 START sql view model staging.stg_prices ................ [RUN]
1 of 5 OK created sql view model staging.stg_prices ........... [OK in 2.34s]
2 of 5 START sql view model staging.stg_fear_greed ............ [RUN]
2 of 5 OK created sql view model staging.stg_fear_greed ....... [OK in 1.12s]
3 of 5 START sql table model gold.dim_coins ................... [RUN]
3 of 5 OK created sql table model gold.dim_coins .............. [OK in 4.56s]
4 of 5 START sql table model gold.dim_dates ................... [RUN]
4 of 5 OK created sql table model gold.dim_dates .............. [OK in 3.21s]
5 of 5 START sql table model gold.fact_market_daily ........... [RUN]
5 of 5 OK created sql table model gold.fact_market_daily ...... [OK in 8.45s]

Finished running 2 view models, 3 table models in 22.34s.
Completed successfully

Done. PASS=5 WARN=0 ERROR=0 SKIP=0 TOTAL=5
```

Y para `dbt test`:

```
Running with dbt=1.8.x
Found 5 models, 7 tests, 2 sources

Concurrency: 1 threads (target='dev')

1 of 7 START test not_null_dim_coins_coin_id .................. [PASS in 1.23s]
2 of 7 START test unique_dim_coins_coin_id .................... [PASS in 1.45s]
...
7 of 7 START test assert_positive_prices ...................... [PASS in 2.34s]

Finished running 7 tests in 15.67s.
Completed successfully

Done. PASS=7 WARN=0 ERROR=0 SKIP=0 TOTAL=7
```

---

### 7.12 — Verificar los resultados

Comprueba que las tablas se crearon correctamente en Spark SQL:

```bash
# Abrir PySpark shell
make spark-shell
```

Dentro del shell:

```python
# Ver las tablas Gold creadas por dbt
spark.sql("SELECT * FROM cryptolake.gold.fact_market_daily LIMIT 5").show()
spark.sql("SELECT * FROM cryptolake.gold.dim_coins").show()
spark.sql("SELECT * FROM cryptolake.gold.dim_dates LIMIT 5").show()

# Contar registros
spark.sql("SELECT count(*) FROM cryptolake.gold.fact_market_daily").show()

# Ver las vistas staging
spark.sql("SELECT * FROM cryptolake.staging.stg_prices LIMIT 3").show()

# Consulta analítica de ejemplo: top coins por volatilidad
spark.sql("""
    SELECT coin_id, 
           ROUND(AVG(volatility_7d), 2) as avg_volatility,
           ROUND(AVG(price_usd), 2) as avg_price
    FROM cryptolake.gold.fact_market_daily
    GROUP BY coin_id
    ORDER BY avg_volatility DESC
""").show()

exit()
```

También verifica en MinIO (http://localhost:9001) que las tablas Gold están en el bucket
`cryptolake-gold` gracias a la macro `create_table_as` que añadimos.

---

### 7.13 — Actualizar Makefile y hacer commit

Añade estas reglas al final de tu `Makefile`:

```makefile
# ── dbt ─────────────────────────────────────────────────────
dbt-run: ## Ejecutar modelos dbt (staging → gold)
	cd src/transformation/dbt_cryptolake && dbt run --profiles-dir .

dbt-test: ## Ejecutar tests dbt
	cd src/transformation/dbt_cryptolake && dbt test --profiles-dir .

dbt-all: ## Ejecutar dbt run + test
	$(MAKE) dbt-run
	$(MAKE) dbt-test
```

Actualiza también la regla `pipeline` para que use dbt en lugar de `silver_to_gold.py`:

```makefile
pipeline: ## Ejecutar pipeline completo: Bronze → Silver → Gold (dbt)
	@echo "🚀 Ejecutando pipeline completo..."
	$(MAKE) bronze-load
	$(MAKE) silver-transform
	$(MAKE) dbt-run
	$(MAKE) dbt-test
	@echo "✅ Pipeline completado!"
```

Commit:

```bash
cd ~/Projects/cryptolake
git add .
git commit -m "feat: dbt Gold layer with star schema and data tests

- Spark Thrift Server added for JDBC connectivity
- dbt project with staging views + mart tables
- Star schema: dim_coins, dim_dates, fact_market_daily
- Window functions: MA7, MA30, volatility, price change %
- Custom macros: generate_schema_name, create_table_as (Iceberg LOCATION)
- Data quality tests: not_null, unique, assert_positive_prices
- Dual targets: dev (local) and prod (Docker/Airflow)"
```

---

## PARTE 8 (FASE 6): Orquestación con Apache Airflow

### 8.1 — Conceptos fundamentales

**¿Qué es Airflow?**

Apache Airflow es un orquestador de workflows. Piensa en él como un "director de orquesta"
que coordina cuándo y en qué orden se ejecuta cada pieza de tu pipeline de datos.

Sin Airflow, tendrías que ejecutar manualmente `make pipeline` cada día. Con Airflow,
defines que el pipeline se ejecute automáticamente a las 06:00 UTC, y si algo falla,
reintenta 2 veces, envía una alerta, y muestra exactamente dónde falló.

**Conceptos clave:**

- **DAG** (Directed Acyclic Graph): Un "grafo" que define tu workflow. "Dirigido" porque las
  flechas van en una dirección (A → B → C). "Acíclico" porque no hay ciclos (A → B → A no
  está permitido). En la práctica, es un archivo Python que define tareas y sus dependencias.

- **Task**: Una unidad de trabajo individual. Por ejemplo, "extraer datos de CoinGecko" o
  "ejecutar dbt run". Cada task es independiente y tiene su propio estado (success, failed,
  running, etc.).

- **TaskGroup**: Agrupa tareas relacionadas visualmente en la UI de Airflow. No cambia la
  ejecución, solo la organización. Es como una carpeta para tareas.

- **Operators**: El "tipo" de tarea. Cada operator sabe ejecutar un tipo de trabajo específico:
  - `BashOperator`: Ejecuta un comando bash
  - `PythonOperator`: Ejecuta una función Python
  - `SparkSubmitOperator`: Envía un job a Spark (no lo usaremos por complejidad)

- **Schedule**: Cuándo se ejecuta el DAG. Usa sintaxis cron:
  - `"0 6 * * *"` = cada día a las 06:00
  - `"0 */4 * * *"` = cada 4 horas
  - `None` = solo ejecución manual (trigger)

**Nuestro DAG Master — CryptoLake Full Pipeline:**

```
TaskGroup: ingestion         TaskGroup: bronze       TaskGroup: silver
┌──────────────────────┐    ┌─────────────────┐    ┌──────────────────┐
│ extract_coingecko    │    │                 │    │                  │
│         +            │───▶│ api_to_bronze   │───▶│ bronze_to_silver │
│ extract_fear_greed   │    │                 │    │                  │
└──────────────────────┘    └─────────────────┘    └──────────────────┘
                                                            │
                    TaskGroup: quality      TaskGroup: gold  │
                    ┌────────────────┐    ┌───────────────┐  │
                    │                │◀───│  dbt_run      │◀─┘
                    │ quality_check  │    │     +         │
                    │                │    │  dbt_test     │
                    └────────────────┘    └───────────────┘
```

---

### 8.2 — Actualizar el Dockerfile de Airflow

Airflow necesita poder hacer dos cosas nuevas:

1. **Ejecutar spark-submit en el contenedor Spark** → Necesita Docker CLI
2. **Ejecutar dbt** → Necesita dbt-spark instalado

Reemplaza el archivo `docker/airflow/Dockerfile` completo:

```bash
cat > docker/airflow/Dockerfile << 'DOCKERFILE'
# ============================================================
# Apache Airflow para CryptoLake
# ============================================================
# Incluye:
# - Providers de Airflow para Spark
# - Docker CLI para ejecutar spark-submit en contenedores Spark
# - dbt-spark para la transformación Gold
# - Dependencias Python del proyecto
# ============================================================
FROM apache/airflow:2.9.3-python3.11

# ── Instalar como root ──────────────────────────────────────
USER root

# Docker CLI: para poder ejecutar "docker exec" en el contenedor de Spark.
# Esto es un patrón común en entornos de desarrollo local donde Airflow
# orquesta contenedores Docker hermanos (sibling containers).
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    docker.io \
    && rm -rf /var/lib/apt/lists/*

# Añadir el usuario airflow al grupo docker para que pueda
# usar el Docker socket sin ser root
RUN groupadd -f docker && usermod -aG docker airflow

# ── Instalar paquetes Python como airflow ────────────────────
USER airflow

RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark==4.7.1 \
    "dbt-spark[PyHive]==1.8.0" \
    requests==2.31.0 \
    pydantic==2.5.0 \
    pydantic-settings==2.1.0 \
    structlog==24.1.0
DOCKERFILE
```

---

### 8.3 — Actualizar docker-compose.yml para Airflow

Necesitamos dos cambios en los servicios de Airflow:

1. **Montar el Docker socket** — para que Airflow pueda ejecutar `docker exec` en el
   contenedor Spark. El Docker socket (`/var/run/docker.sock`) es el canal de comunicación
   con el Docker daemon.

2. **Montar el proyecto dbt** — ya tenemos `./src:/opt/airflow/src` montado, así que
   el proyecto dbt ya es accesible en `/opt/airflow/src/transformation/dbt_cryptolake`.

Busca los servicios `airflow-webserver` y `airflow-scheduler` en tu `docker-compose.yml`
y añade el Docker socket al bloque `volumes` de **ambos**:

```yaml
  # En airflow-webserver, sección volumes:
  airflow-webserver:
    # ... (build, container_name, ports, environment igual que antes) ...
    volumes:
      - ./src/orchestration/dags:/opt/airflow/dags
      - ./src:/opt/airflow/src
      - airflow-logs:/opt/airflow/logs
      - /var/run/docker.sock:/var/run/docker.sock   # ← AÑADIR
    # ... resto igual ...

  # En airflow-scheduler, sección volumes:
  airflow-scheduler:
    # ... (build, container_name, environment igual que antes) ...
    volumes:
      - ./src/orchestration/dags:/opt/airflow/dags
      - ./src:/opt/airflow/src
      - airflow-logs:/opt/airflow/logs
      - /var/run/docker.sock:/var/run/docker.sock   # ← AÑADIR
    # ... resto igual ...
```

**Nota sobre permisos del Docker socket en macOS:**

En Docker Desktop para Mac, el socket se comparte automáticamente y los permisos suelen
funcionar sin problemas. Si al ejecutar el DAG ves un error de permisos
(`Permission denied: /var/run/docker.sock`), la solución rápida es añadir a ambos servicios
de Airflow:

```yaml
    # Solo si tienes problemas de permisos del socket:
    user: root
```

Esto hace que Airflow corra como root dentro del contenedor (seguro en desarrollo local,
no recomendado en producción).

---

### 8.4 — Crear el DAG Master

Este es el corazón de la orquestación. Un solo archivo Python que define todo el pipeline.

```bash
cat > src/orchestration/dags/dag_full_pipeline.py << 'PYEOF'
"""
DAG Master de CryptoLake.

Ejecuta el pipeline completo de datos diariamente:
1. Ingesta batch (CoinGecko + Fear & Greed via APIs)
2. Bronze load (APIs → Iceberg Bronze con Spark)
3. Silver processing (Bronze → Silver con Spark)
4. Gold transformation (Silver → Gold con dbt)
5. Data quality checks (dbt tests)

Schedule: Diario a las 06:00 UTC
Retry: 2 reintentos con 5 minutos entre cada uno
Timeout: 1 hora máximo por task

Ejecución manual: También se puede trigger desde la UI de Airflow.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup


# ================================================================
# Configuración por defecto para todas las tareas del DAG.
# Se puede sobreescribir en tareas individuales.
# ================================================================
default_args = {
    # Nombre del dueño (aparece en la UI de Airflow)
    "owner": "cryptolake",

    # depends_on_past=False: cada ejecución es independiente.
    # Si ayer falló, hoy se ejecuta igualmente.
    "depends_on_past": False,

    # No enviar emails al fallar (requeriría configurar SMTP)
    "email_on_failure": False,

    # Si una tarea falla, reintenta 2 veces
    "retries": 2,

    # Espera 5 minutos entre reintentos
    "retry_delay": timedelta(minutes=5),

    # Si una tarea tarda más de 1 hora, se cancela
    "execution_timeout": timedelta(hours=1),
}


# ================================================================
# Definición del DAG
# ================================================================
# "with DAG(...) as dag:" es un context manager de Python.
# Todo lo que definamos dentro pertenece a este DAG.
# ================================================================
with DAG(
    # ID único del DAG (aparece en la UI de Airflow)
    dag_id="cryptolake_full_pipeline",

    default_args=default_args,

    description="Pipeline completo: Ingesta → Bronze → Silver → Gold → Quality",

    # Schedule en formato cron: "minuto hora día mes día_semana"
    # "0 6 * * *" = a las 06:00, todos los días, todos los meses
    schedule="0 6 * * *",

    # Fecha desde la que Airflow consideraría ejecutar este DAG.
    # Con catchup=False, NO ejecuta las fechas pasadas.
    start_date=datetime(2025, 1, 1),

    # catchup=False: No ejecutar retroactivamente para fechas pasadas.
    # Si activamos el DAG hoy, solo se ejecuta hoy, no intenta
    # ejecutar todos los días desde start_date.
    catchup=False,

    # Tags para filtrar en la UI de Airflow
    tags=["cryptolake", "production"],

    # doc_md: la docstring de este archivo aparece como documentación
    # del DAG en la UI de Airflow
    doc_md=__doc__,

) as dag:

    # ════════════════════════════════════════════════════════════
    # GRUPO 1: INGESTA BATCH
    # ════════════════════════════════════════════════════════════
    # Descarga datos de las APIs externas.
    # CoinGecko y Fear & Greed se ejecutan en PARALELO (no hay
    # dependencia entre ellas — una no necesita a la otra).
    # ════════════════════════════════════════════════════════════
    with TaskGroup("ingestion", tooltip="Descarga datos de APIs externas") as ingestion_group:

        extract_coingecko = BashOperator(
            task_id="extract_coingecko",
            # Ejecutamos el extractor Python directamente en el contenedor de Airflow.
            # El módulo está montado en /opt/airflow/src/ via docker-compose volumes.
            bash_command=(
                "cd /opt/airflow && "
                "python -m src.ingestion.batch.coingecko_extractor"
            ),
        )

        extract_fear_greed = BashOperator(
            task_id="extract_fear_greed",
            bash_command=(
                "cd /opt/airflow && "
                "python -m src.ingestion.batch.fear_greed_extractor"
            ),
        )

        # No hay ">>" entre ellas = se ejecutan en paralelo

    # ════════════════════════════════════════════════════════════
    # GRUPO 2: BRONZE LOAD (APIs → Iceberg Bronze)
    # ════════════════════════════════════════════════════════════
    # Ejecuta spark-submit en el contenedor de Spark usando
    # "docker exec". Este patrón se llama "sibling containers":
    # Airflow usa el Docker socket para ejecutar comandos en
    # contenedores hermanos que comparten la misma red Docker.
    #
    # En producción se usaría KubernetesPodOperator, EMROperator,
    # o Livy, pero para desarrollo local esto es lo más simple.
    # ════════════════════════════════════════════════════════════
    with TaskGroup("bronze_load", tooltip="Cargar datos en Iceberg Bronze") as bronze_group:

        api_to_bronze = BashOperator(
            task_id="api_to_bronze",
            bash_command=(
                "docker exec cryptolake-spark-master "
                "/opt/spark/bin/spark-submit "
                "/opt/spark/work/src/processing/batch/api_to_bronze.py"
            ),
        )

    # ════════════════════════════════════════════════════════════
    # GRUPO 3: SILVER PROCESSING (Bronze → Silver)
    # ════════════════════════════════════════════════════════════
    # Deduplicación, limpieza y MERGE INTO. Todo con Spark.
    # ════════════════════════════════════════════════════════════
    with TaskGroup("silver_processing", tooltip="Limpiar y deduplicar en Silver") as silver_group:

        bronze_to_silver = BashOperator(
            task_id="bronze_to_silver",
            bash_command=(
                "docker exec cryptolake-spark-master "
                "/opt/spark/bin/spark-submit "
                "/opt/spark/work/src/processing/batch/bronze_to_silver.py"
            ),
        )

    # ════════════════════════════════════════════════════════════
    # GRUPO 4: GOLD TRANSFORMATION (dbt)
    # ════════════════════════════════════════════════════════════
    # dbt se ejecuta directamente en el contenedor de Airflow
    # (dbt-spark está instalado en el Dockerfile de Airflow).
    # Conecta al Spark Thrift Server via JDBC.
    #
    # Usamos --target prod para que dbt use la configuración
    # de producción (host: spark-thrift en vez de localhost).
    # ════════════════════════════════════════════════════════════
    with TaskGroup("gold_transformation", tooltip="Modelado dimensional con dbt") as gold_group:

        dbt_run = BashOperator(
            task_id="dbt_run",
            bash_command=(
                "cd /opt/airflow/src/transformation/dbt_cryptolake && "
                "dbt run --profiles-dir . --target prod"
            ),
        )

        dbt_test = BashOperator(
            task_id="dbt_test",
            bash_command=(
                "cd /opt/airflow/src/transformation/dbt_cryptolake && "
                "dbt test --profiles-dir . --target prod"
            ),
        )

        # dbt_test se ejecuta DESPUÉS de dbt_run
        dbt_run >> dbt_test

    # ════════════════════════════════════════════════════════════
    # GRUPO 5: DATA QUALITY
    # ════════════════════════════════════════════════════════════
    # Placeholder para Great Expectations (Fase 7).
    # Por ahora, los tests de dbt son nuestra validación de calidad.
    # ════════════════════════════════════════════════════════════
    with TaskGroup("data_quality", tooltip="Validación de calidad de datos") as quality_group:

        quality_check = BashOperator(
            task_id="quality_summary",
            bash_command='echo "✅ Data quality checks passed (dbt tests ran in gold_transformation group)"',
        )

    # ════════════════════════════════════════════════════════════
    # DEPENDENCIAS ENTRE GRUPOS
    # ════════════════════════════════════════════════════════════
    # El operador ">>" define el orden de ejecución:
    # ingestion → bronze → silver → gold → quality
    #
    # Esto se visualiza en la UI de Airflow como un grafo
    # de izquierda a derecha con flechas entre los grupos.
    # ════════════════════════════════════════════════════════════
    ingestion_group >> bronze_group >> silver_group >> gold_group >> quality_group
PYEOF
```

---

### 8.5 — Reconstruir y verificar

```bash
# Reconstruir imágenes con los cambios en el Dockerfile de Airflow
docker compose down
docker compose up -d --build

# Espera ~90 segundos para que todos los servicios arranquen
sleep 90

# Verificar que todos están running
make status
```

Verifica que tienes estos servicios nuevos o actualizados:
- `cryptolake-spark-thrift` → running (puerto 10000)
- `cryptolake-airflow-webserver` → running (puerto 8083)
- `cryptolake-airflow-scheduler` → running

---

### 8.6 — Activar y ejecutar el DAG en Airflow

1. Abre **http://localhost:8083** en tu navegador
2. Login con `admin` / `admin`
3. Verás el DAG `cryptolake_full_pipeline` en la lista (con tag "production")
4. El DAG está **pausado** por defecto (el toggle a la izquierda está en OFF)
5. **Antes de activarlo**, haz una ejecución manual de prueba:
   - Haz clic en el nombre del DAG para entrar en su vista detalle
   - Haz clic en el botón **"Trigger DAG"** (icono ▶️ arriba a la derecha)
   - Confirma la ejecución

6. Verás el DAG ejecutándose en la pestaña **"Graph"**:
   - Los nodos se ponen **verde oscuro** cuando están en ejecución
   - Se ponen **verde claro** cuando completan exitosamente
   - Se ponen **rojo** si fallan

7. Para ver los logs de una tarea específica:
   - Haz clic en el nodo de la tarea (ej: `bronze_load.api_to_bronze`)
   - Selecciona **"Log"** en el popup

8. Una vez que la ejecución manual funcione correctamente, activa el toggle
   para que se ejecute automáticamente según el schedule (diario a las 06:00 UTC).

**Troubleshooting común:**

Si `api_to_bronze` o `bronze_to_silver` fallan con error de Docker, verifica:

```bash
# ¿Puede Airflow acceder al Docker socket?
docker exec cryptolake-airflow-scheduler docker ps

# Si falla con "permission denied", añade "user: root" al servicio
# airflow-scheduler en docker-compose.yml y reconstruye
```

Si `dbt_run` falla con error de conexión, verifica:

```bash
# ¿Está el Thrift Server corriendo?
docker logs cryptolake-spark-thrift 2>&1 | tail -10

# ¿Puede Airflow llegar al Thrift Server?
docker exec cryptolake-airflow-scheduler \
    python -c "from pyhive import hive; conn = hive.connect('spark-thrift', 10000); print('OK')"
```

---

### 8.7 — Actualizar Makefile y hacer commit

Añade estas reglas al final de tu `Makefile`:

```makefile
# ── Airflow ─────────────────────────────────────────────────
airflow-trigger: ## Trigger manual del DAG completo en Airflow
	docker exec cryptolake-airflow-scheduler \
	    airflow dags trigger cryptolake_full_pipeline

airflow-status: ## Ver estado de la última ejecución del DAG
	docker exec cryptolake-airflow-scheduler \
	    airflow dags list-runs -d cryptolake_full_pipeline --limit 5
```

Commit:

```bash
cd ~/Projects/cryptolake
git add .
git commit -m "feat: Airflow orchestration with full pipeline DAG

- DAG master with 5 TaskGroups: ingestion → bronze → silver → gold → quality
- Spark jobs via docker exec (sibling container pattern)
- dbt Gold layer triggered from Airflow with --target prod
- Airflow Dockerfile updated with Docker CLI + dbt-spark
- Docker socket mounted for container orchestration
- Schedule: daily at 06:00 UTC with 2 retries"
```

---

## Resumen de lo implementado

### Fase 5 — dbt Gold Layer
- Spark Thrift Server como punto de entrada JDBC
- Proyecto dbt con staging (views) + marts (tables)
- Star schema: `dim_coins`, `dim_dates`, `fact_market_daily`
- Window functions: MA7, MA30, volatilidad, cambio porcentual
- Macros personalizadas: `generate_schema_name`, `create_table_as` (LOCATION para Iceberg)
- Tests automáticos de calidad de datos

### Fase 6 — Airflow Orchestration
- DAG master `cryptolake_full_pipeline` con 5 TaskGroups
- Ejecución diaria automática a las 06:00 UTC
- Spark jobs orquestados via Docker exec (patrón sibling containers)
- dbt ejecutado directamente desde Airflow con target prod
- Reintentos automáticos y timeouts configurados

### Pipeline completo ahora:

```
[APIs] → [Ingesta Python] → [Spark: Bronze] → [Spark: Silver] → [dbt: Gold] → [Tests]
   ↑          ↑                    ↑                 ↑               ↑            ↑
 CoinGecko  Airflow Task      spark-submit      spark-submit     dbt run      dbt test
 Fear&Greed  Group 1           Group 2           Group 3         Group 4      Group 4
```
