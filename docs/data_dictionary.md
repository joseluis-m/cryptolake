# Data Dictionary — CryptoLake

Documentación completa de cada campo en cada tabla del Lakehouse.

---

## Bronze Layer

### cryptolake.bronze.historical_prices

Datos raw de precios históricos desde CoinGecko API. Append-only.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `coin_id` | STRING | No | Identificador CoinGecko (ej: `bitcoin`, `ethereum`) |
| `timestamp_ms` | BIGINT | No | Timestamp Unix en milisegundos del datapoint |
| `price_usd` | DOUBLE | No | Precio en USD en el momento del timestamp |
| `market_cap_usd` | DOUBLE | Sí | Capitalización de mercado en USD |
| `volume_24h_usd` | DOUBLE | Sí | Volumen de trading en las últimas 24h en USD |
| `_ingested_at` | STRING | No | ISO timestamp de cuándo el extractor Python descargó los datos |
| `_source` | STRING | No | Origen de los datos (ej: `coingecko_market_chart`) |
| `_loaded_at` | TIMESTAMP | No | Timestamp de Spark de cuándo se escribió en Iceberg |

**Particionado por**: `coin_id`  
**Compresión**: zstd

### cryptolake.bronze.fear_greed

Datos raw del Fear & Greed Index desde Alternative.me API.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `value` | INT | No | Valor del índice (0-100). 0 = Extreme Fear, 100 = Extreme Greed |
| `classification` | STRING | No | Clasificación textual: Extreme Fear, Fear, Neutral, Greed, Extreme Greed |
| `timestamp` | BIGINT | No | Timestamp Unix en segundos del dato |
| `_ingested_at` | STRING | No | ISO timestamp de extracción |
| `_source` | STRING | No | Origen: `alternative_me_fng` |
| `_loaded_at` | TIMESTAMP | No | Timestamp de carga en Iceberg |

**Compresión**: zstd

---

## Silver Layer

### cryptolake.silver.daily_prices

Precios limpios, deduplicados, con un registro por coin/día.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `coin_id` | STRING | No | Identificador de la criptomoneda |
| `price_date` | DATE | No | Fecha del precio (convertida desde `timestamp_ms`) |
| `price_usd` | DOUBLE | No | Precio en USD (solo valores > 0) |
| `market_cap_usd` | DOUBLE | Sí | Market cap (null si el valor original era <= 0) |
| `volume_24h_usd` | DOUBLE | Sí | Volumen 24h (null si el valor original era <= 0) |
| `_processed_at` | TIMESTAMP | No | Timestamp de procesamiento Silver |

**Particionado por**: `coin_id`  
**Deduplicación**: `ROW_NUMBER() OVER (PARTITION BY coin_id, price_date ORDER BY _loaded_at DESC)`  
**Actualización**: MERGE INTO (upsert incremental)

### cryptolake.silver.fear_greed

Fear & Greed Index limpio, con fecha en formato DATE.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `index_date` | DATE | No | Fecha del índice (convertida desde `timestamp`) |
| `fear_greed_value` | INT | No | Valor del índice 0-100 |
| `classification` | STRING | No | Clasificación textual del sentimiento |
| `_processed_at` | TIMESTAMP | No | Timestamp de procesamiento |

**Actualización**: MERGE INTO

---

## Gold Layer (dbt models)

### cryptolake.gold.dim_coins

Dimensión de criptomonedas con estadísticas agregadas. SCD Type 1.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `coin_id` | STRING | No | **PK**. Identificador único de la criptomoneda |
| `first_tracked_date` | DATE | No | Primera fecha con datos |
| `last_tracked_date` | DATE | No | Última fecha con datos |
| `total_days_tracked` | INT | No | Número de días con datos |
| `all_time_low` | DOUBLE | No | Precio mínimo histórico |
| `all_time_high` | DOUBLE | No | Precio máximo histórico |
| `avg_price` | DOUBLE | No | Precio medio |
| `avg_daily_volume` | DOUBLE | Sí | Volumen medio diario |
| `price_range_pct` | DOUBLE | No | Rango de precio en % ((max-min)/min × 100) |
| `_loaded_at` | TIMESTAMP | No | Timestamp de generación por dbt |

**Materialización dbt**: `table` (recreada completa en cada run)

### cryptolake.gold.dim_dates

Dimensión calendario generada desde las fechas de los precios.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `date_day` | DATE | No | **PK**. Fecha del calendario |
| `year` | INT | No | Año |
| `month` | INT | No | Mes (1-12) |
| `day_of_month` | INT | No | Día del mes (1-31) |
| `day_of_week` | INT | No | Día de la semana (1=Domingo, 7=Sábado) |
| `week_of_year` | INT | No | Semana del año (1-52) |
| `quarter` | INT | No | Trimestre (1-4) |
| `is_weekend` | BOOLEAN | No | `true` si es sábado o domingo |
| `day_name` | STRING | No | Nombre del día (Monday, Tuesday, ...) |
| `month_name` | STRING | No | Nombre del mes (January, February, ...) |

**Materialización dbt**: `table`

### cryptolake.gold.fact_market_daily

Tabla de hechos central. Granularidad: 1 fila = 1 moneda × 1 día.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `coin_id` | STRING | No | **FK** → dim_coins.coin_id |
| `price_date` | DATE | No | **FK** → dim_dates.date_day |
| `price_usd` | DOUBLE | No | Precio de cierre del día |
| `market_cap_usd` | DOUBLE | Sí | Capitalización de mercado |
| `volume_24h_usd` | DOUBLE | Sí | Volumen de trading 24h |
| `price_change_pct_1d` | DOUBLE | Sí | Cambio % respecto al día anterior |
| `moving_avg_7d` | DOUBLE | Sí | Media móvil 7 días |
| `moving_avg_30d` | DOUBLE | Sí | Media móvil 30 días |
| `volatility_7d` | DOUBLE | Sí | Desviación estándar del precio (7 días) |
| `avg_volume_7d` | DOUBLE | Sí | Media de volumen 7 días |
| `fear_greed_value` | INT | Sí | Valor Fear & Greed del día (via LEFT JOIN) |
| `market_sentiment` | STRING | Sí | Clasificación del sentimiento |
| `sentiment_score` | INT | Sí | Score numérico 1-5 del sentimiento |
| `ma30_signal` | STRING | Sí | ABOVE_MA30 / BELOW_MA30 / AT_MA30 |
| `combined_signal` | STRING | Sí | Señal combinada precio + sentimiento |
| `_loaded_at` | TIMESTAMP | No | Timestamp de generación por dbt |

**Materialización dbt**: `table`  
**Unique key**: `[coin_id, price_date]`

---

## Quality Layer

### cryptolake.quality.check_results

Resultados de los quality checks (generada por `run_quality_checks.py`).

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `check_name` | STRING | No | Nombre del check (ej: `no_duplicates`) |
| `layer` | STRING | No | Capa validada: bronze, silver, gold |
| `table_name` | STRING | No | Nombre completo de la tabla |
| `status` | STRING | No | passed, failed, warning, error |
| `metric_value` | DOUBLE | Sí | Valor medido (ej: 0 duplicados) |
| `threshold` | DOUBLE | Sí | Umbral aceptable |
| `message` | STRING | Sí | Descripción del resultado |
| `checked_at` | STRING | No | ISO timestamp de la ejecución |
| `run_id` | STRING | No | Identificador único del run |
