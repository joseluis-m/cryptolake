# Data Dictionary -- CryptoLake

Complete field-level documentation for every table in the Lakehouse.

---

## Bronze Layer

### cryptolake.bronze.historical_prices

Raw historical price data from CoinGecko API. Append-only.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `coin_id` | STRING | No | CoinGecko identifier (e.g. `bitcoin`, `ethereum`) |
| `timestamp_ms` | BIGINT | No | Unix timestamp in milliseconds of the datapoint |
| `price_usd` | DOUBLE | No | Price in USD at the given timestamp |
| `market_cap_usd` | DOUBLE | Yes | Market capitalization in USD |
| `volume_24h_usd` | DOUBLE | Yes | 24-hour trading volume in USD |
| `_ingested_at` | STRING | No | ISO timestamp of when the Python extractor downloaded the data |
| `_source` | STRING | No | Data origin (e.g. `coingecko_market_chart`) |
| `_loaded_at` | TIMESTAMP | No | Spark timestamp of when the record was written to Iceberg |

**Partitioned by**: `coin_id`
**Compression**: zstd

### cryptolake.bronze.fear_greed

Raw Fear & Greed Index data from Alternative.me API.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `value` | INT | No | Index value (0-100). 0 = Extreme Fear, 100 = Extreme Greed |
| `classification` | STRING | No | Text label: Extreme Fear, Fear, Neutral, Greed, Extreme Greed |
| `timestamp` | BIGINT | No | Unix timestamp in seconds |
| `_ingested_at` | STRING | No | ISO timestamp of extraction |
| `_source` | STRING | No | Origin: `alternative_me_fng` |
| `_loaded_at` | TIMESTAMP | No | Iceberg load timestamp |

**Compression**: zstd

---

## Silver Layer

### cryptolake.silver.daily_prices

Cleaned, deduplicated prices with one record per coin per day.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `coin_id` | STRING | No | Cryptocurrency identifier |
| `price_date` | DATE | No | Price date (converted from `timestamp_ms`) |
| `price_usd` | DOUBLE | No | Price in USD (only values > 0) |
| `market_cap_usd` | DOUBLE | Yes | Market cap (null if original value was <= 0) |
| `volume_24h_usd` | DOUBLE | Yes | 24h volume (null if original value was <= 0) |
| `_processed_at` | TIMESTAMP | No | Silver processing timestamp |

**Partitioned by**: `coin_id`
**Deduplication**: `ROW_NUMBER() OVER (PARTITION BY coin_id, price_date ORDER BY _loaded_at DESC)`
**Update strategy**: MERGE INTO (incremental upsert)

### cryptolake.silver.fear_greed

Cleaned Fear & Greed Index with DATE-typed dates.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `index_date` | DATE | No | Index date (converted from `timestamp`) |
| `fear_greed_value` | INT | No | Index value 0-100 |
| `classification` | STRING | No | Sentiment classification label |
| `_processed_at` | TIMESTAMP | No | Processing timestamp |

**Update strategy**: MERGE INTO

---

## Gold Layer (dbt models)

### cryptolake.gold.dim_coins

Cryptocurrency dimension with aggregate statistics. SCD Type 1.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `coin_id` | STRING | No | **PK**. Unique cryptocurrency identifier |
| `first_tracked_date` | DATE | No | Earliest date with data |
| `last_tracked_date` | DATE | No | Most recent date with data |
| `total_days_tracked` | INT | No | Number of days with data |
| `all_time_low` | DOUBLE | No | Historical minimum price |
| `all_time_high` | DOUBLE | No | Historical maximum price |
| `avg_price` | DOUBLE | No | Average price |
| `avg_daily_volume` | DOUBLE | Yes | Average daily volume |
| `price_range_pct` | DOUBLE | No | Price range in % ((max-min)/min x 100) |
| `_loaded_at` | TIMESTAMP | No | dbt generation timestamp |

**dbt materialization**: `table` (fully recreated on each run)

### cryptolake.gold.dim_dates

Calendar dimension generated from observed price dates.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `date_day` | DATE | No | **PK**. Calendar date |
| `year` | INT | No | Year |
| `month` | INT | No | Month (1-12) |
| `day_of_month` | INT | No | Day of month (1-31) |
| `day_of_week` | INT | No | Day of week (1=Sunday, 7=Saturday) |
| `week_of_year` | INT | No | Week of year (1-52) |
| `quarter` | INT | No | Quarter (1-4) |
| `is_weekend` | BOOLEAN | No | `true` if Saturday or Sunday |
| `day_name` | STRING | No | Day name (Monday, Tuesday, ...) |
| `month_name` | STRING | No | Month name (January, February, ...) |

**dbt materialization**: `table`

### cryptolake.gold.fact_market_daily

Central fact table. Granularity: 1 row = 1 coin x 1 day.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `coin_id` | STRING | No | **FK** -> dim_coins.coin_id |
| `price_date` | DATE | No | **FK** -> dim_dates.date_day |
| `price_usd` | DOUBLE | No | Daily closing price |
| `market_cap_usd` | DOUBLE | Yes | Market capitalization |
| `volume_24h_usd` | DOUBLE | Yes | 24h trading volume |
| `price_change_pct_1d` | DOUBLE | Yes | Day-over-day change % |
| `moving_avg_7d` | DOUBLE | Yes | 7-day moving average |
| `moving_avg_30d` | DOUBLE | Yes | 30-day moving average |
| `volatility_7d` | DOUBLE | Yes | 7-day price standard deviation |
| `avg_volume_7d` | DOUBLE | Yes | 7-day average volume |
| `fear_greed_value` | INT | Yes | Fear & Greed value for the day (via LEFT JOIN) |
| `market_sentiment` | STRING | Yes | Sentiment classification |
| `sentiment_score` | INT | Yes | Numeric sentiment score 1-5 |
| `ma30_signal` | STRING | Yes | ABOVE_MA30 / BELOW_MA30 / AT_MA30 |
| `combined_signal` | STRING | Yes | Combined price + sentiment signal |
| `_loaded_at` | TIMESTAMP | No | dbt generation timestamp |

**dbt materialization**: `table`
**Unique key**: `[coin_id, price_date]`

---

## Quality Layer

### cryptolake.quality.check_results

Quality check results (generated by `run_quality_checks.py`).

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `check_name` | STRING | No | Check name (e.g. `no_duplicates`) |
| `layer` | STRING | No | Validated layer: bronze, silver, gold |
| `table_name` | STRING | No | Fully qualified table name |
| `status` | STRING | No | passed, failed, warning, error |
| `metric_value` | DOUBLE | Yes | Measured value (e.g. 0 duplicates) |
| `threshold` | DOUBLE | Yes | Acceptable threshold |
| `message` | STRING | Yes | Result description |
| `checked_at` | STRING | No | ISO timestamp of the check execution |
| `run_id` | STRING | No | Unique run identifier |
