from __future__ import annotations

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
        LOCATION 's3://cryptolake-gold/fact_market_daily'
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
        spark.sql("CREATE NAMESPACE IF NOT EXISTS cryptolake.gold LOCATION 's3://cryptolake-gold/'")

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
