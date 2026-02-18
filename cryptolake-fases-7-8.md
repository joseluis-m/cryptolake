# CryptoLake — Fase 7 y Fase 8

## Guía paso a paso: Data Quality + Serving + CI/CD + Terraform

> **Punto de partida**: Tienes el pipeline completo funcionando en Airflow
> (Fases 1-6): Ingesta → Bronze → Silver → Gold (dbt), todo orquestado.
>
> **Al terminar esta guía**: Tendrás validación de calidad automatizada,
> una API REST con FastAPI que sirve datos del Lakehouse, un dashboard
> interactivo con Streamlit, CI/CD con GitHub Actions, y la infraestructura
> definida como código con Terraform.

---

## PARTE 9 (FASE 7): Data Quality + Serving Layer

### 9.1 — Conceptos: ¿Qué es la Serving Layer y por qué la necesitamos?

Hasta ahora, para consultar los datos del Lakehouse necesitas:
- Abrir un PySpark shell y escribir SQL
- Mirar directamente las tablas Iceberg desde Spark

Eso está bien para desarrollo, pero un analista, un trader, o un frontend
**no pueden hacer eso**. Necesitan:

1. **Una API REST** — Cualquier aplicación puede pedir datos con HTTP
2. **Un dashboard** — Visualización interactiva sin escribir código
3. **Validación** — Garantía de que los datos que sirves son correctos

**¿Cómo conectamos FastAPI con los datos del Lakehouse?**

```
Streamlit Dashboard ──HTTP──▶ FastAPI ──PyHive/JDBC──▶ Spark Thrift Server ──▶ Iceberg (MinIO)
```

El Spark Thrift Server que configuraste en la Fase 5 (puerto 10000) ya
expone todas las tablas Iceberg como si fueran un data warehouse SQL.
FastAPI se conecta a él con PyHive (el mismo driver que usa dbt) y ejecuta
queries sobre las tablas Gold del star schema.

**¿Por qué no conectar Streamlit directamente a Spark?**

Porque tener una API REST intermedia aporta:
- **Separación de responsabilidades**: La API controla qué datos se exponen
- **Cacheo**: La API puede cachear resultados frecuentes
- **Seguridad**: La API puede tener autenticación
- **Reutilización**: Cualquier frontend, app móvil o notebook puede usarla
- **Documentación automática**: FastAPI genera docs OpenAPI/Swagger

### 9.2 — Data Quality: Validadores custom

La guía general menciona Great Expectations, pero tiene el mismo problema
de dependencias que encontramos con dbt-spark: conflictos de protobuf con
Airflow. En su lugar, implementamos validadores custom que hacen lo mismo:
checks declarativos ejecutados en Spark.

**¿Qué validamos?**

| Capa | Check | Por qué |
|------|-------|---------|
| Bronze | Tabla existe | El job de ingesta pudo haber fallado |
| Bronze | Mínimo de filas | Detecta ingestas vacías o parciales |
| Bronze | Schema correcto | Detecta cambios en las APIs externas |
| Bronze | Freshness (<48h) | Detecta pipelines que dejaron de ejecutarse |
| Silver | No duplicados | El MERGE INTO podría no estar funcionando |
| Silver | Precios positivos | Datos corruptos de la API |
| Silver | No nulls en PKs | Integridad de las claves |
| Silver | No fechas futuras | Datos incorrectos de la API |
| Silver | F&G en rango [0,100] | Validación de dominio |
| Silver | Clasificaciones válidas | Solo valores esperados |
| Gold | PKs únicas en dims | Integridad dimensional |
| Gold | No gaps en fechas | Completitud temporal |
| Gold | Fact tiene datos | El dbt run pudo haber fallado |
| Gold | FK coin_id existe en dim | Integridad referencial |
| Gold | FK price_date existe en dim | Integridad referencial |

Crea la carpeta y archivos:

```bash
mkdir -p src/quality
```

**src/quality/__init__.py:**

```bash
cat > src/quality/__init__.py << 'PYEOF'
"""CryptoLake Data Quality Framework — Fase 7."""
PYEOF
```

**src/quality/validators.py:**

```bash
cat > src/quality/validators.py << 'PYEOF'
"""
Framework de validación de calidad de datos — CryptoLake Fase 7.

Checks alineados con los schemas reales del proyecto:

Bronze:
  - historical_prices: coin_id, timestamp_ms, price_usd, market_cap_usd,
    volume_24h_usd, _ingested_at, _source, _loaded_at
  - fear_greed: value, classification, timestamp,
    _ingested_at, _source, _loaded_at

Silver:
  - daily_prices: coin_id, price_date, price_usd, market_cap_usd,
    volume_24h_usd, _processed_at
  - fear_greed: index_date, fear_greed_value, classification, _processed_at

Gold (dbt):
  - dim_coins: coin_id, first_tracked_date, all_time_high, avg_price, ...
  - dim_dates: date_day, year, month, quarter, is_weekend, ...
  - fact_market_daily: coin_id, price_date, price_usd, moving_avg_7d,
    fear_greed_value, ma30_signal, ...
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


# ════════════════════════════════════════════════════════════
# Modelos de resultado
# ════════════════════════════════════════════════════════════

class CheckStatus(str, Enum):
    PASSED = "passed"
    FAILED = "failed"
    WARNING = "warning"
    ERROR = "error"


@dataclass
class CheckResult:
    """Resultado de un check individual."""
    check_name: str
    layer: str
    table_name: str
    status: CheckStatus
    metric_value: Optional[float] = None
    threshold: Optional[float] = None
    message: str = ""
    checked_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    def to_dict(self) -> dict:
        return {
            "check_name": self.check_name,
            "layer": self.layer,
            "table_name": self.table_name,
            "status": self.status.value,
            "metric_value": self.metric_value,
            "threshold": self.threshold,
            "message": self.message,
            "checked_at": self.checked_at,
        }


# ════════════════════════════════════════════════════════════
# Base
# ════════════════════════════════════════════════════════════

class BaseValidator:
    """Clase base con utilidades comunes para todos los validadores."""

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.results: list[CheckResult] = []

    def _add(self, result: CheckResult):
        self.results.append(result)
        icons = {"passed": "✅", "failed": "❌", "warning": "⚠️", "error": "💥"}
        icon = icons.get(result.status.value, "❓")
        logger.info(
            f"{icon} [{result.layer}] {result.check_name} "
            f"on {result.table_name}: {result.message}"
        )

    def _exists(self, table: str) -> bool:
        try:
            self.spark.sql(f"DESCRIBE TABLE {table}")
            return True
        except Exception:
            return False

    def _count(self, table: str) -> int:
        return self.spark.sql(f"SELECT COUNT(*) AS cnt FROM {table}").first().cnt

    def _nulls(self, table: str, col: str) -> int:
        return self.spark.sql(
            f"SELECT COUNT(*) AS cnt FROM {table} WHERE {col} IS NULL"
        ).first().cnt

    def _distinct(self, table: str, col: str) -> int:
        return self.spark.sql(
            f"SELECT COUNT(DISTINCT {col}) AS cnt FROM {table}"
        ).first().cnt

    def get_summary(self) -> dict:
        total = len(self.results)
        passed = sum(1 for r in self.results if r.status == CheckStatus.PASSED)
        failed = sum(1 for r in self.results if r.status == CheckStatus.FAILED)
        return {
            "total": total,
            "passed": passed,
            "failed": failed,
            "warnings": sum(1 for r in self.results if r.status == CheckStatus.WARNING),
            "errors": sum(1 for r in self.results if r.status == CheckStatus.ERROR),
            "pass_rate": round(passed / total * 100, 1) if total > 0 else 0,
        }


# ════════════════════════════════════════════════════════════
# BRONZE
# ════════════════════════════════════════════════════════════
# Columnas reales:
#   historical_prices: coin_id, timestamp_ms, price_usd, market_cap_usd,
#                      volume_24h_usd, _ingested_at, _source, _loaded_at
#   fear_greed:        value, classification, timestamp,
#                      _ingested_at, _source, _loaded_at
# ════════════════════════════════════════════════════════════

class BronzeValidator(BaseValidator):
    """Valida las tablas de la capa Bronze."""

    TABLES = {
        "cryptolake.bronze.historical_prices": {
            "min_rows": 100,
            "required_columns": [
                "coin_id", "timestamp_ms", "price_usd",
                "market_cap_usd", "volume_24h_usd",
                "_ingested_at", "_source", "_loaded_at",
            ],
            "freshness_col": "_loaded_at",
        },
        "cryptolake.bronze.fear_greed": {
            "min_rows": 30,
            "required_columns": [
                "value", "classification", "timestamp",
                "_ingested_at", "_source", "_loaded_at",
            ],
            "freshness_col": "_loaded_at",
        },
    }

    def check_all(self) -> list[CheckResult]:
        for table, cfg in self.TABLES.items():
            self._check_exists(table)
            if self._exists(table):
                self._check_row_count(table, cfg["min_rows"])
                self._check_schema(table, cfg["required_columns"])
                self._check_freshness(table, cfg["freshness_col"])
        return self.results

    def _check_exists(self, table: str):
        exists = self._exists(table)
        self._add(CheckResult(
            check_name="table_exists", layer="bronze", table_name=table,
            status=CheckStatus.PASSED if exists else CheckStatus.FAILED,
            message=f"Table {'exists' if exists else 'NOT FOUND'}",
        ))

    def _check_row_count(self, table: str, min_rows: int):
        count = self._count(table)
        self._add(CheckResult(
            check_name="min_row_count", layer="bronze", table_name=table,
            status=CheckStatus.PASSED if count >= min_rows else CheckStatus.FAILED,
            metric_value=float(count), threshold=float(min_rows),
            message=f"Rows: {count} (min: {min_rows})",
        ))

    def _check_schema(self, table: str, required: list[str]):
        df = self.spark.sql(f"SELECT * FROM {table} LIMIT 0")
        missing = set(required) - set(df.columns)
        self._add(CheckResult(
            check_name="schema_check", layer="bronze", table_name=table,
            status=CheckStatus.PASSED if not missing else CheckStatus.FAILED,
            message=f"Missing: {missing}" if missing else "All columns present",
        ))

    def _check_freshness(self, table: str, ts_col: str):
        """Verifica que _loaded_at no supere las 48h de antigüedad."""
        try:
            row = self.spark.sql(f"""
                SELECT TIMESTAMPDIFF(
                    HOUR, MAX({ts_col}), CURRENT_TIMESTAMP()
                ) AS hours FROM {table}
            """).first()
            hours = row.hours
            if hours is None:
                self._add(CheckResult(
                    check_name="data_freshness", layer="bronze",
                    table_name=table, status=CheckStatus.WARNING,
                    message="No timestamp values found",
                ))
                return

            threshold = 48
            status = (CheckStatus.PASSED if hours <= threshold
                      else CheckStatus.WARNING if hours <= 72
                      else CheckStatus.FAILED)
            self._add(CheckResult(
                check_name="data_freshness", layer="bronze",
                table_name=table, status=status,
                metric_value=float(hours), threshold=float(threshold),
                message=f"Last load: {hours}h ago (threshold: {threshold}h)",
            ))
        except Exception as e:
            self._add(CheckResult(
                check_name="data_freshness", layer="bronze",
                table_name=table, status=CheckStatus.ERROR,
                message=f"Error: {e}",
            ))


# ════════════════════════════════════════════════════════════
# SILVER
# ════════════════════════════════════════════════════════════
# Columnas reales:
#   daily_prices: coin_id, price_date, price_usd, market_cap_usd,
#                 volume_24h_usd, _processed_at
#   fear_greed:   index_date, fear_greed_value, classification,
#                 _processed_at
# ════════════════════════════════════════════════════════════

class SilverValidator(BaseValidator):
    """Valida las tablas de la capa Silver."""

    def check_all(self) -> list[CheckResult]:
        self._check_daily_prices()
        self._check_fear_greed()
        return self.results

    def _check_daily_prices(self):
        table = "cryptolake.silver.daily_prices"
        if not self._exists(table):
            self._add(CheckResult(
                check_name="table_exists", layer="silver",
                table_name=table, status=CheckStatus.FAILED,
                message="NOT FOUND",
            ))
            return

        # No duplicados: (coin_id, price_date) único
        dups = self.spark.sql(f"""
            SELECT COUNT(*) AS cnt FROM (
                SELECT coin_id, price_date, COUNT(*) AS n
                FROM {table} GROUP BY coin_id, price_date HAVING n > 1
            )
        """).first().cnt
        self._add(CheckResult(
            check_name="no_duplicates", layer="silver", table_name=table,
            status=CheckStatus.PASSED if dups == 0 else CheckStatus.FAILED,
            metric_value=float(dups), threshold=0.0,
            message=f"Duplicate (coin_id, price_date): {dups}",
        ))

        # Precios positivos
        neg = self.spark.sql(
            f"SELECT COUNT(*) AS cnt FROM {table} WHERE price_usd <= 0"
        ).first().cnt
        self._add(CheckResult(
            check_name="positive_prices", layer="silver", table_name=table,
            status=CheckStatus.PASSED if neg == 0 else CheckStatus.FAILED,
            metric_value=float(neg), threshold=0.0,
            message=f"price_usd <= 0: {neg}",
        ))

        # Not null en columnas clave
        for col in ["coin_id", "price_date", "price_usd"]:
            n = self._nulls(table, col)
            self._add(CheckResult(
                check_name=f"not_null_{col}", layer="silver",
                table_name=table,
                status=CheckStatus.PASSED if n == 0 else CheckStatus.FAILED,
                metric_value=float(n), threshold=0.0,
                message=f"Nulls in {col}: {n}",
            ))

        # No fechas futuras
        future = self.spark.sql(
            f"SELECT COUNT(*) AS cnt FROM {table} "
            f"WHERE price_date > CURRENT_DATE()"
        ).first().cnt
        self._add(CheckResult(
            check_name="no_future_dates", layer="silver", table_name=table,
            status=CheckStatus.PASSED if future == 0 else CheckStatus.FAILED,
            metric_value=float(future), threshold=0.0,
            message=f"Future dates: {future}",
        ))

    def _check_fear_greed(self):
        table = "cryptolake.silver.fear_greed"
        if not self._exists(table):
            self._add(CheckResult(
                check_name="table_exists", layer="silver",
                table_name=table, status=CheckStatus.FAILED,
                message="NOT FOUND",
            ))
            return

        # Valores entre 0 y 100
        out = self.spark.sql(f"""
            SELECT COUNT(*) AS cnt FROM {table}
            WHERE fear_greed_value < 0 OR fear_greed_value > 100
        """).first().cnt
        self._add(CheckResult(
            check_name="value_range_0_100", layer="silver",
            table_name=table,
            status=CheckStatus.PASSED if out == 0 else CheckStatus.FAILED,
            metric_value=float(out), threshold=0.0,
            message=f"Out of [0,100]: {out}",
        ))

        # Clasificaciones válidas
        valid = [
            "Extreme Fear", "Fear", "Neutral", "Greed", "Extreme Greed"
        ]
        in_clause = ", ".join(f"'{v}'" for v in valid)
        invalid = self.spark.sql(f"""
            SELECT COUNT(*) AS cnt FROM {table}
            WHERE classification NOT IN ({in_clause})
        """).first().cnt
        self._add(CheckResult(
            check_name="valid_classifications", layer="silver",
            table_name=table,
            status=CheckStatus.PASSED if invalid == 0 else CheckStatus.FAILED,
            metric_value=float(invalid), threshold=0.0,
            message=f"Invalid classifications: {invalid}",
        ))

        # Not null en index_date (PK de la tabla)
        n = self._nulls(table, "index_date")
        self._add(CheckResult(
            check_name="not_null_index_date", layer="silver",
            table_name=table,
            status=CheckStatus.PASSED if n == 0 else CheckStatus.FAILED,
            metric_value=float(n), threshold=0.0,
            message=f"Nulls in index_date: {n}",
        ))


# ════════════════════════════════════════════════════════════
# GOLD
# ════════════════════════════════════════════════════════════
# dim_dates usa date_day (NO date_key)
# fact_market_daily usa price_date como FK a dim_dates.date_day
# dim_coins usa coin_id como PK
# ════════════════════════════════════════════════════════════

class GoldValidator(BaseValidator):
    """Valida las tablas de la capa Gold (star schema dbt)."""

    def check_all(self) -> list[CheckResult]:
        self._check_dim_coins()
        self._check_dim_dates()
        self._check_fact_market_daily()
        self._check_referential_integrity()
        return self.results

    def _check_dim_coins(self):
        table = "cryptolake.gold.dim_coins"
        if not self._exists(table):
            self._add(CheckResult(
                check_name="table_exists", layer="gold",
                table_name=table, status=CheckStatus.FAILED,
                message="NOT FOUND",
            ))
            return

        # coin_id es PK → debe ser único
        total = self._count(table)
        distinct = self._distinct(table, "coin_id")
        self._add(CheckResult(
            check_name="unique_pk_coin_id", layer="gold",
            table_name=table,
            status=CheckStatus.PASSED if total == distinct else CheckStatus.FAILED,
            metric_value=float(total - distinct), threshold=0.0,
            message=f"Total: {total}, Distinct: {distinct}",
        ))

    def _check_dim_dates(self):
        table = "cryptolake.gold.dim_dates"
        if not self._exists(table):
            self._add(CheckResult(
                check_name="table_exists", layer="gold",
                table_name=table, status=CheckStatus.FAILED,
                message="NOT FOUND",
            ))
            return

        # Continuidad: no gaps en date_day
        row = self.spark.sql(f"""
            SELECT COUNT(*) AS total,
                   DATEDIFF(MAX(date_day), MIN(date_day)) + 1 AS expected
            FROM {table}
        """).first()
        gaps = row.expected - row.total
        self._add(CheckResult(
            check_name="no_date_gaps", layer="gold",
            table_name=table,
            status=CheckStatus.PASSED if gaps == 0 else CheckStatus.WARNING,
            metric_value=float(gaps), threshold=0.0,
            message=f"Missing dates: {gaps} (total: {row.total})",
        ))

    def _check_fact_market_daily(self):
        table = "cryptolake.gold.fact_market_daily"
        if not self._exists(table):
            self._add(CheckResult(
                check_name="table_exists", layer="gold",
                table_name=table, status=CheckStatus.FAILED,
                message="NOT FOUND",
            ))
            return

        count = self._count(table)
        self._add(CheckResult(
            check_name="has_rows", layer="gold",
            table_name=table,
            status=CheckStatus.PASSED if count > 0 else CheckStatus.FAILED,
            metric_value=float(count), threshold=1.0,
            message=f"Rows: {count}",
        ))

    def _check_referential_integrity(self):
        """
        FK checks:
          fact.coin_id       → dim_coins.coin_id
          fact.price_date    → dim_dates.date_day
        """
        fact = "cryptolake.gold.fact_market_daily"
        dim_coins = "cryptolake.gold.dim_coins"
        dim_dates = "cryptolake.gold.dim_dates"

        if not all(self._exists(t) for t in [fact, dim_coins, dim_dates]):
            return

        # coin_id → dim_coins.coin_id
        orphans = self.spark.sql(f"""
            SELECT COUNT(*) AS cnt FROM {fact} f
            LEFT JOIN {dim_coins} d ON f.coin_id = d.coin_id
            WHERE d.coin_id IS NULL
        """).first().cnt
        self._add(CheckResult(
            check_name="fk_coin_id", layer="gold", table_name=fact,
            status=CheckStatus.PASSED if orphans == 0 else CheckStatus.FAILED,
            metric_value=float(orphans), threshold=0.0,
            message=f"Orphan coin_id: {orphans}",
        ))

        # price_date → dim_dates.date_day
        orphans = self.spark.sql(f"""
            SELECT COUNT(*) AS cnt FROM {fact} f
            LEFT JOIN {dim_dates} d ON f.price_date = d.date_day
            WHERE d.date_day IS NULL
        """).first().cnt
        self._add(CheckResult(
            check_name="fk_price_date", layer="gold", table_name=fact,
            status=CheckStatus.PASSED if orphans == 0 else CheckStatus.FAILED,
            metric_value=float(orphans), threshold=0.0,
            message=f"Orphan price_date: {orphans}",
        ))
PYEOF
```

**src/quality/run_quality_checks.py** — el spark-submit que ejecuta todo:

```bash
cat > src/quality/run_quality_checks.py << 'PYEOF'
"""
Ejecutor de quality checks — CryptoLake Fase 7.

spark-submit src/quality/run_quality_checks.py
spark-submit src/quality/run_quality_checks.py --layer bronze
spark-submit src/quality/run_quality_checks.py --layer silver --layer gold
"""
import argparse
import json
import logging
import sys
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType, StringType, StructField, StructType,
)

sys.path.insert(0, "/opt/spark/work")
from src.quality.validators import (
    BronzeValidator, SilverValidator, GoldValidator,
    CheckResult, CheckStatus,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

RESULTS_TABLE = "cryptolake.quality.check_results"
RESULTS_SCHEMA = StructType([
    StructField("check_name", StringType(), False),
    StructField("layer", StringType(), False),
    StructField("table_name", StringType(), False),
    StructField("status", StringType(), False),
    StructField("metric_value", DoubleType(), True),
    StructField("threshold", DoubleType(), True),
    StructField("message", StringType(), True),
    StructField("checked_at", StringType(), False),
    StructField("run_id", StringType(), False),
])


def persist_results(spark, results, run_id):
    """Guarda los resultados en cryptolake.quality.check_results."""
    rows = [dict(**r.to_dict(), run_id=run_id) for r in results]
    df = spark.createDataFrame(rows, schema=RESULTS_SCHEMA)
    spark.sql("CREATE NAMESPACE IF NOT EXISTS cryptolake.quality")
    df.writeTo(RESULTS_TABLE).using("iceberg").createOrReplace()
    logger.info(f"📊 {len(rows)} results saved to {RESULTS_TABLE}")


def print_summary(results):
    """Imprime resumen y devuelve True si todo pasa."""
    print("\n" + "=" * 60)
    print("📋 DATA QUALITY SUMMARY — CryptoLake")
    print("=" * 60)

    by_layer = {}
    for r in results:
        by_layer.setdefault(r.layer, []).append(r)

    has_failures = False
    for layer in ["bronze", "silver", "gold"]:
        checks = by_layer.get(layer, [])
        if not checks:
            continue
        p = sum(1 for c in checks if c.status == CheckStatus.PASSED)
        f = sum(1 for c in checks if c.status == CheckStatus.FAILED)
        w = sum(1 for c in checks if c.status == CheckStatus.WARNING)
        if f > 0:
            has_failures = True
        print(f"\n  🔷 {layer.upper()} ({len(checks)} checks)")
        print(f"     ✅ {p}  ❌ {f}  ⚠️ {w}")
        for c in checks:
            if c.status != CheckStatus.PASSED:
                icons = {"failed": "❌", "warning": "⚠️", "error": "💥"}
                short = c.table_name.split(".")[-1]
                print(f"     {icons.get(c.status.value, '?')} "
                      f"{c.check_name} ({short}): {c.message}")

    total = len(results)
    ok = sum(1 for r in results if r.status == CheckStatus.PASSED)
    rate = round(ok / total * 100, 1) if total > 0 else 0
    emoji = "✅" if not has_failures else "❌"
    print(f"\n{'=' * 60}")
    print(f"  {emoji} Pass rate: {rate}% ({ok}/{total})")
    print(f"{'=' * 60}\n")
    return not has_failures


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--layer", action="append", default=None,
                        choices=["bronze", "silver", "gold"])
    args = parser.parse_args()
    layers = args.layer or ["bronze", "silver", "gold"]

    spark = SparkSession.builder.appName("CryptoLake-Quality").getOrCreate()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    all_results = []

    logger.info(f"🔍 Quality checks (run_id={run_id}, layers={layers})")

    if "bronze" in layers:
        all_results.extend(BronzeValidator(spark).check_all())
    if "silver" in layers:
        all_results.extend(SilverValidator(spark).check_all())
    if "gold" in layers:
        all_results.extend(GoldValidator(spark).check_all())

    try:
        persist_results(spark, all_results, run_id)
    except Exception as e:
        logger.warning(f"⚠️ Could not persist results: {e}")

    ok = print_summary(all_results)
    spark.stop()

    if not ok:
        sys.exit(1)


if __name__ == "__main__":
    main()
PYEOF
```

Prueba los quality checks:

```bash
# Todos los checks
docker exec cryptolake-spark-master \
    /opt/spark/bin/spark-submit \
    /opt/spark/work/src/quality/run_quality_checks.py

# Solo una capa
docker exec cryptolake-spark-master \
    /opt/spark/bin/spark-submit \
    /opt/spark/work/src/quality/run_quality_checks.py --layer bronze
```

### 9.3 — FastAPI: REST API para servir datos del Lakehouse

Creamos la estructura de la API:

```bash
mkdir -p src/serving/api/routes
mkdir -p src/serving/api/models
```

**src/serving/__init__.py** y **src/serving/api/__init__.py:**

```bash
touch src/serving/__init__.py
touch src/serving/api/__init__.py
touch src/serving/api/routes/__init__.py
touch src/serving/api/models/__init__.py
```

**src/serving/api/database.py** — conexión al Spark Thrift Server:

```bash
cat > src/serving/api/database.py << 'PYEOF'
"""
Conexión al Spark Thrift Server via PyHive.

Reutiliza la misma conexión que dbt usa (thrift en puerto 10000).
La API ejecuta queries SQL sobre las tablas Gold del star schema.
"""
import os

from pyhive import hive


# Dentro de Docker: spark-thrift. Desde Mac: localhost
THRIFT_HOST = os.getenv("THRIFT_HOST", "spark-thrift")
THRIFT_PORT = int(os.getenv("THRIFT_PORT", "10000"))


def get_connection():
    """Crea una conexión PyHive al Spark Thrift Server."""
    return hive.connect(
        host=THRIFT_HOST,
        port=THRIFT_PORT,
        auth="NOSASL",
    )


def execute_query(sql: str) -> list[dict]:
    """
    Ejecuta una query SQL y devuelve una lista de diccionarios.

    Ejemplo:
        rows = execute_query("SELECT * FROM cryptolake.gold.dim_coins")
        # [{"coin_id": "bitcoin", "avg_price": 95000.0, ...}, ...]
    """
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        return [dict(zip(columns, row)) for row in rows]
    finally:
        conn.close()
PYEOF
```

**src/serving/api/models/schemas.py** — modelos Pydantic de respuesta:

```bash
cat > src/serving/api/models/schemas.py << 'PYEOF'
"""
Modelos de respuesta Pydantic — alineados con el star schema Gold.

Estos modelos definen la estructura exacta de las respuestas JSON
de la API. FastAPI los usa para:
- Validar la forma de los datos antes de enviarlos
- Generar documentación OpenAPI automática
- Serializar correctamente tipos como date/float
"""
from datetime import date
from typing import Optional

from pydantic import BaseModel


class PriceResponse(BaseModel):
    """Una fila de fact_market_daily."""
    coin_id: str
    price_date: date
    price_usd: float
    market_cap_usd: Optional[float] = None
    volume_24h_usd: Optional[float] = None
    price_change_pct_1d: Optional[float] = None
    moving_avg_7d: Optional[float] = None
    moving_avg_30d: Optional[float] = None
    volatility_7d: Optional[float] = None
    fear_greed_value: Optional[int] = None
    market_sentiment: Optional[str] = None
    ma30_signal: Optional[str] = None


class CoinResponse(BaseModel):
    """Una fila de dim_coins."""
    coin_id: str
    first_tracked_date: Optional[date] = None
    last_tracked_date: Optional[date] = None
    total_days_tracked: Optional[int] = None
    all_time_low: Optional[float] = None
    all_time_high: Optional[float] = None
    avg_price: Optional[float] = None
    avg_daily_volume: Optional[float] = None
    price_range_pct: Optional[float] = None


class MarketOverview(BaseModel):
    """Resumen general del mercado."""
    total_coins: int
    date_range_start: Optional[date] = None
    date_range_end: Optional[date] = None
    total_fact_rows: int
    latest_fear_greed: Optional[int] = None
    latest_sentiment: Optional[str] = None


class FearGreedResponse(BaseModel):
    """Un dato del Fear & Greed Index."""
    index_date: date
    fear_greed_value: int
    classification: str


class HealthResponse(BaseModel):
    """Health check de la API."""
    status: str
    thrift_connected: bool
    tables_available: int
PYEOF
```

**src/serving/api/routes/prices.py:**

```bash
cat > src/serving/api/routes/prices.py << 'PYEOF'
"""
Endpoints de precios — consultan fact_market_daily.
"""
from datetime import date, timedelta
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from src.serving.api.database import execute_query
from src.serving.api.models.schemas import PriceResponse

router = APIRouter(tags=["Prices"])


@router.get("/prices/{coin_id}", response_model=list[PriceResponse])
async def get_prices(
    coin_id: str,
    start_date: Optional[date] = Query(
        default=None,
        description="Fecha inicio (default: 30 días atrás)",
    ),
    end_date: Optional[date] = Query(
        default=None,
        description="Fecha fin (default: hoy)",
    ),
    limit: int = Query(default=100, le=1000),
):
    """
    Precios históricos de una criptomoneda.

    Incluye métricas calculadas: moving averages, volatilidad,
    señales técnicas y sentimiento del mercado.
    """
    if not end_date:
        end_date = date.today()
    if not start_date:
        start_date = end_date - timedelta(days=30)

    try:
        rows = execute_query(f"""
            SELECT
                coin_id, price_date, price_usd,
                market_cap_usd, volume_24h_usd,
                price_change_pct_1d,
                moving_avg_7d, moving_avg_30d,
                volatility_7d,
                fear_greed_value, market_sentiment,
                ma30_signal
            FROM cryptolake.gold.fact_market_daily
            WHERE coin_id = '{coin_id}'
              AND price_date BETWEEN '{start_date}' AND '{end_date}'
            ORDER BY price_date DESC
            LIMIT {limit}
        """)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query error: {e}")

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No data found for {coin_id}",
        )
    return rows
PYEOF
```

**src/serving/api/routes/analytics.py:**

```bash
cat > src/serving/api/routes/analytics.py << 'PYEOF'
"""
Endpoints analíticos — market overview, fear & greed, coins.
"""
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from src.serving.api.database import execute_query
from src.serving.api.models.schemas import (
    CoinResponse,
    FearGreedResponse,
    MarketOverview,
)

router = APIRouter(prefix="/analytics", tags=["Analytics"])


@router.get("/market-overview", response_model=MarketOverview)
async def get_market_overview():
    """Resumen general del mercado crypto."""
    try:
        coins = execute_query(
            "SELECT COUNT(*) AS cnt FROM cryptolake.gold.dim_coins"
        )
        facts = execute_query("""
            SELECT COUNT(*) AS cnt,
                   MIN(price_date) AS min_d,
                   MAX(price_date) AS max_d
            FROM cryptolake.gold.fact_market_daily
        """)
        fg = execute_query("""
            SELECT fear_greed_value, classification
            FROM cryptolake.silver.fear_greed
            ORDER BY index_date DESC LIMIT 1
        """)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return MarketOverview(
        total_coins=coins[0]["cnt"] if coins else 0,
        date_range_start=facts[0]["min_d"] if facts else None,
        date_range_end=facts[0]["max_d"] if facts else None,
        total_fact_rows=facts[0]["cnt"] if facts else 0,
        latest_fear_greed=fg[0]["fear_greed_value"] if fg else None,
        latest_sentiment=fg[0]["classification"] if fg else None,
    )


@router.get("/coins", response_model=list[CoinResponse])
async def get_coins():
    """Lista todas las criptomonedas con estadísticas."""
    try:
        rows = execute_query("""
            SELECT coin_id, first_tracked_date, last_tracked_date,
                   total_days_tracked, all_time_low, all_time_high,
                   avg_price, avg_daily_volume, price_range_pct
            FROM cryptolake.gold.dim_coins
            ORDER BY avg_price DESC
        """)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return rows


@router.get("/fear-greed", response_model=list[FearGreedResponse])
async def get_fear_greed(
    limit: int = Query(default=30, le=365),
):
    """Histórico del Fear & Greed Index."""
    try:
        rows = execute_query(f"""
            SELECT index_date, fear_greed_value, classification
            FROM cryptolake.silver.fear_greed
            ORDER BY index_date DESC
            LIMIT {limit}
        """)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return rows
PYEOF
```

**src/serving/api/routes/health.py:**

```bash
cat > src/serving/api/routes/health.py << 'PYEOF'
"""Health check endpoint."""
from fastapi import APIRouter

from src.serving.api.database import execute_query
from src.serving.api.models.schemas import HealthResponse

router = APIRouter(tags=["Health"])


@router.get("/health", response_model=HealthResponse)
async def health_check():
    """Verifica que la API puede conectar con el Lakehouse."""
    try:
        tables = execute_query("SHOW TABLES IN cryptolake.gold")
        return HealthResponse(
            status="healthy",
            thrift_connected=True,
            tables_available=len(tables),
        )
    except Exception:
        return HealthResponse(
            status="degraded",
            thrift_connected=False,
            tables_available=0,
        )
PYEOF
```

**src/serving/api/main.py** — la aplicación FastAPI:

```bash
cat > src/serving/api/main.py << 'PYEOF'
"""
API REST de CryptoLake.

Endpoints:
- GET /api/v1/prices/{coin_id}           — Precios históricos
- GET /api/v1/analytics/market-overview   — Overview del mercado
- GET /api/v1/analytics/coins             — Lista de criptomonedas
- GET /api/v1/analytics/fear-greed        — Fear & Greed histórico
- GET /api/v1/health                      — Health check

Documentación automática:
- Swagger UI: http://localhost:8000/docs
- ReDoc:      http://localhost:8000/redoc
"""
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.serving.api.routes import analytics, health, prices


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events."""
    print("🚀 CryptoLake API starting...")
    yield
    print("👋 CryptoLake API shutting down...")


app = FastAPI(
    title="CryptoLake API",
    description=(
        "Real-time crypto analytics powered by a Lakehouse architecture. "
        "Queries Apache Iceberg tables via Spark Thrift Server."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(prices.router, prefix="/api/v1")
app.include_router(analytics.router, prefix="/api/v1")
app.include_router(health.router, prefix="/api/v1")


@app.get("/")
async def root():
    return {
        "project": "CryptoLake",
        "docs": "/docs",
        "health": "/api/v1/health",
    }
PYEOF
```

### 9.4 — Dockerfile para la API

```bash
mkdir -p docker/api
```

```bash
cat > docker/api/Dockerfile << 'DOCKERFILE'
FROM python:3.11-slim

WORKDIR /app

# Dependencias del sistema para PyHive (SASL)
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc libsasl2-dev && \
    rm -rf /var/lib/apt/lists/*

# Dependencias Python
RUN pip install --no-cache-dir \
    fastapi==0.110.0 \
    uvicorn==0.27.0 \
    pyhive[hive]==0.7.0 \
    thrift==0.16.0 \
    thrift-sasl==0.4.3 \
    pydantic==2.5.0 \
    pydantic-settings==2.1.0

# El código se monta como volumen en docker-compose
CMD ["uvicorn", "src.serving.api.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
DOCKERFILE
```

### 9.5 — Streamlit Dashboard

```bash
mkdir -p src/serving/dashboard
```

**src/serving/dashboard/app.py:**

```bash
cat > src/serving/dashboard/app.py << 'PYEOF'
"""
Dashboard interactivo de CryptoLake — Streamlit.

Consume la API REST de FastAPI y muestra visualizaciones del mercado crypto.

Ejecución local:
    streamlit run src/serving/dashboard/app.py

En Docker: se levanta automáticamente en http://localhost:8501
"""
import os

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import streamlit as st

# ── Configuración ───────────────────────────────────────────
API_URL = os.getenv("API_URL", "http://localhost:8000")

st.set_page_config(
    page_title="CryptoLake Dashboard",
    page_icon="🏔️",
    layout="wide",
)


# ── Helpers ─────────────────────────────────────────────────
def api_get(endpoint: str):
    """Llama a la API y devuelve JSON o None si falla."""
    try:
        resp = requests.get(f"{API_URL}{endpoint}", timeout=10)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        st.error(f"Error calling API: {e}")
        return None


# ── Header ──────────────────────────────────────────────────
st.title("🏔️ CryptoLake — Crypto Analytics Dashboard")
st.caption("Powered by Apache Iceberg + Spark + dbt + FastAPI")

# ── Health Check ────────────────────────────────────────────
health = api_get("/api/v1/health")
if not health or health.get("status") != "healthy":
    st.warning("⚠️ API not available. Make sure the pipeline has run.")
    st.stop()

# ── Market Overview ─────────────────────────────────────────
st.header("📊 Market Overview")
overview = api_get("/api/v1/analytics/market-overview")

if overview:
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Coins Tracked", overview.get("total_coins", 0))
    col2.metric("Fact Rows", f"{overview.get('total_fact_rows', 0):,}")
    col3.metric(
        "Fear & Greed",
        overview.get("latest_fear_greed", "—"),
        overview.get("latest_sentiment", ""),
    )
    col4.metric(
        "Date Range",
        f"{overview.get('date_range_start', '?')} → {overview.get('date_range_end', '?')}",
    )

# ── Coin Selector ───────────────────────────────────────────
st.header("📈 Price Analysis")
coins = api_get("/api/v1/analytics/coins")

if coins:
    coin_ids = [c["coin_id"] for c in coins]
    selected = st.selectbox("Select cryptocurrency:", coin_ids)

    # ── Price Chart ─────────────────────────────────────────
    prices = api_get(f"/api/v1/prices/{selected}?limit=365")

    if prices:
        df = pd.DataFrame(prices)
        df["price_date"] = pd.to_datetime(df["price_date"])
        df = df.sort_values("price_date")

        # Precio + Moving Averages
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df["price_date"], y=df["price_usd"],
            name="Price", line=dict(color="#4A90D9", width=2),
        ))
        if "moving_avg_7d" in df.columns:
            fig.add_trace(go.Scatter(
                x=df["price_date"], y=df["moving_avg_7d"],
                name="MA 7d", line=dict(color="#F5A623", dash="dash"),
            ))
        if "moving_avg_30d" in df.columns:
            fig.add_trace(go.Scatter(
                x=df["price_date"], y=df["moving_avg_30d"],
                name="MA 30d", line=dict(color="#7B68EE", dash="dot"),
            ))
        fig.update_layout(
            title=f"{selected.title()} — Price & Moving Averages",
            xaxis_title="Date", yaxis_title="Price (USD)",
            template="plotly_dark",
            height=450,
        )
        st.plotly_chart(fig, use_container_width=True)

        # Tabla de coins
        st.subheader("🪙 Coin Stats")
        coins_df = pd.DataFrame(coins)
        st.dataframe(coins_df, use_container_width=True)

# ── Fear & Greed ────────────────────────────────────────────
st.header("😱 Fear & Greed Index")
fg = api_get("/api/v1/analytics/fear-greed?limit=60")

if fg:
    fg_df = pd.DataFrame(fg)
    fg_df["index_date"] = pd.to_datetime(fg_df["index_date"])
    fg_df = fg_df.sort_values("index_date")

    color_map = {
        "Extreme Fear": "#DC3545",
        "Fear": "#FD7E14",
        "Neutral": "#FFC107",
        "Greed": "#28A745",
        "Extreme Greed": "#20C997",
    }

    fig2 = px.bar(
        fg_df, x="index_date", y="fear_greed_value",
        color="classification", color_discrete_map=color_map,
        title="Fear & Greed Index (last 60 days)",
        template="plotly_dark",
        height=350,
    )
    fig2.add_hline(y=50, line_dash="dash", line_color="gray")
    st.plotly_chart(fig2, use_container_width=True)

# ── Footer ──────────────────────────────────────────────────
st.divider()
st.caption(
    "CryptoLake — Data Engineering Portfolio Project | "
    "Apache Iceberg • Spark • dbt • Airflow • FastAPI • Streamlit"
)
PYEOF
```

### 9.6 — Actualizar docker-compose.yml

Añade estos servicios **después del bloque `airflow-scheduler`** en tu
`docker-compose.yml`:

```yaml
  # ============================================================
  # SERVING LAYER (Fase 7)
  # ============================================================
  api:
    build:
      context: ./docker/api
      dockerfile: Dockerfile
    container_name: cryptolake-api
    ports:
      - "8000:8000"
    environment:
      <<: *common-env
      THRIFT_HOST: spark-thrift
      THRIFT_PORT: "10000"
    volumes:
      - ./src:/app/src
    depends_on:
      - spark-thrift

  dashboard:
    image: python:3.11-slim
    container_name: cryptolake-dashboard
    ports:
      - "8501:8501"
    environment:
      <<: *common-env
      API_URL: http://api:8000
    volumes:
      - ./src/serving/dashboard:/app
    command: >
      bash -c "pip install streamlit requests plotly pandas &&
      streamlit run /app/app.py --server.address 0.0.0.0"
    depends_on:
      - api
```

### 9.7 — Actualizar el DAG de Airflow

Reemplaza tu `src/orchestration/dags/dag_full_pipeline.py` con esta versión
que incluye el grupo `data_quality` con los validadores reales:

```bash
cat > src/orchestration/dags/dag_full_pipeline.py << 'PYEOF'
"""
DAG Master de CryptoLake — Fase 7.

Pipeline completo:
1. Init namespaces Iceberg
2. Ingesta batch (CoinGecko + Fear & Greed)
3. Bronze load (APIs → Iceberg)
4. Silver processing (Bronze → Silver con Spark)
5. Gold transformation (Silver → Gold con dbt)
6. Data quality checks (validadores custom)

Schedule: Diario a las 06:00 UTC
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

default_args = {
    "owner": "cryptolake",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}

with DAG(
    dag_id="cryptolake_full_pipeline",
    default_args=default_args,
    description="Pipeline: Ingesta → Bronze → Silver → Gold → Quality",
    schedule="0 6 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["cryptolake", "production"],
    doc_md=__doc__,
) as dag:

    # ── INIT ────────────────────────────────────────────────
    init_namespaces = BashOperator(
        task_id="init_namespaces",
        bash_command=(
            "docker exec cryptolake-spark-master "
            "/opt/spark/bin/spark-submit --master 'local[1]' "
            "/opt/spark/work/src/processing/batch/init_namespaces.py"
        ),
    )

    # ── INGESTA ─────────────────────────────────────────────
    with TaskGroup("ingestion") as ingestion_group:
        extract_coingecko = BashOperator(
            task_id="extract_coingecko",
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

    # ── BRONZE ──────────────────────────────────────────────
    with TaskGroup("bronze_load") as bronze_group:
        api_to_bronze = BashOperator(
            task_id="api_to_bronze",
            bash_command=(
                "docker exec cryptolake-spark-master "
                "/opt/spark/bin/spark-submit "
                "/opt/spark/work/src/processing/batch/api_to_bronze.py"
            ),
        )

    # ── SILVER ──────────────────────────────────────────────
    with TaskGroup("silver_processing") as silver_group:
        bronze_to_silver = BashOperator(
            task_id="bronze_to_silver",
            bash_command=(
                "docker exec cryptolake-spark-master "
                "/opt/spark/bin/spark-submit "
                "/opt/spark/work/src/processing/batch/bronze_to_silver.py"
            ),
        )

    # ── GOLD (dbt) ──────────────────────────────────────────
    with TaskGroup("gold_transformation") as gold_group:
        dbt_run = BashOperator(
            task_id="dbt_run",
            bash_command=(
                "cd /opt/airflow/src/transformation/dbt_cryptolake && "
                "/opt/dbt-venv/bin/dbt run --profiles-dir . --target prod"
            ),
        )
        dbt_test = BashOperator(
            task_id="dbt_test",
            bash_command=(
                "cd /opt/airflow/src/transformation/dbt_cryptolake && "
                "/opt/dbt-venv/bin/dbt test --profiles-dir . --target prod"
            ),
        )
        dbt_run >> dbt_test

    # ── DATA QUALITY (Fase 7) ───────────────────────────────
    with TaskGroup("data_quality") as quality_group:
        quality_checks = BashOperator(
            task_id="quality_checks",
            bash_command=(
                "docker exec cryptolake-spark-master "
                "/opt/spark/bin/spark-submit "
                "/opt/spark/work/src/quality/run_quality_checks.py"
            ),
        )

    # ── DEPENDENCIAS ────────────────────────────────────────
    (init_namespaces >> ingestion_group >> bronze_group
     >> silver_group >> gold_group >> quality_group)
PYEOF
```

### 9.8 — Actualizar el Makefile

Añade estos targets al final de tu Makefile:

```makefile
# ── Fase 7: Data Quality ────────────────────────────────────

quality-check: ## Run quality checks (all layers)
	docker exec cryptolake-spark-master \
	    /opt/spark/bin/spark-submit \
	    /opt/spark/work/src/quality/run_quality_checks.py

quality-bronze: ## Quality checks: Bronze only
	docker exec cryptolake-spark-master \
	    /opt/spark/bin/spark-submit \
	    /opt/spark/work/src/quality/run_quality_checks.py --layer bronze

quality-silver: ## Quality checks: Silver only
	docker exec cryptolake-spark-master \
	    /opt/spark/bin/spark-submit \
	    /opt/spark/work/src/quality/run_quality_checks.py --layer silver

quality-gold: ## Quality checks: Gold only
	docker exec cryptolake-spark-master \
	    /opt/spark/bin/spark-submit \
	    /opt/spark/work/src/quality/run_quality_checks.py --layer gold


# ── Fase 7: Serving ─────────────────────────────────────────

api-logs: ## Tail API logs
	docker logs -f cryptolake-api

dashboard-logs: ## Tail Dashboard logs
	docker logs -f cryptolake-dashboard


# ── Pipeline completo (actualizado) ─────────────────────────

pipeline: ## Full pipeline: Init → Bronze → Silver → Gold → Quality
	@echo "🚀 Running full CryptoLake pipeline..."
	$(MAKE) init-namespaces
	$(MAKE) bronze-load
	$(MAKE) silver-transform
	$(MAKE) dbt-run
	$(MAKE) dbt-test
	$(MAKE) quality-check
	@echo "✅ Pipeline complete!"
	@echo "   API Docs:  http://localhost:8000/docs"
	@echo "   Dashboard: http://localhost:8501"
```

### 9.9 — Probar todo

```bash
# 1. Reconstruir los contenedores (incluye api + dashboard nuevos)
docker compose up -d --build

# 2. Verificar que los servicios están corriendo
docker ps | grep -E "api|dashboard"

# 3. Abrir la documentación de la API
open http://localhost:8000/docs

# 4. Probar un endpoint manualmente
curl http://localhost:8000/api/v1/health
curl http://localhost:8000/api/v1/analytics/market-overview
curl http://localhost:8000/api/v1/prices/bitcoin?limit=5

# 5. Abrir el dashboard
open http://localhost:8501

# 6. Quality checks
make quality-check
```

### 9.10 — Git commit

```bash
cd ~/Projects/cryptolake
git add .
git commit -m "feat: Phase 7 - Data Quality + Serving Layer

- Custom data quality validators (Bronze, Silver, Gold)
  - 15+ checks: freshness, schema, duplicates, nulls, referential integrity
  - Results persisted to cryptolake.quality.check_results
- FastAPI REST API
  - GET /prices/{coin_id} - historical prices with MAs
  - GET /analytics/market-overview - market summary
  - GET /analytics/coins - coin statistics
  - GET /analytics/fear-greed - sentiment history
  - Connects to Spark Thrift Server via PyHive
- Streamlit Dashboard
  - Price charts with moving averages (Plotly)
  - Fear & Greed visualization
  - Market overview KPIs
- Docker services: api (port 8000), dashboard (port 8501)
- Updated DAG with data_quality task group"
```

---

## PARTE 10 (FASE 8): CI/CD + Terraform

### 10.1 — Conceptos: ¿Por qué necesitas CI/CD y Terraform?

Hasta ahora, todo funciona en tu Mac con Docker Compose. Eso está bien para
desarrollo, pero para demostrar profesionalidad necesitas:

1. **CI/CD (Continuous Integration / Continuous Deployment)**:
   - Cada vez que haces push a GitHub, se ejecutan tests automáticamente
   - Si los tests fallan, no puedes mergear el PR
   - Esto demuestra que tu proyecto **funciona** y que mantienes calidad

2. **Terraform (Infrastructure as Code)**:
   - Define toda la infraestructura (buckets S3, clusters) como código
   - Puedes replicar el entorno en AWS con un solo comando
   - Demuestra que piensas en producción, no solo en local

**¿Qué es GitHub Actions?**

Es el sistema de CI/CD integrado en GitHub. Defines workflows en YAML que
se ejecutan automáticamente cuando haces push, abres un PR, etc.

```
git push → GitHub Actions detecta el push → ejecuta el workflow → ✅ o ❌
```

**¿Qué es Terraform?**

Es una herramienta de HashiCorp que te permite definir infraestructura como
código. En vez de ir a la consola de AWS y crear buckets S3 a mano, defines
un archivo `.tf` y ejecutas `terraform apply`.

```
terraform plan   → Muestra qué va a crear/modificar/destruir
terraform apply  → Aplica los cambios
terraform destroy → Destruye todo lo creado
```

### 10.2 — GitHub Actions CI

```bash
mkdir -p .github/workflows
```

**Workflow principal de CI** — se ejecuta en cada PR y push a main:

```bash
cat > .github/workflows/ci.yml << 'YAMLEOF'
# ============================================================
# CI Pipeline — CryptoLake
# ============================================================
# Se ejecuta en cada PR y push a main/develop.
#
# Jobs:
# 1. lint       → Verifica formato y estilo de código
# 2. test       → Ejecuta tests unitarios con pytest
# 3. dbt-test   → Compila modelos dbt (verifica SQL)
# 4. docker-build → Verifica que todas las imágenes Docker construyen
#
# Diagrama de dependencias:
#   lint → test    ─┐
#   lint → dbt-test ├─→ docker-build
#                   ─┘
# ============================================================
name: CI Pipeline

on:
  pull_request:
    branches: [main, develop]
  push:
    branches: [main]

jobs:
  # ──────────────────────────────────────────────────────────
  # JOB 1: Linting
  # ──────────────────────────────────────────────────────────
  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install linters
        run: pip install ruff mypy

      - name: Ruff check (linting)
        run: ruff check src/ tests/

      - name: Ruff format (formatting)
        run: ruff format --check src/ tests/

  # ──────────────────────────────────────────────────────────
  # JOB 2: Unit Tests
  # ──────────────────────────────────────────────────────────
  test:
    runs-on: ubuntu-latest
    needs: lint
    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install dependencies
        run: pip install -e ".[dev]"

      - name: Run tests with coverage
        run: pytest tests/unit/ -v --cov=src --cov-report=xml

      - name: Upload coverage to Codecov
        uses: codecov/codecov-action@v4
        with:
          file: coverage.xml
        continue-on-error: true

  # ──────────────────────────────────────────────────────────
  # JOB 3: dbt Compile
  # ──────────────────────────────────────────────────────────
  # No ejecutamos dbt run (necesitaría Spark Thrift Server),
  # pero sí compilamos para verificar que el SQL es válido.
  # ──────────────────────────────────────────────────────────
  dbt-test:
    runs-on: ubuntu-latest
    needs: lint
    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install dbt
        run: pip install "dbt-core>=1.8" "dbt-spark[PyHive]>=1.8"

      - name: dbt compile
        run: |
          cd src/transformation/dbt_cryptolake
          dbt compile --profiles-dir . --target ci

  # ──────────────────────────────────────────────────────────
  # JOB 4: Docker Build
  # ──────────────────────────────────────────────────────────
  docker-build:
    runs-on: ubuntu-latest
    needs: [test, dbt-test]
    steps:
      - uses: actions/checkout@v4

      - uses: docker/setup-buildx-action@v3

      - name: Build all Docker images
        run: |
          docker compose build --parallel
          echo "✅ All images built successfully"
YAMLEOF
```

**Workflow de Data Quality** — se ejecuta con schedule (requiere entorno up):

```bash
cat > .github/workflows/data-quality.yml << 'YAMLEOF'
# ============================================================
# Data Quality Checks — CryptoLake
# ============================================================
# Se ejecuta manualmente o con schedule.
# Útil para verificar la calidad de datos en un entorno staging.
# ============================================================
name: Data Quality

on:
  workflow_dispatch:
    inputs:
      layer:
        description: "Layer to validate (bronze/silver/gold/all)"
        required: false
        default: "all"

jobs:
  quality:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Start services
        run: docker compose up -d --wait

      - name: Wait for services
        run: sleep 30

      - name: Run quality checks
        run: |
          if [ "${{ github.event.inputs.layer }}" = "all" ]; then
            make quality-check
          else
            make quality-${{ github.event.inputs.layer }}
          fi

      - name: Cleanup
        if: always()
        run: docker compose down
YAMLEOF
```

### 10.3 — Configurar dbt para CI

dbt necesita un target `ci` que compile sin necesitar un Spark Thrift Server
real. Añade este target a tu `profiles.yml`:

```bash
# En src/transformation/dbt_cryptolake/profiles.yml, añade:
```

Abre `src/transformation/dbt_cryptolake/profiles.yml` y añade el target `ci`:

```yaml
cryptolake:
  target: dev

  outputs:
    dev:
      type: spark
      method: thrift
      host: localhost
      port: 10000
      schema: gold
      threads: 1

    prod:
      type: spark
      method: thrift
      host: spark-thrift
      port: 10000
      schema: gold
      threads: 2

    # CI: solo compila SQL sin conectar a Spark
    ci:
      type: spark
      method: thrift
      host: localhost
      port: 10000
      schema: gold
      threads: 1
```

> **Nota**: `dbt compile` con el target `ci` solo valida la sintaxis SQL.
> No se conecta realmente a Spark (no ejecuta las queries). Pero si `dbt compile`
> falla, significa que hay errores en tus modelos SQL.

### 10.4 — Tests unitarios básicos

```bash
mkdir -p tests/unit
```

**tests/conftest.py:**

```bash
cat > tests/conftest.py << 'PYEOF'
"""Pytest fixtures para CryptoLake."""
import pytest
PYEOF
```

**tests/unit/test_schemas.py** — verifica que los modelos Pydantic son correctos:

```bash
cat > tests/unit/test_schemas.py << 'PYEOF'
"""Tests unitarios para los schemas de la API."""
from datetime import date

from src.serving.api.models.schemas import (
    CoinResponse,
    FearGreedResponse,
    HealthResponse,
    MarketOverview,
    PriceResponse,
)


def test_price_response():
    """PriceResponse se instancia correctamente con datos mínimos."""
    p = PriceResponse(
        coin_id="bitcoin",
        price_date=date(2025, 1, 15),
        price_usd=95000.0,
    )
    assert p.coin_id == "bitcoin"
    assert p.price_usd == 95000.0
    assert p.moving_avg_7d is None  # Optional


def test_coin_response():
    """CoinResponse se instancia correctamente."""
    c = CoinResponse(
        coin_id="ethereum",
        all_time_high=4000.0,
        avg_price=3200.0,
    )
    assert c.coin_id == "ethereum"
    assert c.first_tracked_date is None  # Optional


def test_market_overview():
    """MarketOverview con datos mínimos."""
    m = MarketOverview(total_coins=8, total_fact_rows=5000)
    assert m.total_coins == 8


def test_fear_greed_response():
    """FearGreedResponse con todos los campos."""
    fg = FearGreedResponse(
        index_date=date(2025, 2, 1),
        fear_greed_value=25,
        classification="Extreme Fear",
    )
    assert fg.fear_greed_value == 25
    assert fg.classification == "Extreme Fear"


def test_health_response():
    """HealthResponse healthy."""
    h = HealthResponse(
        status="healthy",
        thrift_connected=True,
        tables_available=3,
    )
    assert h.status == "healthy"
PYEOF
```

**tests/unit/test_validators.py** — tests para los quality checkers:

```bash
cat > tests/unit/test_validators.py << 'PYEOF'
"""Tests unitarios para el módulo de quality."""
from src.quality.validators import CheckResult, CheckStatus


def test_check_result_to_dict():
    """CheckResult se serializa correctamente."""
    r = CheckResult(
        check_name="test_check",
        layer="bronze",
        table_name="test_table",
        status=CheckStatus.PASSED,
        metric_value=100.0,
        threshold=50.0,
        message="All good",
    )
    d = r.to_dict()
    assert d["status"] == "passed"
    assert d["metric_value"] == 100.0
    assert "checked_at" in d


def test_check_status_values():
    """CheckStatus tiene los valores esperados."""
    assert CheckStatus.PASSED.value == "passed"
    assert CheckStatus.FAILED.value == "failed"
    assert CheckStatus.WARNING.value == "warning"
    assert CheckStatus.ERROR.value == "error"
PYEOF
```

### 10.5 — Terraform: Infrastructure as Code

```bash
mkdir -p terraform/modules/storage
mkdir -p terraform/environments/local
mkdir -p terraform/environments/aws
```

**terraform/modules/storage/main.tf** — módulo reutilizable para buckets:

```bash
cat > terraform/modules/storage/main.tf << 'HCLEOF'
# ============================================================
# Módulo: storage
# ============================================================
# Crea los buckets S3 para el Lakehouse (Bronze, Silver, Gold).
# En local usamos MinIO (S3-compatible).
# En AWS, se crean buckets S3 reales.
# ============================================================

variable "environment" {
  type        = string
  description = "Environment name (local, staging, production)"
}

variable "project_name" {
  type    = string
  default = "cryptolake"
}

# ── S3 Buckets ──────────────────────────────────────────────

resource "aws_s3_bucket" "bronze" {
  bucket = "${var.project_name}-${var.environment}-bronze"

  tags = {
    Project     = var.project_name
    Environment = var.environment
    Layer       = "bronze"
  }
}

resource "aws_s3_bucket" "silver" {
  bucket = "${var.project_name}-${var.environment}-silver"

  tags = {
    Project     = var.project_name
    Environment = var.environment
    Layer       = "silver"
  }
}

resource "aws_s3_bucket" "gold" {
  bucket = "${var.project_name}-${var.environment}-gold"

  tags = {
    Project     = var.project_name
    Environment = var.environment
    Layer       = "gold"
  }
}

# ── Lifecycle: archivar Bronze viejo a Glacier ──────────────

resource "aws_s3_bucket_lifecycle_configuration" "bronze_lifecycle" {
  bucket = aws_s3_bucket.bronze.id

  rule {
    id     = "archive-old-data"
    status = "Enabled"

    transition {
      days          = 90
      storage_class = "GLACIER"
    }
  }
}

# ── Versionado: proteger datos raw en Bronze ────────────────

resource "aws_s3_bucket_versioning" "bronze_versioning" {
  bucket = aws_s3_bucket.bronze.id

  versioning_configuration {
    status = "Enabled"
  }
}

# ── Outputs ─────────────────────────────────────────────────

output "bronze_bucket_name" {
  value = aws_s3_bucket.bronze.id
}

output "silver_bucket_name" {
  value = aws_s3_bucket.silver.id
}

output "gold_bucket_name" {
  value = aws_s3_bucket.gold.id
}
HCLEOF
```

**terraform/environments/aws/main.tf** — entorno AWS real:

```bash
cat > terraform/environments/aws/main.tf << 'HCLEOF'
# ============================================================
# CryptoLake — AWS Environment
# ============================================================
# Despliega el storage layer en AWS S3.
# Ejecutar:
#   cd terraform/environments/aws
#   terraform init
#   terraform plan
#   terraform apply
# ============================================================

terraform {
  required_version = ">= 1.8"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # En producción: backend remoto en S3
  # backend "s3" {
  #   bucket = "cryptolake-terraform-state"
  #   key    = "aws/terraform.tfstate"
  #   region = "us-east-1"
  # }
}

provider "aws" {
  region = var.aws_region
}

variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "environment" {
  type    = string
  default = "staging"
}

# ── Módulo de Storage ───────────────────────────────────────

module "storage" {
  source      = "../../modules/storage"
  environment = var.environment
}

# ── Outputs ─────────────────────────────────────────────────

output "bronze_bucket" {
  value = module.storage.bronze_bucket_name
}

output "silver_bucket" {
  value = module.storage.silver_bucket_name
}

output "gold_bucket" {
  value = module.storage.gold_bucket_name
}
HCLEOF
```

**terraform/environments/local/main.tf** — documenta el entorno local:

```bash
cat > terraform/environments/local/main.tf << 'HCLEOF'
# ============================================================
# CryptoLake — Local Environment
# ============================================================
# En local, la infraestructura se gestiona con Docker Compose.
# MinIO simula S3, no necesita Terraform.
#
# Este archivo existe como documentación y para demostrar que
# la estructura de Terraform soporta múltiples entornos.
#
# Buckets locales (creados por minio-init en docker-compose):
#   - cryptolake-bronze
#   - cryptolake-silver
#   - cryptolake-gold
#   - cryptolake-checkpoints
# ============================================================

terraform {
  required_version = ">= 1.8"
}

output "info" {
  value = "Local environment uses Docker Compose + MinIO. No Terraform resources needed."
}
HCLEOF
```

### 10.6 — Archivos de configuración de calidad de código

**.pre-commit-config.yaml:**

```bash
cat > .pre-commit-config.yaml << 'YAMLEOF'
repos:
  - repo: https://github.com/astral-sh/ruff-pre-commit
    rev: v0.3.0
    hooks:
      - id: ruff
        args: [--fix]
      - id: ruff-format
YAMLEOF
```

### 10.7 — Probar CI localmente

Antes de hacer push, verifica que el CI pasaría:

```bash
# Instalar herramientas de linting
pip install ruff mypy

# Lint
ruff check src/ tests/
ruff format --check src/ tests/

# Tests
pip install -e ".[dev]"
pytest tests/unit/ -v

# dbt compile
cd src/transformation/dbt_cryptolake
dbt compile --profiles-dir . --target ci
cd ../../..

# Docker build
docker compose build --parallel
```

### 10.8 — Git commit

```bash
cd ~/Projects/cryptolake
git add .
git commit -m "feat: Phase 8 - CI/CD + Terraform

- GitHub Actions CI pipeline
  - lint (ruff check + format)
  - test (pytest + coverage)
  - dbt-test (compile models)
  - docker-build (verify images)
- GitHub Actions data quality workflow (manual trigger)
- Terraform modules
  - storage: S3 buckets with lifecycle & versioning
  - aws environment: staging deployment
  - local environment: documentation
- Unit tests for API schemas and quality validators
- Pre-commit hooks (ruff)"
```

---

## Resumen de archivos creados

### Fase 7: Data Quality + Serving

```
src/quality/
├── __init__.py
├── validators.py                  # Framework: BronzeValidator, SilverValidator, GoldValidator
└── run_quality_checks.py          # spark-submit ejecutable

src/serving/
├── __init__.py
├── api/
│   ├── __init__.py
│   ├── main.py                    # FastAPI app (endpoints documentados en /docs)
│   ├── database.py                # Conexión PyHive → Spark Thrift Server
│   ├── routes/
│   │   ├── __init__.py
│   │   ├── prices.py              # GET /prices/{coin_id}
│   │   ├── analytics.py           # GET /analytics/market-overview, /coins, /fear-greed
│   │   └── health.py              # GET /health
│   └── models/
│       ├── __init__.py
│       └── schemas.py             # Pydantic: PriceResponse, CoinResponse, etc.
└── dashboard/
    └── app.py                     # Streamlit dashboard

docker/api/
└── Dockerfile                     # Imagen FastAPI + PyHive

src/orchestration/dags/
└── dag_full_pipeline.py           # (actualizado con data_quality group)
```

### Fase 8: CI/CD + Terraform

```
.github/workflows/
├── ci.yml                         # lint → test → dbt-test → docker-build
└── data-quality.yml               # Quality checks (manual trigger)

terraform/
├── modules/
│   └── storage/
│       └── main.tf                # Buckets S3 + lifecycle + versioning
└── environments/
    ├── aws/
    │   └── main.tf                # Staging/production en AWS
    └── local/
        └── main.tf                # Documentación del entorno local

tests/
├── conftest.py
└── unit/
    ├── test_schemas.py            # Tests de modelos Pydantic
    └── test_validators.py         # Tests de quality framework

.pre-commit-config.yaml            # Ruff hooks
```

### URLs de acceso

| Servicio | URL | Puerto |
|----------|-----|--------|
| API REST (Swagger docs) | http://localhost:8000/docs | 8000 |
| API REST (ReDoc) | http://localhost:8000/redoc | 8000 |
| Streamlit Dashboard | http://localhost:8501 | 8501 |
| Airflow UI | http://localhost:8083 | 8083 |
| MinIO Console | http://localhost:9001 | 9001 |
| Spark UI | http://localhost:8082 | 8082 |
| Kafka UI | http://localhost:8080 | 8080 |
