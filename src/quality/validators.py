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
    checked_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

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
            f"{icon} [{result.layer}] {result.check_name} on {result.table_name}: {result.message}"
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
        return (
            self.spark.sql(f"SELECT COUNT(*) AS cnt FROM {table} WHERE {col} IS NULL").first().cnt
        )

    def _distinct(self, table: str, col: str) -> int:
        return self.spark.sql(f"SELECT COUNT(DISTINCT {col}) AS cnt FROM {table}").first().cnt

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
                "coin_id",
                "timestamp_ms",
                "price_usd",
                "market_cap_usd",
                "volume_24h_usd",
                "_ingested_at",
                "_source",
                "_loaded_at",
            ],
            "freshness_col": "_loaded_at",
        },
        "cryptolake.bronze.fear_greed": {
            "min_rows": 30,
            "required_columns": [
                "value",
                "classification",
                "timestamp",
                "_ingested_at",
                "_source",
                "_loaded_at",
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
        self._add(
            CheckResult(
                check_name="table_exists",
                layer="bronze",
                table_name=table,
                status=CheckStatus.PASSED if exists else CheckStatus.FAILED,
                message=f"Table {'exists' if exists else 'NOT FOUND'}",
            )
        )

    def _check_row_count(self, table: str, min_rows: int):
        count = self._count(table)
        self._add(
            CheckResult(
                check_name="min_row_count",
                layer="bronze",
                table_name=table,
                status=CheckStatus.PASSED if count >= min_rows else CheckStatus.FAILED,
                metric_value=float(count),
                threshold=float(min_rows),
                message=f"Rows: {count} (min: {min_rows})",
            )
        )

    def _check_schema(self, table: str, required: list[str]):
        df = self.spark.sql(f"SELECT * FROM {table} LIMIT 0")
        missing = set(required) - set(df.columns)
        self._add(
            CheckResult(
                check_name="schema_check",
                layer="bronze",
                table_name=table,
                status=CheckStatus.PASSED if not missing else CheckStatus.FAILED,
                message=f"Missing: {missing}" if missing else "All columns present",
            )
        )

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
                self._add(
                    CheckResult(
                        check_name="data_freshness",
                        layer="bronze",
                        table_name=table,
                        status=CheckStatus.WARNING,
                        message="No timestamp values found",
                    )
                )
                return

            threshold = 48
            status = (
                CheckStatus.PASSED
                if hours <= threshold
                else CheckStatus.WARNING
                if hours <= 72
                else CheckStatus.FAILED
            )
            self._add(
                CheckResult(
                    check_name="data_freshness",
                    layer="bronze",
                    table_name=table,
                    status=status,
                    metric_value=float(hours),
                    threshold=float(threshold),
                    message=f"Last load: {hours}h ago (threshold: {threshold}h)",
                )
            )
        except Exception as e:
            self._add(
                CheckResult(
                    check_name="data_freshness",
                    layer="bronze",
                    table_name=table,
                    status=CheckStatus.ERROR,
                    message=f"Error: {e}",
                )
            )


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
            self._add(
                CheckResult(
                    check_name="table_exists",
                    layer="silver",
                    table_name=table,
                    status=CheckStatus.FAILED,
                    message="NOT FOUND",
                )
            )
            return

        # No duplicados: (coin_id, price_date) único
        dups = (
            self.spark.sql(f"""
            SELECT COUNT(*) AS cnt FROM (
                SELECT coin_id, price_date, COUNT(*) AS n
                FROM {table} GROUP BY coin_id, price_date HAVING n > 1
            )
        """)
            .first()
            .cnt
        )
        self._add(
            CheckResult(
                check_name="no_duplicates",
                layer="silver",
                table_name=table,
                status=CheckStatus.PASSED if dups == 0 else CheckStatus.FAILED,
                metric_value=float(dups),
                threshold=0.0,
                message=f"Duplicate (coin_id, price_date): {dups}",
            )
        )

        # Precios positivos
        neg = (
            self.spark.sql(f"SELECT COUNT(*) AS cnt FROM {table} WHERE price_usd <= 0").first().cnt
        )
        self._add(
            CheckResult(
                check_name="positive_prices",
                layer="silver",
                table_name=table,
                status=CheckStatus.PASSED if neg == 0 else CheckStatus.FAILED,
                metric_value=float(neg),
                threshold=0.0,
                message=f"price_usd <= 0: {neg}",
            )
        )

        # Not null en columnas clave
        for col in ["coin_id", "price_date", "price_usd"]:
            n = self._nulls(table, col)
            self._add(
                CheckResult(
                    check_name=f"not_null_{col}",
                    layer="silver",
                    table_name=table,
                    status=CheckStatus.PASSED if n == 0 else CheckStatus.FAILED,
                    metric_value=float(n),
                    threshold=0.0,
                    message=f"Nulls in {col}: {n}",
                )
            )

        # No fechas futuras
        future = (
            self.spark.sql(f"SELECT COUNT(*) AS cnt FROM {table} WHERE price_date > CURRENT_DATE()")
            .first()
            .cnt
        )
        self._add(
            CheckResult(
                check_name="no_future_dates",
                layer="silver",
                table_name=table,
                status=CheckStatus.PASSED if future == 0 else CheckStatus.FAILED,
                metric_value=float(future),
                threshold=0.0,
                message=f"Future dates: {future}",
            )
        )

    def _check_fear_greed(self):
        table = "cryptolake.silver.fear_greed"
        if not self._exists(table):
            self._add(
                CheckResult(
                    check_name="table_exists",
                    layer="silver",
                    table_name=table,
                    status=CheckStatus.FAILED,
                    message="NOT FOUND",
                )
            )
            return

        # Valores entre 0 y 100
        out = (
            self.spark.sql(f"""
            SELECT COUNT(*) AS cnt FROM {table}
            WHERE fear_greed_value < 0 OR fear_greed_value > 100
        """)
            .first()
            .cnt
        )
        self._add(
            CheckResult(
                check_name="value_range_0_100",
                layer="silver",
                table_name=table,
                status=CheckStatus.PASSED if out == 0 else CheckStatus.FAILED,
                metric_value=float(out),
                threshold=0.0,
                message=f"Out of [0,100]: {out}",
            )
        )

        # Clasificaciones válidas
        valid = ["Extreme Fear", "Fear", "Neutral", "Greed", "Extreme Greed"]
        in_clause = ", ".join(f"'{v}'" for v in valid)
        invalid = (
            self.spark.sql(f"""
            SELECT COUNT(*) AS cnt FROM {table}
            WHERE classification NOT IN ({in_clause})
        """)
            .first()
            .cnt
        )
        self._add(
            CheckResult(
                check_name="valid_classifications",
                layer="silver",
                table_name=table,
                status=CheckStatus.PASSED if invalid == 0 else CheckStatus.FAILED,
                metric_value=float(invalid),
                threshold=0.0,
                message=f"Invalid classifications: {invalid}",
            )
        )

        # Not null en index_date (PK de la tabla)
        n = self._nulls(table, "index_date")
        self._add(
            CheckResult(
                check_name="not_null_index_date",
                layer="silver",
                table_name=table,
                status=CheckStatus.PASSED if n == 0 else CheckStatus.FAILED,
                metric_value=float(n),
                threshold=0.0,
                message=f"Nulls in index_date: {n}",
            )
        )


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
            self._add(
                CheckResult(
                    check_name="table_exists",
                    layer="gold",
                    table_name=table,
                    status=CheckStatus.FAILED,
                    message="NOT FOUND",
                )
            )
            return

        # coin_id es PK → debe ser único
        total = self._count(table)
        distinct = self._distinct(table, "coin_id")
        self._add(
            CheckResult(
                check_name="unique_pk_coin_id",
                layer="gold",
                table_name=table,
                status=CheckStatus.PASSED if total == distinct else CheckStatus.FAILED,
                metric_value=float(total - distinct),
                threshold=0.0,
                message=f"Total: {total}, Distinct: {distinct}",
            )
        )

    def _check_dim_dates(self):
        table = "cryptolake.gold.dim_dates"
        if not self._exists(table):
            self._add(
                CheckResult(
                    check_name="table_exists",
                    layer="gold",
                    table_name=table,
                    status=CheckStatus.FAILED,
                    message="NOT FOUND",
                )
            )
            return

        # Continuidad: no gaps en date_day
        row = self.spark.sql(f"""
            SELECT COUNT(*) AS total,
                   DATEDIFF(MAX(date_day), MIN(date_day)) + 1 AS expected
            FROM {table}
        """).first()
        gaps = row.expected - row.total
        self._add(
            CheckResult(
                check_name="no_date_gaps",
                layer="gold",
                table_name=table,
                status=CheckStatus.PASSED if gaps == 0 else CheckStatus.WARNING,
                metric_value=float(gaps),
                threshold=0.0,
                message=f"Missing dates: {gaps} (total: {row.total})",
            )
        )

    def _check_fact_market_daily(self):
        table = "cryptolake.gold.fact_market_daily"
        if not self._exists(table):
            self._add(
                CheckResult(
                    check_name="table_exists",
                    layer="gold",
                    table_name=table,
                    status=CheckStatus.FAILED,
                    message="NOT FOUND",
                )
            )
            return

        count = self._count(table)
        self._add(
            CheckResult(
                check_name="has_rows",
                layer="gold",
                table_name=table,
                status=CheckStatus.PASSED if count > 0 else CheckStatus.FAILED,
                metric_value=float(count),
                threshold=1.0,
                message=f"Rows: {count}",
            )
        )

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
        orphans = (
            self.spark.sql(f"""
            SELECT COUNT(*) AS cnt FROM {fact} f
            LEFT JOIN {dim_coins} d ON f.coin_id = d.coin_id
            WHERE d.coin_id IS NULL
        """)
            .first()
            .cnt
        )
        self._add(
            CheckResult(
                check_name="fk_coin_id",
                layer="gold",
                table_name=fact,
                status=CheckStatus.PASSED if orphans == 0 else CheckStatus.FAILED,
                metric_value=float(orphans),
                threshold=0.0,
                message=f"Orphan coin_id: {orphans}",
            )
        )

        # price_date → dim_dates.date_day
        orphans = (
            self.spark.sql(f"""
            SELECT COUNT(*) AS cnt FROM {fact} f
            LEFT JOIN {dim_dates} d ON f.price_date = d.date_day
            WHERE d.date_day IS NULL
        """)
            .first()
            .cnt
        )
        self._add(
            CheckResult(
                check_name="fk_price_date",
                layer="gold",
                table_name=fact,
                status=CheckStatus.PASSED if orphans == 0 else CheckStatus.FAILED,
                metric_value=float(orphans),
                threshold=0.0,
                message=f"Orphan price_date: {orphans}",
            )
        )
