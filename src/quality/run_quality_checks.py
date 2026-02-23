"""
Data quality check runner.

Usage:
    spark-submit src/quality/run_quality_checks.py
    spark-submit src/quality/run_quality_checks.py --layer bronze
    spark-submit src/quality/run_quality_checks.py --layer silver --layer gold
"""

import argparse
import logging
import sys
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)

sys.path.insert(0, "/opt/spark/work")
from src.quality.validators import (
    BronzeValidator,
    CheckStatus,
    GoldValidator,
    SilverValidator,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

RESULTS_TABLE = "cryptolake.quality.check_results"
RESULTS_SCHEMA = StructType(
    [
        StructField("check_name", StringType(), False),
        StructField("layer", StringType(), False),
        StructField("table_name", StringType(), False),
        StructField("status", StringType(), False),
        StructField("metric_value", DoubleType(), True),
        StructField("threshold", DoubleType(), True),
        StructField("message", StringType(), True),
        StructField("checked_at", StringType(), False),
        StructField("run_id", StringType(), False),
    ]
)


def persist_results(spark, results, run_id):
    """Save results to cryptolake.quality.check_results."""
    rows = [dict(**r.to_dict(), run_id=run_id) for r in results]
    df = spark.createDataFrame(rows, schema=RESULTS_SCHEMA)
    spark.sql("CREATE NAMESPACE IF NOT EXISTS cryptolake.quality")
    df.writeTo(RESULTS_TABLE).using("iceberg").createOrReplace()
    logger.info(f"{len(rows)} results saved to {RESULTS_TABLE}")


def print_summary(results):
    """Print summary and return True if all checks pass."""
    print("\n" + "=" * 60)
    print("DATA QUALITY SUMMARY -- CryptoLake")
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
        print(f"\n  {layer.upper()} ({len(checks)} checks)")
        print(f"     Passed: {p}  Failed: {f}  Warnings: {w}")
        for c in checks:
            if c.status != CheckStatus.PASSED:
                markers = {"failed": "[-]", "warning": "[!]", "error": "[X]"}
                short = c.table_name.split(".")[-1]
                print(
                    f"     {markers.get(c.status.value, '?')} {c.check_name} ({short}): {c.message}"
                )

    total = len(results)
    ok = sum(1 for r in results if r.status == CheckStatus.PASSED)
    rate = round(ok / total * 100, 1) if total > 0 else 0
    marker = "[+]" if not has_failures else "[-]"
    print(f"\n{'=' * 60}")
    print(f"  {marker} Pass rate: {rate}% ({ok}/{total})")
    print(f"{'=' * 60}\n")
    return not has_failures


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--layer", action="append", default=None, choices=["bronze", "silver", "gold"]
    )
    args = parser.parse_args()
    layers = args.layer or ["bronze", "silver", "gold"]

    spark = SparkSession.builder.appName("CryptoLake-Quality").getOrCreate()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    all_results = []

    logger.info(f"Quality checks (run_id={run_id}, layers={layers})")

    if "bronze" in layers:
        all_results.extend(BronzeValidator(spark).check_all())
    if "silver" in layers:
        all_results.extend(SilverValidator(spark).check_all())
    if "gold" in layers:
        all_results.extend(GoldValidator(spark).check_all())

    try:
        persist_results(spark, all_results, run_id)
    except Exception as e:
        logger.warning(f"Could not persist results: {e}")

    ok = print_summary(all_results)
    spark.stop()

    if not ok:
        sys.exit(1)


if __name__ == "__main__":
    main()
