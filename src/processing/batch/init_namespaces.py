"""Crear namespaces Iceberg necesarios para CryptoLake."""

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

for ns in ["default", "bronze", "silver", "gold", "staging"]:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS cryptolake.{ns}")
    print(f"✅ Namespace cryptolake.{ns} OK")

spark.stop()
