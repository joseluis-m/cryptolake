#!/usr/bin/env python3
"""
Health check: verifica que todos los servicios de CryptoLake están running.

Uso:
    python scripts/health_check.py
"""
import subprocess
import sys


SERVICES = {
    "cryptolake-minio": "MinIO (Storage)",
    "cryptolake-kafka": "Kafka (Streaming)",
    "cryptolake-spark-master": "Spark Master",
    "cryptolake-spark-worker": "Spark Worker",
    "cryptolake-spark-thrift": "Spark Thrift Server",
    "cryptolake-airflow-webserver": "Airflow Webserver",
    "cryptolake-airflow-scheduler": "Airflow Scheduler",
    "cryptolake-iceberg-rest": "Iceberg REST Catalog",
    "cryptolake-api": "FastAPI",
    "cryptolake-dashboard": "Streamlit Dashboard",
}


def check_services():
    print("🔍 CryptoLake Health Check")
    print("=" * 50)

    result = subprocess.run(
        ["docker", "ps", "--format", "{{.Names}}"],
        capture_output=True, text=True,
    )
    running = set(result.stdout.strip().split("\n"))

    healthy = 0
    total = len(SERVICES)

    for container, label in SERVICES.items():
        if container in running:
            print(f"  ✅ {label:<30} running")
            healthy += 1
        else:
            print(f"  ❌ {label:<30} NOT RUNNING")

    print("=" * 50)
    print(f"  {'✅' if healthy == total else '⚠️'} {healthy}/{total} services running")

    if healthy < total:
        print("\n  Tip: run 'make up' to start all services")
        sys.exit(1)


if __name__ == "__main__":
    check_services()
