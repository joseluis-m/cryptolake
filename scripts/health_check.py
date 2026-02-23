#!/usr/bin/env python3
"""
Health check: verifies all CryptoLake services are running.

Usage:
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
    print("CryptoLake Health Check")
    print("=" * 50)

    result = subprocess.run(
        ["docker", "ps", "--format", "{{.Names}}"],
        capture_output=True,
        text=True,
    )
    running = set(result.stdout.strip().split("\n"))

    healthy = 0
    total = len(SERVICES)

    for container, label in SERVICES.items():
        status = "OK" if container in running else "NOT RUNNING"
        print(f"  [{status:>11}]  {label}")
        if container in running:
            healthy += 1

    print("=" * 50)
    print(f"  {healthy}/{total} services running")

    if healthy < total:
        print("\n  Tip: run 'make up' to start all services")
        sys.exit(1)


if __name__ == "__main__":
    check_services()
