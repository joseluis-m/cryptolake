# ============================================================
# CryptoLake — Airflow Dockerfile
# ============================================================
# Base: Apache Airflow 2.9.3 on Python 3.11
#
# Includes:
# - Docker CLI for executing spark-submit on sibling containers
# - dbt-spark in isolated virtualenv (avoids protobuf conflicts)
# - Airflow Spark provider + project Python dependencies
# ============================================================

FROM apache/airflow:2.9.3-python3.11

# -- System dependencies (as root) --------------------------
USER root

# Docker CLI: allows Airflow to run "docker exec" on Spark containers.
# This is a common pattern for local dev where Airflow orchestrates
# sibling Docker containers via the shared Docker socket.
RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential ca-certificates curl && \
    install -m 0755 -d /etc/apt/keyrings && \
    curl -fsSL https://download.docker.com/linux/debian/gpg -o /etc/apt/keyrings/docker.asc && \
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/debian bookworm stable" > /etc/apt/sources.list.d/docker.list && \
    apt-get update && \
    apt-get install -y --no-install-recommends docker-ce-cli && \
    rm -rf /var/lib/apt/lists/*

# Allow airflow user to use Docker socket without root
RUN groupadd -f docker && usermod -aG docker airflow

# Create dbt venv directory (owned by airflow)
RUN mkdir -p /opt/dbt-venv && chown airflow /opt/dbt-venv

# -- Python packages (as airflow) ---------------------------
USER airflow

# Airflow-compatible packages (no dbt here to avoid conflicts)
RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark==4.7.1 \
    requests==2.31.0 \
    pydantic==2.5.0 \
    pydantic-settings==2.1.0 \
    structlog==24.1.0

# dbt in isolated virtualenv to prevent protobuf version
# conflicts between Airflow and dbt-spark
RUN python -m venv /opt/dbt-venv && \
    /opt/dbt-venv/bin/pip install --no-cache-dir \
    "dbt-spark[PyHive]==1.8.0" \
    "dbt-core==1.8.0" \
    "protobuf>=4.25.3,<5.0.0"
