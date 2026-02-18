# ============================================================
# Apache Airflow para CryptoLake
# ============================================================
# Incluye:
# - Providers de Airflow para Spark
# - Docker CLI para ejecutar spark-submit en contenedores Spark
# - dbt-spark para la transformación Gold
# - Dependencias Python del proyecto
# ============================================================
FROM apache/airflow:2.9.3-python3.11

# ── Instalar como root ──────────────────────────────────────
USER root

# Docker CLI: para poder ejecutar "docker exec" en el contenedor de Spark.
# Esto es un patrón común en entornos de desarrollo local donde Airflow
# orquesta contenedores Docker hermanos (sibling containers).
RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential ca-certificates curl && \
    install -m 0755 -d /etc/apt/keyrings && \
    curl -fsSL https://download.docker.com/linux/debian/gpg -o /etc/apt/keyrings/docker.asc && \
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/debian bookworm stable" > /etc/apt/sources.list.d/docker.list && \
    apt-get update && \
    apt-get install -y --no-install-recommends docker-ce-cli && \
    rm -rf /var/lib/apt/lists/*

# Añadir el usuario airflow al grupo docker para que pueda
# usar el Docker socket sin ser root
RUN groupadd -f docker && usermod -aG docker airflow

# Crear directorio para dbt venv (como root, luego dar permisos a airflow)
RUN mkdir -p /opt/dbt-venv && chown airflow /opt/dbt-venv

# ── Instalar paquetes Python como airflow ────────────────────
USER airflow

# Paquetes compatibles con Airflow (sin dbt)
RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark==4.7.1 \
    requests==2.31.0 \
    pydantic==2.5.0 \
    pydantic-settings==2.1.0 \
    structlog==24.1.0

# dbt en virtualenv aislado (evita conflicto protobuf con Airflow)
RUN python -m venv /opt/dbt-venv && \
    /opt/dbt-venv/bin/pip install --no-cache-dir \
    "dbt-spark[PyHive]==1.8.0" \
    "dbt-core==1.8.0" \
    "protobuf>=4.25.3,<5.0.0"
