# ============================================================
# CryptoLake — Spark Dockerfile
# ============================================================
# Base: Apache Spark 3.5.3 with PySpark (official image)
# Docs: https://hub.docker.com/r/apache/spark-py
#
# Adds:
# - Python libraries for ingestion and processing
# - Iceberg 1.5.2 JARs (table format + S3/MinIO support)
# - Kafka JARs for Structured Streaming
# - Custom spark-defaults.conf for Iceberg catalog config
# ============================================================

FROM apache/spark:3.5.3-python3

USER root

# -- Python dependencies -------------------------------------
RUN pip install --no-cache-dir \
    pyiceberg[s3fs]==0.7.1 \
    pyarrow==15.0.1 \
    kafka-python==2.0.2 \
    requests==2.31.0 \
    pydantic==2.5.0 \
    pydantic-settings==2.1.0 \
    structlog==24.1.0

# -- Iceberg JARs -------------------------------------------
# Spark requires Java libraries (.jar) to read/write Iceberg
# tables and connect to S3-compatible storage (MinIO).
ENV ICEBERG_VERSION=1.5.2

RUN curl -L -o /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar \
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar" \
 && curl -L -o /opt/spark/jars/iceberg-aws-bundle-${ICEBERG_VERSION}.jar \
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar"

# -- Kafka JARs (Structured Streaming) ----------------------
ENV SPARK_VERSION=3.5.3
ENV SCALA_VERSION=2.12

RUN curl -L -o /opt/spark/jars/spark-sql-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar \
    "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar" \
 && curl -L -o /opt/spark/jars/kafka-clients-3.6.1.jar \
    "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.6.1/kafka-clients-3.6.1.jar" \
 && curl -L -o /opt/spark/jars/spark-token-provider-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar \
    "https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-token-provider-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar" \
 && curl -L -o /opt/spark/jars/commons-pool2-2.12.0.jar \
    "https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar"

# -- Spark configuration ------------------------------------
COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf

# Allow imports like "from src.config import settings"
ENV PYTHONPATH="/opt/spark/work:${PYTHONPATH}"

# Run as non-root (spark user, UID 185)
USER spark
