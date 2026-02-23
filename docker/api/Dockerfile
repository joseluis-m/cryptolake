# ============================================================
# CryptoLake — FastAPI Dockerfile
# ============================================================
# Serves the REST API on port 8000.
# Connects to Spark Thrift Server via PyHive for Gold layer queries.
# Source code is mounted as a volume in docker-compose.
# ============================================================

FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    fastapi==0.110.0 \
    uvicorn==0.27.0 \
    pyhive==0.7.0 \
    thrift==0.16.0 \
    pure-sasl==0.6.2 \
    pydantic==2.5.0 \
    pydantic-settings==2.1.0

CMD ["uvicorn", "src.serving.api.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
