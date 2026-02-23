# Setup Guide -- CryptoLake

Complete guide to set up the project from scratch.

## Prerequisites

| Software | Minimum Version | Verify with |
|----------|----------------|-------------|
| Docker Desktop | 24+ | `docker --version` |
| Docker Compose | v2+ | `docker compose version` |
| Python | 3.11+ | `python3 --version` |
| Make | any | `make --version` |
| Git | any | `git --version` |

**Recommended Docker resources**: 6 CPU cores, 8 GB RAM, 40 GB disk.

## Step 1: Clone and Configure

```bash
git clone https://github.com/joseluis-m/cryptolake.git
cd cryptolake
cp .env.example .env
```

## Step 2: Start Services

```bash
make up
```

This starts 12+ containers. Wait approximately 60 seconds for all services
to become healthy.

Verify with:

```bash
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```

## Step 3: Run the Pipeline

```bash
# Full pipeline step by step
make init-namespaces    # Create Iceberg namespaces
make bronze-load        # Ingest data from APIs -> Bronze
make silver-transform   # Bronze -> Silver (clean + deduplicate)
make dbt-run            # Silver -> Gold (star schema with dbt)
make dbt-test           # Validate dbt models
make quality-check      # Quality checks (15+ validations)

# Or all at once:
make pipeline
```

## Step 4: Verify Results

```bash
# REST API
curl http://localhost:8000/api/v1/health
curl http://localhost:8000/api/v1/analytics/market-overview

# Dashboard
open http://localhost:8501

# Airflow UI
open http://localhost:8083   # admin / admin
```

## Step 5: Local Development

```bash
# Create Python virtualenv
python3 -m venv .venv
source .venv/bin/activate

# Install development tools
pip install -e ".[dev]"

# Linting
ruff check src/ tests/
ruff format src/ tests/

# Unit tests
pytest tests/unit/ -v

# dbt compile (verify SQL without executing)
make dbt-run
```

## Step 6: Real-Time Streaming (Optional)

The streaming pipeline is independent from the batch pipeline and demonstrates
Kafka + Spark Structured Streaming. It requires a local Python virtual
environment with additional dependencies.

```bash
# Create venv and install dependencies (one time)
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt confluent-kafka websockets

# Start streaming (Spark consumer in background + Binance producer in foreground)
make stream-start

# Ctrl+C to stop the producer, then:
make stream-stop
```

After a few seconds, check the Kafka UI at http://localhost:8080 to see the
`prices.realtime` topic with messages flowing. Data lands in MinIO under
`cryptolake-bronze/realtime_trades/`.

## Troubleshooting

**Docker: "Not enough memory"**
Docker Desktop -> Settings -> Resources -> Allocate at least 8 GB RAM.

**dbt: "Permission denied" on logs/ or target/**
`sudo chown -R $USER:$USER src/transformation/dbt_cryptolake/logs/ src/transformation/dbt_cryptolake/target/`

**Spark Thrift: "Connection refused"**
Verify the container is running: `docker logs cryptolake-spark-thrift 2>&1 | tail -10`

**Ruff: E402 errors in Spark scripts**
Configured in `ruff.toml` as ignored (module-level docstrings before imports).
