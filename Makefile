# ============================================================
# CryptoLake — Makefile
# ============================================================
# Developer commands for the CryptoLake data platform.
# Run `make help` to see all available targets.
# ============================================================

.PHONY: help up down down-clean rebuild logs status pipeline

# -- General -------------------------------------------------

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
	awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

up: ## Start all services (12+ containers)
	@echo "Starting CryptoLake..."
	docker compose up -d --build
	@echo ""
	@echo "Waiting for services to be ready (~60s first time)..."
	@sleep 30
	@echo ""
	@echo "CryptoLake is running."
	@echo ""
	@echo "Services available:"
	@echo "   MinIO Console:   http://localhost:9001  (cryptolake / cryptolake123)"
	@echo "   Kafka UI:        http://localhost:8080"
	@echo "   Spark UI:        http://localhost:8082"
	@echo "   Airflow:         http://localhost:8083  (admin / admin)"
	@echo "   API Docs:        http://localhost:8000/docs"
	@echo "   Dashboard:       http://localhost:8501"

down: ## Stop all services (preserves data)
	docker compose down

down-clean: ## Stop and DELETE all data (volumes)
	docker compose down -v
	@echo "All volumes removed."

rebuild: ## Rebuild images and restart (use after Dockerfile changes)
	docker compose down
	docker compose up -d --build
	@echo "Waiting..."
	@sleep 30
	@echo "Rebuilt and started."

status: ## Show service status
	@docker compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}"

logs: ## Tail logs for all services
	docker compose logs -f

logs-kafka: ## Tail Kafka logs
	docker compose logs -f kafka

logs-spark: ## Tail Spark logs (master + worker)
	docker compose logs -f spark-master spark-worker

logs-airflow: ## Tail Airflow logs
	docker compose logs -f airflow-webserver airflow-scheduler

# -- Pipeline ------------------------------------------------

pipeline: ## Run full ELT pipeline: Bronze -> Silver -> Gold -> Quality
	@echo "Running full pipeline..."
	$(MAKE) init-namespaces
	$(MAKE) bronze-load
	$(MAKE) silver-transform
	$(MAKE) dbt-run
	$(MAKE) dbt-test
	$(MAKE) quality-check
	@echo "Pipeline completed."
	@echo "   API Docs:  http://localhost:8000/docs"
	@echo "   Dashboard: http://localhost:8501"

init-namespaces: ## Create Iceberg namespaces (required after down-clean)
	@echo "Creating Iceberg namespaces..."
	docker exec cryptolake-spark-master \
	    /opt/spark/bin/spark-submit --master 'local[1]' \
	    /opt/spark/work/src/processing/batch/init_namespaces.py
	@echo "Namespaces created."

bronze-load: ## Load data from APIs into Bronze layer
	docker exec cryptolake-spark-master \
	    /opt/spark/bin/spark-submit \
	    /opt/spark/work/src/processing/batch/api_to_bronze.py

silver-transform: ## Transform Bronze -> Silver (clean + deduplicate)
	docker exec cryptolake-spark-master \
	    /opt/spark/bin/spark-submit \
	    /opt/spark/work/src/processing/batch/bronze_to_silver.py

gold-transform: ## Transform Silver -> Gold (PySpark, without dbt)
	docker exec cryptolake-spark-master \
	    /opt/spark/bin/spark-submit \
	    /opt/spark/work/src/processing/batch/silver_to_gold.py

# -- dbt (via Airflow container) -----------------------------

dbt-run: ## Run dbt models (staging -> gold star schema)
	docker exec cryptolake-airflow-scheduler \
	    bash -c "cd /opt/airflow/src/transformation/dbt_cryptolake && \
	    /opt/dbt-venv/bin/dbt run --profiles-dir . --target prod"

dbt-test: ## Run dbt tests
	docker exec cryptolake-airflow-scheduler \
	    bash -c "cd /opt/airflow/src/transformation/dbt_cryptolake && \
	    /opt/dbt-venv/bin/dbt test --profiles-dir . --target prod"

dbt-all: ## Run dbt run + test
	$(MAKE) dbt-run
	$(MAKE) dbt-test

# -- Data Quality --------------------------------------------

quality-check: ## Run quality checks (all layers)
	docker exec cryptolake-spark-master \
	    /opt/spark/bin/spark-submit \
	    /opt/spark/work/src/quality/run_quality_checks.py

quality-bronze: ## Quality checks: Bronze only
	docker exec cryptolake-spark-master \
	    /opt/spark/bin/spark-submit \
	    /opt/spark/work/src/quality/run_quality_checks.py --layer bronze

quality-silver: ## Quality checks: Silver only
	docker exec cryptolake-spark-master \
	    /opt/spark/bin/spark-submit \
	    /opt/spark/work/src/quality/run_quality_checks.py --layer silver

quality-gold: ## Quality checks: Gold only
	docker exec cryptolake-spark-master \
	    /opt/spark/bin/spark-submit \
	    /opt/spark/work/src/quality/run_quality_checks.py --layer gold

# -- Serving -------------------------------------------------

api-logs: ## Tail API logs
	docker logs -f cryptolake-api

dashboard-logs: ## Tail Dashboard logs
	docker logs -f cryptolake-dashboard

# -- Kafka ---------------------------------------------------

spark-shell: ## Open interactive PySpark shell with Iceberg
	docker exec -it cryptolake-spark-master \
	    /opt/spark/bin/pyspark

kafka-topics: ## List Kafka topics
	docker exec cryptolake-kafka \
	    kafka-topics --bootstrap-server localhost:29092 --list

kafka-create-topics: ## Create required Kafka topics
	docker exec cryptolake-kafka \
	    kafka-topics --bootstrap-server localhost:29092 \
	    --create --topic prices.realtime \
	    --partitions 3 --replication-factor 1 \
	    --config retention.ms=86400000
	@echo "Topic 'prices.realtime' created (retention: 24h, 3 partitions)."

kafka-describe: ## Describe prices topic
	docker exec cryptolake-kafka \
	    kafka-topics --bootstrap-server localhost:29092 \
	    --describe --topic prices.realtime

# -- Airflow -------------------------------------------------

airflow-trigger: ## Trigger full pipeline DAG manually
	docker exec cryptolake-airflow-scheduler \
	    airflow dags trigger cryptolake_full_pipeline

airflow-status: ## Show last 5 DAG runs
	docker exec cryptolake-airflow-scheduler \
	    airflow dags list-runs -d cryptolake_full_pipeline --limit 5
