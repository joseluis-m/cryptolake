.PHONY: help up down down-clean logs status spark-shell kafka-topics

help: ## Mostrar esta ayuda
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
	awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

up: ## Arrancar todos los servicios
	@echo "🚀 Arrancando CryptoLake..."
	docker compose up -d --build
	@echo ""
	@echo "⏳ Esperando a que los servicios estén listos (~60s la primera vez)..."
	@sleep 30
	@echo ""
	@echo "✅ CryptoLake está corriendo!"
	@echo ""
	@echo "📊 Servicios disponibles:"
	@echo "   MinIO Console:   http://localhost:9001  (user: cryptolake / pass: cryptolake123)"
	@echo "   Kafka UI:        http://localhost:8080"
	@echo "   Spark UI:        http://localhost:8082"
	@echo "   Airflow:         http://localhost:8083  (user: admin / pass: admin)"
	@echo "   Iceberg Catalog: http://localhost:8181"

down: ## Parar todos los servicios (conserva datos)
	docker compose down

down-clean: ## Parar y BORRAR todos los datos
	docker compose down -v
	@echo "🗑️  Todos los volumes eliminados"

rebuild: ## Reconstruir imágenes y arrancar (usar tras cambios en Dockerfiles)
	docker compose down
	docker compose up -d --build
	@echo "⏳ Esperando..."
	@sleep 30
	@echo "✅ Reconstruido y arrancado"

logs: ## Ver logs de todos los servicios
	docker compose logs -f

logs-kafka: ## Ver logs solo de Kafka
	docker compose logs -f kafka

logs-spark: ## Ver logs de Spark (master + worker)
	docker compose logs -f spark-master spark-worker

logs-airflow: ## Ver logs de Airflow
	docker compose logs -f airflow-webserver airflow-scheduler

status: ## Ver estado de los servicios
	@docker compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}"

spark-shell: ## Abrir consola PySpark interactiva con Iceberg configurado
	docker exec -it cryptolake-spark-master \
	    /opt/spark/bin/pyspark

kafka-topics: ## Listar topics de Kafka
	docker exec cryptolake-kafka \
	    kafka-topics --bootstrap-server localhost:29092 --list

kafka-create-topics: ## Crear los topics necesarios
	docker exec cryptolake-kafka \
	    kafka-topics --bootstrap-server localhost:29092 \
	    --create --topic prices.realtime \
	    --partitions 3 --replication-factor 1 \
	    --config retention.ms=86400000
	@echo "✅ Topic 'prices.realtime' creado (retención: 24h, 3 particiones)"

kafka-describe: ## Describir el topic de precios
	docker exec cryptolake-kafka \
	    kafka-topics --bootstrap-server localhost:29092 \
	    --describe --topic prices.realtime

bronze-load: ## Cargar datos de APIs a Bronze
	docker exec cryptolake-spark-master \
	    /opt/spark/bin/spark-submit \
	    /opt/spark/work/src/processing/batch/api_to_bronze.py

silver-transform: ## Transformar Bronze → Silver
	docker exec cryptolake-spark-master \
	    /opt/spark/bin/spark-submit \
	    /opt/spark/work/src/processing/batch/bronze_to_silver.py

gold-transform: ## Transformar Silver → Gold (sin dbt)
	docker exec cryptolake-spark-master \
	    /opt/spark/bin/spark-submit \
	    /opt/spark/work/src/processing/batch/silver_to_gold.py

init-namespaces: ## Crear namespaces Iceberg (necesario tras down-clean)
	@echo "📦 Creando namespaces Iceberg..."
	docker exec cryptolake-spark-master \
	    /opt/spark/bin/spark-submit --master 'local[1]' \
	    /opt/spark/work/src/processing/batch/init_namespaces.py
	@echo "✅ Namespaces creados"

pipeline: ## Ejecutar pipeline completo: Bronze → Silver → Gold
	@echo "🚀 Ejecutando pipeline completo..."
	$(MAKE) init-namespaces
	$(MAKE) bronze-load
	$(MAKE) silver-transform
#	$(MAKE) gold-transform
	$(MAKE) dbt-run
	$(MAKE) dbt-test
	@echo "✅ Pipeline completado!"

# ── dbt (via contenedor Airflow, consistente con el pipeline) ──
dbt-run: ## Ejecutar modelos dbt (staging → gold)
	docker exec cryptolake-airflow-scheduler \
	    bash -c "cd /opt/airflow/src/transformation/dbt_cryptolake && /opt/dbt-venv/bin/dbt run --profiles-dir . --target prod"

dbt-test: ## Ejecutar tests dbt
	docker exec cryptolake-airflow-scheduler \
	    bash -c "cd /opt/airflow/src/transformation/dbt_cryptolake && /opt/dbt-venv/bin/dbt test --profiles-dir . --target prod"

dbt-all: ## Ejecutar dbt run + test
	$(MAKE) dbt-run
	$(MAKE) dbt-test

# ── dbt local (usa el venv de tu Mac) ──────────────────────
dbt-run-local: ## Ejecutar dbt run en local
	cd src/transformation/dbt_cryptolake && .venv/bin/dbt run --profiles-dir .

dbt-test-local: ## Ejecutar dbt test en local
	cd src/transformation/dbt_cryptolake && .venv/bin/dbt test --profiles-dir .

# ── Airflow ─────────────────────────────────────────────────
airflow-trigger: ## Trigger manual del DAG completo en Airflow
	docker exec cryptolake-airflow-scheduler \
	    airflow dags trigger cryptolake_full_pipeline

airflow-status: ## Ver estado de la última ejecución del DAG
	docker exec cryptolake-airflow-scheduler \
	    airflow dags list-runs -d cryptolake_full_pipeline --limit 5