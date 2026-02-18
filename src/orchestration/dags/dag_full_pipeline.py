"""
DAG Master de CryptoLake.

Ejecuta el pipeline completo de datos diariamente:
1. Ingesta batch (CoinGecko + Fear & Greed via APIs)
2. Bronze load (APIs → Iceberg Bronze con Spark)
3. Silver processing (Bronze → Silver con Spark)
4. Gold transformation (Silver → Gold con dbt)
5. Data quality checks (dbt tests)

Schedule: Diario a las 06:00 UTC
Retry: 2 reintentos con 5 minutos entre cada uno
Timeout: 1 hora máximo por task

Ejecución manual: También se puede trigger desde la UI de Airflow.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup


# ================================================================
# Configuración por defecto para todas las tareas del DAG.
# Se puede sobreescribir en tareas individuales.
# ================================================================
default_args = {
    # Nombre del dueño (aparece en la UI de Airflow)
    "owner": "cryptolake",

    # depends_on_past=False: cada ejecución es independiente.
    # Si ayer falló, hoy se ejecuta igualmente.
    "depends_on_past": False,

    # No enviar emails al fallar (requeriría configurar SMTP)
    "email_on_failure": False,

    # Si una tarea falla, reintenta 2 veces
    "retries": 2,

    # Espera 5 minutos entre reintentos
    "retry_delay": timedelta(minutes=5),

    # Si una tarea tarda más de 1 hora, se cancela
    "execution_timeout": timedelta(hours=1),
}


# ================================================================
# Definición del DAG
# ================================================================
# "with DAG(...) as dag:" es un context manager de Python.
# Todo lo que definamos dentro pertenece a este DAG.
# ================================================================
with DAG(
    # ID único del DAG (aparece en la UI de Airflow)
    dag_id="cryptolake_full_pipeline",

    default_args=default_args,

    description="Pipeline completo: Ingesta → Bronze → Silver → Gold → Quality",

    # Schedule en formato cron: "minuto hora día mes día_semana"
    # "0 6 * * *" = a las 06:00, todos los días, todos los meses
    schedule="0 6 * * *",

    # Fecha desde la que Airflow consideraría ejecutar este DAG.
    # Con catchup=False, NO ejecuta las fechas pasadas.
    start_date=datetime(2025, 1, 1),

    # catchup=False: No ejecutar retroactivamente para fechas pasadas.
    # Si activamos el DAG hoy, solo se ejecuta hoy, no intenta
    # ejecutar todos los días desde start_date.
    catchup=False,

    # Tags para filtrar en la UI de Airflow
    tags=["cryptolake", "production"],

    # doc_md: la docstring de este archivo aparece como documentación
    # del DAG en la UI de Airflow
    doc_md=__doc__,

) as dag:

    # ════════════════════════════════════════════════════════════
    # GRUPO 1: INGESTA BATCH
    # ════════════════════════════════════════════════════════════
    # Descarga datos de las APIs externas.
    # CoinGecko y Fear & Greed se ejecutan en PARALELO (no hay
    # dependencia entre ellas — una no necesita a la otra).
    # ════════════════════════════════════════════════════════════
    with TaskGroup("ingestion", tooltip="Descarga datos de APIs externas") as ingestion_group:

        extract_coingecko = BashOperator(
            task_id="extract_coingecko",
            # Ejecutamos el extractor Python directamente en el contenedor de Airflow.
            # El módulo está montado en /opt/airflow/src/ via docker-compose volumes.
            bash_command=(
                "cd /opt/airflow && "
                "python -m src.ingestion.batch.coingecko_extractor"
            ),
        )

        extract_fear_greed = BashOperator(
            task_id="extract_fear_greed",
            bash_command=(
                "cd /opt/airflow && "
                "python -m src.ingestion.batch.fear_greed_extractor"
            ),
        )

        # No hay ">>" entre ellas = se ejecutan en paralelo

    # ════════════════════════════════════════════════════════════
    # GRUPO 2: BRONZE LOAD (APIs → Iceberg Bronze)
    # ════════════════════════════════════════════════════════════
    # Ejecuta spark-submit en el contenedor de Spark usando
    # "docker exec". Este patrón se llama "sibling containers":
    # Airflow usa el Docker socket para ejecutar comandos en
    # contenedores hermanos que comparten la misma red Docker.
    #
    # En producción se usaría KubernetesPodOperator, EMROperator,
    # o Livy, pero para desarrollo local esto es lo más simple.
    # ════════════════════════════════════════════════════════════
    with TaskGroup("bronze_load", tooltip="Cargar datos en Iceberg Bronze") as bronze_group:

        api_to_bronze = BashOperator(
            task_id="api_to_bronze",
            bash_command=(
                "docker exec cryptolake-spark-master "
                "/opt/spark/bin/spark-submit "
                "/opt/spark/work/src/processing/batch/api_to_bronze.py"
            ),
        )

    # ════════════════════════════════════════════════════════════
    # GRUPO 3: SILVER PROCESSING (Bronze → Silver)
    # ════════════════════════════════════════════════════════════
    # Deduplicación, limpieza y MERGE INTO. Todo con Spark.
    # ════════════════════════════════════════════════════════════
    with TaskGroup("silver_processing", tooltip="Limpiar y deduplicar en Silver") as silver_group:

        bronze_to_silver = BashOperator(
            task_id="bronze_to_silver",
            bash_command=(
                "docker exec cryptolake-spark-master "
                "/opt/spark/bin/spark-submit "
                "/opt/spark/work/src/processing/batch/bronze_to_silver.py"
            ),
        )

    # ════════════════════════════════════════════════════════════
    # GRUPO 4: GOLD TRANSFORMATION (dbt)
    # ════════════════════════════════════════════════════════════
    # dbt se ejecuta directamente en el contenedor de Airflow
    # (dbt-spark está instalado en el Dockerfile de Airflow).
    # Conecta al Spark Thrift Server via JDBC.
    #
    # Usamos --target prod para que dbt use la configuración
    # de producción (host: spark-thrift en vez de localhost).
    # ════════════════════════════════════════════════════════════
    with TaskGroup("gold_transformation", tooltip="Modelado dimensional con dbt") as gold_group:
        
        dbt_run = BashOperator(
            task_id="dbt_run",
            bash_command=(
                "export PYTHONNOUSERSITE=1 && "
                "export PYTHONPATH=/opt/dbt-venv/lib/python3.11/site-packages && "
                "cd /opt/airflow/src/transformation/dbt_cryptolake && "
                "/opt/dbt-venv/bin/dbt run --profiles-dir . --target prod"
            ),
        )

        dbt_test = BashOperator(
            task_id="dbt_test",
            bash_command=(
                "export PYTHONNOUSERSITE=1 && "
                "export PYTHONPATH=/opt/dbt-venv/lib/python3.11/site-packages && "
                "cd /opt/airflow/src/transformation/dbt_cryptolake && "
                "/opt/dbt-venv/bin/dbt test --profiles-dir . --target prod"
            ),
        )

        # dbt_test se ejecuta DESPUÉS de dbt_run
        dbt_run >> dbt_test

    # ════════════════════════════════════════════════════════════
    # GRUPO 5: DATA QUALITY
    # ════════════════════════════════════════════════════════════
    # Placeholder para Great Expectations (Fase 7).
    # Por ahora, los tests de dbt son nuestra validación de calidad.
    # ════════════════════════════════════════════════════════════
    with TaskGroup("data_quality", tooltip="Validación de calidad de datos") as quality_group:

        quality_check = BashOperator(
            task_id="quality_summary",
            bash_command='echo "✅ Data quality checks passed (dbt tests ran in gold_transformation group)"',
        )

    # ════════════════════════════════════════════════════════════
    # DEPENDENCIAS ENTRE GRUPOS
    # ════════════════════════════════════════════════════════════
    # El operador ">>" define el orden de ejecución:
    # ingestion → bronze → silver → gold → quality
    #
    # Esto se visualiza en la UI de Airflow como un grafo
    # de izquierda a derecha con flechas entre los grupos.
    # ════════════════════════════════════════════════════════════
    ingestion_group >> bronze_group >> silver_group >> gold_group >> quality_group
