"""
CryptoLake Master DAG — Full ELT Pipeline.

Steps:
1. Init Iceberg namespaces
2. Batch ingestion (CoinGecko + Fear & Greed)
3. Bronze load (APIs -> Iceberg)
4. Silver processing (Bronze -> Silver with Spark)
5. Gold transformation (Silver -> Gold with dbt)
6. Data quality checks (custom validators)

Schedule: Daily at 06:00 UTC
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

default_args = {
    "owner": "cryptolake",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}

# dbt environment isolation: PYTHONNOUSERSITE=1 prevents loading user packages.
# PYTHONPATH points exclusively to the dbt virtualenv, avoiding protobuf
# version conflicts between dbt-core (>=6.0) and Airflow (<5.0).
DBT_ENV = (
    "export PYTHONNOUSERSITE=1 && export PYTHONPATH=/opt/dbt-venv/lib/python3.11/site-packages && "
)
DBT_CMD = DBT_ENV + "cd /opt/airflow/src/transformation/dbt_cryptolake && /opt/dbt-venv/bin/dbt"

with DAG(
    dag_id="cryptolake_full_pipeline",
    default_args=default_args,
    description="Pipeline: Ingest -> Bronze -> Silver -> Gold -> Quality",
    schedule="0 6 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["cryptolake", "production"],
    doc_md=__doc__,
) as dag:
    # -- Init ------------------------------------------------
    init_namespaces = BashOperator(
        task_id="init_namespaces",
        bash_command=(
            "docker exec cryptolake-spark-master "
            "/opt/spark/bin/spark-submit --master 'local[1]' "
            "/opt/spark/work/src/processing/batch/init_namespaces.py"
        ),
    )

    # -- Ingestion -------------------------------------------
    with TaskGroup("ingestion", tooltip="Extract data from external APIs") as ingestion_group:
        extract_coingecko = BashOperator(
            task_id="extract_coingecko",
            bash_command=("cd /opt/airflow && python -m src.ingestion.batch.coingecko_extractor"),
        )
        extract_fear_greed = BashOperator(
            task_id="extract_fear_greed",
            bash_command=("cd /opt/airflow && python -m src.ingestion.batch.fear_greed_extractor"),
        )

    # -- Bronze ----------------------------------------------
    with TaskGroup("bronze_load", tooltip="Load raw data into Iceberg Bronze") as bronze_group:
        api_to_bronze = BashOperator(
            task_id="api_to_bronze",
            bash_command=(
                "docker exec cryptolake-spark-master "
                "/opt/spark/bin/spark-submit "
                "/opt/spark/work/src/processing/batch/api_to_bronze.py"
            ),
        )

    # -- Silver ----------------------------------------------
    with TaskGroup("silver_processing", tooltip="Clean and deduplicate in Silver") as silver_group:
        bronze_to_silver = BashOperator(
            task_id="bronze_to_silver",
            bash_command=(
                "docker exec cryptolake-spark-master "
                "/opt/spark/bin/spark-submit "
                "/opt/spark/work/src/processing/batch/bronze_to_silver.py"
            ),
        )

    # -- Gold (dbt) ------------------------------------------
    with TaskGroup("gold_transformation", tooltip="Dimensional modeling with dbt") as gold_group:
        dbt_run = BashOperator(
            task_id="dbt_run",
            bash_command=f"{DBT_CMD} run --profiles-dir . --target prod",
        )
        dbt_test = BashOperator(
            task_id="dbt_test",
            bash_command=f"{DBT_CMD} test --profiles-dir . --target prod",
        )
        dbt_run >> dbt_test

    # -- Data Quality ----------------------------------------
    with TaskGroup("data_quality", tooltip="Data quality validation") as quality_group:
        quality_checks = BashOperator(
            task_id="quality_checks",
            bash_command=(
                "docker exec cryptolake-spark-master "
                "/opt/spark/bin/spark-submit "
                "/opt/spark/work/src/quality/run_quality_checks.py"
            ),
        )

    # -- Dependencies ----------------------------------------
    (
        init_namespaces
        >> ingestion_group
        >> bronze_group
        >> silver_group
        >> gold_group
        >> quality_group
    )
