"""
MarketPulse - dbt Transformation DAG
Builds the CURATED layer from STAGED using dbt (dbt run), then
validates it (dbt test). Replaces the manual stored-procedure calls
from sql/04_curated_tables.sql in the orchestrated flow.

Schedule: Daily at 07:30 UTC - after both ingestion DAGs
(06:00 prices, 06:30 sentiment) and before the quality checks DAG
(08:00), so quality checks always see a freshly built CURATED layer.

Credentials: dbt/profiles.yml reads SNOWFLAKE_* environment variables;
they are injected into the Airflow containers via docker-compose
(.env substitution) or the Kubernetes Secret. Nothing is hardcoded.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

from utils.dbt_commands import build_dbt_command

DBT_PROJECT_DIR = "/opt/airflow/dbt"

default_args = {
    "owner": "marketpulse",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="marketpulse_transform_dbt",
    default_args=default_args,
    description="Build and test the CURATED layer with dbt",
    schedule_interval="30 7 * * *",  # Daily at 07:30 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["marketpulse", "transform", "dbt"],
) as dag:

    # BashOperator on purpose: the command is exactly what you would
    # run locally, which makes failures easy to reproduce and debug.
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=build_dbt_command("run", project_dir=DBT_PROJECT_DIR),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=build_dbt_command("test", project_dir=DBT_PROJECT_DIR),
    )

    dbt_run >> dbt_test
