from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_DIR = "/opt/airflow/dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt"

_DBT_CMD = f"dbt --no-write-json run --project-dir {DBT_DIR} --profiles-dir {DBT_PROFILES_DIR}"
_DBT_TEST = f"dbt --no-write-json test --project-dir {DBT_DIR} --profiles-dir {DBT_PROFILES_DIR}"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="dbt_run",
    description="Run dbt models + tests after ClickHouse is loaded",
    schedule_interval="5 * * * *",   # HH:05 — after Kafka Engine real-time load settles
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["dbt", "transformation"],
) as dag:

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=_DBT_CMD,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=_DBT_TEST,
    )

    dbt_run >> dbt_test
