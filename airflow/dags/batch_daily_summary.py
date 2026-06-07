"""
Lambda architecture — batch layer DAG.

Pending Phase 4 (BigQuery) migration.
Original flow (local): Spark → MinIO → ClickHouse
Target flow (GCP):     Spark → GCS  → BigQuery

DAG is paused at creation. Enable after Phase 4 BigQuery load is implemented.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task

logger = logging.getLogger(__name__)


@dag(
    dag_id="batch_daily_summary",
    description="Batch layer: Spark → GCS → BigQuery (pending Phase 4)",
    schedule_interval="0 1 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 0,
        "email_on_failure": False,
    },
    tags=["batch", "lambda", "bigquery"],
)
def batch_daily_summary():

    @task
    def pending_bigquery_migration(**context) -> None:
        logger.warning(
            "batch_daily_summary is pending Phase 4 (BigQuery) implementation. "
            "Unpause this DAG after BigQuery load tasks are written. ds=%s",
            context["ds"],
        )

    pending_bigquery_migration()


dag = batch_daily_summary()
