"""
Lambda architecture — batch layer DAG.

Flow:
  1. SparkSubmitOperator submits batch_daily_summary.py to the Spark cluster.
  2. check_spark_output verifies Parquet landed in MinIO.
  3. load_to_clickhouse INSERTs via ClickHouse s3() table function.

Why a separate batch layer?
  - Streaming dedup is watermark-bounded: late events beyond 10 min are dropped.
  - Batch reads the full day partition and deduplicates exactly on order_id.
  - Batch can also reprocess historical dates after bug fixes or schema changes.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

logger = logging.getLogger(__name__)

_MINIO_ENDPOINT  = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
_MINIO_ACCESS    = os.getenv("MINIO_ROOT_USER", "minioadmin")
_MINIO_SECRET    = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
_BUCKET          = "food-delivery-lake"
_CH_HOST         = os.getenv("CLICKHOUSE_HOST", "clickhouse")
_CH_PORT         = int(os.getenv("CLICKHOUSE_NATIVE_PORT", "9000"))
_ALERT_EMAIL     = os.getenv("AIRFLOW_ALERT_EMAIL", "")


@dag(
    dag_id="batch_daily_summary",
    description="Batch layer: Spark → MinIO → batch_daily_city_stats in ClickHouse",
    schedule_interval="0 1 * * *",   # 1 AM daily — Spark runs first, then check + load
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "email_on_failure": bool(_ALERT_EMAIL),
        "email_on_retry": False,
        "email": [_ALERT_EMAIL] if _ALERT_EMAIL else [],
    },
    tags=["batch", "lambda", "spark", "clickhouse"],
)
def batch_daily_summary():

    run_spark_batch = SparkSubmitOperator(
        task_id="run_spark_batch",
        application="/opt/spark/jobs/batch_daily_summary.py",
        conn_id="spark_default",
        application_args=["--date", "{{ ds }}"],
        conf={
            "spark.hadoop.fs.s3a.endpoint":            _MINIO_ENDPOINT,
            "spark.hadoop.fs.s3a.access.key":          _MINIO_ACCESS,
            "spark.hadoop.fs.s3a.secret.key":          _MINIO_SECRET,
            "spark.hadoop.fs.s3a.path.style.access":   "true",
            "spark.hadoop.fs.s3a.impl":                "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        },
        total_executor_cores=2,
        executor_memory="1G",
    )

    @task
    def check_spark_output(**context) -> None:
        """Verify Spark wrote Parquet output before loading.

        On missing output: raises FileNotFoundError → task fails → Airflow retries
        once after 5 min (default_args retries=1) → if still missing, DAG fails
        and sends email alert (email_on_failure=True).
        """
        import boto3

        ds = context["ds"]
        prefix = f"batch/daily_summary/date={ds}/"

        s3 = boto3.client(
            "s3",
            endpoint_url=_MINIO_ENDPOINT,
            aws_access_key_id=_MINIO_ACCESS,
            aws_secret_access_key=_MINIO_SECRET,
        )
        resp = s3.list_objects_v2(Bucket=_BUCKET, Prefix=prefix, MaxKeys=1)
        if resp.get("KeyCount", 0) == 0:
            raise FileNotFoundError(f"No Spark output at s3://{_BUCKET}/{prefix}")
        logger.info("Spark output verified: s3://%s/%s", _BUCKET, prefix)

    @task
    def load_to_clickhouse(**context) -> None:
        """INSERT INTO batch_daily_city_stats using ClickHouse s3() function.

        Atomicity note: DELETE and INSERT are two separate ClickHouse statements —
        not wrapped in a transaction. If INSERT fails mid-flight, the day's rows are
        absent until the next retry. Retry is idempotent: DELETE is a no-op on already-
        deleted rows, then INSERT re-loads the full partition.

        ClickHouse DELETE FROM on MergeTree is an async lightweight mutation (parts are
        rebuilt in the background), so a brief overlap between old and new rows is
        possible during normal execution. Eventual consistency is guaranteed once the
        mutation and the INSERT both complete.

        Partial load risk: if the s3() read fails after ClickHouse has already committed
        some INSERT blocks, those blocks remain. The retry's DELETE will clean them up
        before re-inserting. No manual intervention needed for single-day failures.
        """
        from clickhouse_driver import Client

        ds = context["ds"]
        client = Client(host=_CH_HOST, port=_CH_PORT)

        client.execute(
            "DELETE FROM food_delivery.batch_daily_city_stats WHERE batch_date = %(d)s",
            {"d": ds},
        )
        minio_glob = f"{_MINIO_ENDPOINT}/{_BUCKET}/batch/daily_summary/date={ds}/*.parquet"
        client.execute(f"""
            INSERT INTO food_delivery.batch_daily_city_stats
            SELECT
                toDate(batch_date)              AS batch_date,
                city,
                payment_method,
                platform,
                toUInt32(total_orders)          AS total_orders,
                toUInt32(delivered_orders)      AS delivered_orders,
                toUInt32(cancelled_orders)      AS cancelled_orders,
                toUInt64(gross_revenue_vnd)     AS gross_revenue_vnd,
                avg_order_vnd,
                toUInt32(paid_orders)           AS paid_orders,
                avg_payment_delay_seconds,
                now()                           AS processed_at
            FROM s3(
                '{minio_glob}',
                '{_MINIO_ACCESS}',
                '{_MINIO_SECRET}',
                'Parquet'
            )
        """)
        logger.info("Loaded batch_daily_city_stats for %s", ds)

    run_spark_batch >> check_spark_output() >> load_to_clickhouse()


dag = batch_daily_summary()
