"""
Lambda architecture — batch layer DAG.

Flow:
  1. Spark job (batch_daily_summary.py) runs at 1 AM via docker-compose:
       docker compose --profile batch run --rm spark-batch-daily
     In production this step uses KubernetesOperator / SparkSubmitOperator.

  2. This DAG runs at 2 AM (after Spark finishes), performs two tasks:
       check_spark_output  — verify Parquet landed in MinIO
       load_to_clickhouse  — INSERT via ClickHouse s3() table function

Why a separate batch layer?
  - Streaming dedup is watermark-bounded: late events beyond 10 min are dropped.
  - Batch reads the full day partition and deduplicates exactly on order_id.
  - Batch can also reprocess historical dates after bug fixes or schema changes.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task

logger = logging.getLogger(__name__)

_MINIO_ENDPOINT  = "http://minio:9000"
_MINIO_ACCESS    = "minioadmin"
_MINIO_SECRET    = "minioadmin"
_BUCKET          = "food-delivery-lake"
_CH_HOST         = "clickhouse"
_CH_PORT         = 9000   # native TCP inside Docker network


@dag(
    dag_id="batch_daily_summary",
    description="Batch layer: MinIO cold path → batch_daily_city_stats in ClickHouse",
    schedule_interval="0 2 * * *",   # 2 AM daily — Spark job runs at 1 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "email_on_failure": True,
        "email_on_retry": False,
        "email": ["caophon9ats2018@gmail.com"],
    },
    tags=["batch", "lambda", "spark", "clickhouse"],
)
def batch_daily_summary():

    @task
    def check_spark_output(**context) -> None:
        """
        Verify Spark wrote Parquet output for yesterday before loading.
        Fails fast so the load step never runs against missing data.
        """
        import boto3

        ds = context["ds"]   # YYYY-MM-DD, Airflow execution date (= yesterday)
        prefix = f"batch/daily_summary/date={ds}/"

        s3 = boto3.client(
            "s3",
            endpoint_url=_MINIO_ENDPOINT,
            aws_access_key_id=_MINIO_ACCESS,
            aws_secret_access_key=_MINIO_SECRET,
        )
        resp = s3.list_objects_v2(Bucket=_BUCKET, Prefix=prefix, MaxKeys=1)
        if resp.get("KeyCount", 0) == 0:
            raise FileNotFoundError(
                f"No Spark output at s3://{_BUCKET}/{prefix}\n"
                "Trigger manually: docker compose --profile batch run --rm spark-batch-daily"
            )
        logger.info("Spark output verified: s3://%s/%s", _BUCKET, prefix)

    @task
    def load_to_clickhouse(**context) -> None:
        """
        INSERT INTO batch_daily_city_stats using ClickHouse's built-in s3() function.
        ClickHouse pulls the Parquet directly from MinIO — no file transfer through Airflow.

        Idempotency: DELETE WHERE batch_date = ds before INSERT so re-runs are safe.
        ReplacingMergeTree processed_at version also guards against duplicates at query time.
        """
        from clickhouse_driver import Client

        ds = context["ds"]
        client = Client(host=_CH_HOST, port=_CH_PORT)

        # Lightweight delete (ClickHouse 23.3+) clears the day before re-inserting
        client.execute(
            "DELETE FROM food_delivery.batch_daily_city_stats WHERE batch_date = %(d)s",
            {"d": ds},
        )
        logger.info("Cleared existing rows for %s", ds)

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

    check_spark_output() >> load_to_clickhouse()


dag = batch_daily_summary()
