"""
Batch layer DAG — loads GCS JSONL files (written by pubsub-subscriber) into BigQuery raw tables.

Schedule: daily at 00:30 UTC (07:30 Vietnam).  Airflow's logical date == the data date
(i.e. the run at 2024-01-16T00:30 loads year=2024/month=01/day=15).

GCS source layout (written by pubsub_subscriber/subscriber.py):
  gs://{bucket}/pubsub-raw/{topic}/year=YYYY/month=MM/day=DD/batch_*.jsonl

BigQuery destination:
  {project}.food_delivery_raw.{table}$YYYYMMDD   (WRITE_TRUNCATE per partition — idempotent)
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task

logger = logging.getLogger(__name__)

GCP_PROJECT = os.getenv("GCP_PROJECT_ID", "project-739a3554-aa69-4eab-9e2")
GCS_BUCKET  = os.getenv("GCS_BUCKET",     "vn-food-delivery-lake-739a3554")
BQ_DATASET  = "food_delivery_raw"

# (topic_name, bq_table_name, schema_fields)
_LOADS: list[tuple[str, str, list[dict]]] = [
    (
        "raw-orders",
        "raw_orders",
        [
            {"name": "order_id",         "field_type": "STRING",    "mode": "REQUIRED"},
            {"name": "customer_id",      "field_type": "STRING",    "mode": "REQUIRED"},
            {"name": "restaurant_id",    "field_type": "STRING",    "mode": "REQUIRED"},
            {"name": "rider_id",         "field_type": "STRING",    "mode": "REQUIRED"},
            {"name": "city",             "field_type": "STRING",    "mode": "REQUIRED"},
            {"name": "district",         "field_type": "STRING",    "mode": "REQUIRED"},
            {"name": "status",           "field_type": "STRING",    "mode": "REQUIRED"},
            {"name": "items",            "field_type": "STRING",    "mode": "REQUIRED"},
            {"name": "subtotal_vnd",     "field_type": "INTEGER",   "mode": "REQUIRED"},
            {"name": "delivery_fee_vnd", "field_type": "INTEGER",   "mode": "REQUIRED"},
            {"name": "discount_vnd",     "field_type": "INTEGER",   "mode": "REQUIRED"},
            {"name": "total_vnd",        "field_type": "INTEGER",   "mode": "REQUIRED"},
            {"name": "payment_method",   "field_type": "STRING",    "mode": "REQUIRED"},
            {"name": "platform",         "field_type": "STRING",    "mode": "REQUIRED"},
            {"name": "placed_at",        "field_type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "event_timestamp",  "field_type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "producer_ts",      "field_type": "FLOAT",     "mode": "NULLABLE"},
        ],
    ),
    (
        "raw-payments",
        "raw_payments",
        [
            {"name": "payment_id",             "field_type": "STRING",    "mode": "REQUIRED"},
            {"name": "order_id",               "field_type": "STRING",    "mode": "REQUIRED"},
            {"name": "amount_vnd",             "field_type": "INTEGER",   "mode": "REQUIRED"},
            {"name": "method",                 "field_type": "STRING",    "mode": "REQUIRED"},
            {"name": "status",                 "field_type": "STRING",    "mode": "REQUIRED"},
            {"name": "gateway_transaction_id", "field_type": "STRING",    "mode": "NULLABLE"},
            {"name": "processed_at",           "field_type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "event_timestamp",        "field_type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "producer_ts",            "field_type": "FLOAT",     "mode": "NULLABLE"},
        ],
    ),
    (
        "raw-rider-events",
        "raw_rider_events",
        [
            {"name": "event_id",        "field_type": "STRING",    "mode": "REQUIRED"},
            {"name": "rider_id",        "field_type": "STRING",    "mode": "REQUIRED"},
            {"name": "order_id",        "field_type": "STRING",    "mode": "NULLABLE"},
            {"name": "city",            "field_type": "STRING",    "mode": "REQUIRED"},
            {"name": "latitude",        "field_type": "FLOAT",     "mode": "REQUIRED"},
            {"name": "longitude",       "field_type": "FLOAT",     "mode": "REQUIRED"},
            {"name": "speed_kmh",       "field_type": "FLOAT",     "mode": "REQUIRED"},
            {"name": "status",          "field_type": "STRING",    "mode": "REQUIRED"},
            {"name": "battery_pct",     "field_type": "INTEGER",   "mode": "REQUIRED"},
            {"name": "event_timestamp", "field_type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "producer_ts",     "field_type": "FLOAT",     "mode": "NULLABLE"},
        ],
    ),
]


def _load_one(topic: str, table: str, schema: list[dict], ds: str) -> int:
    """Load one day's JSONL files from GCS into a BigQuery partitioned table.

    Uses WRITE_TRUNCATE on the day partition so reruns are idempotent.
    Returns row count loaded (0 if no files found for that day).
    """
    from google.api_core import exceptions as gcp_exc
    from google.cloud import bigquery

    year, month, day = ds.split("-")
    partition = ds.replace("-", "")
    source_uri = (
        f"gs://{GCS_BUCKET}/pubsub-raw/{topic}/"
        f"year={year}/month={month}/day={day}/*.jsonl"
    )
    destination = f"{GCP_PROJECT}.{BQ_DATASET}.{table}${partition}"

    client = bigquery.Client(project=GCP_PROJECT)
    bq_schema = [bigquery.SchemaField(**f) for f in schema]
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=bq_schema,
        ignore_unknown_values=True,
    )

    try:
        load_job = client.load_table_from_uri(source_uri, destination, job_config=job_config)
        load_job.result()
        rows = load_job.output_rows
        logger.info("Loaded %d rows → %s (source=%s)", rows, destination, source_uri)
        return rows
    except gcp_exc.BadRequest as exc:
        if "No files to load" in str(exc) or "no files" in str(exc).lower():
            logger.warning("No JSONL files found for topic=%s date=%s — skipping", topic, ds)
            return 0
        raise


@dag(
    dag_id="load_pubsub_to_bigquery",
    description="Batch layer: GCS JSONL → BigQuery raw tables (pubsub-raw/{topic}/year/month/day)",
    schedule_interval="30 0 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "airflow",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "email_on_failure": False,
        "email_on_retry": False,
    },
    tags=["batch", "lambda", "bigquery", "gcs"],
)
def load_pubsub_to_bigquery():

    @task
    def load_orders(**context) -> int:
        topic, table, schema = _LOADS[0]
        return _load_one(topic, table, schema, context["ds"])

    @task
    def load_payments(**context) -> int:
        topic, table, schema = _LOADS[1]
        return _load_one(topic, table, schema, context["ds"])

    @task
    def load_rider_events(**context) -> int:
        topic, table, schema = _LOADS[2]
        return _load_one(topic, table, schema, context["ds"])

    @task
    def log_summary(orders: int, payments: int, riders: int, **context) -> None:
        logger.info(
            "load_pubsub_to_bigquery complete ds=%s | orders=%d payments=%d rider_events=%d",
            context["ds"], orders, payments, riders,
        )

    o = load_orders()
    p = load_payments()
    r = load_rider_events()
    log_summary(o, p, r)


dag = load_pubsub_to_bigquery()
