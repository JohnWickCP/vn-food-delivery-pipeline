from __future__ import annotations

import logging
import os
from datetime import datetime

from airflow.decorators import dag, task

logger = logging.getLogger(__name__)

_CH_HOST     = os.getenv("CLICKHOUSE_HOST", "clickhouse")
_CH_PORT     = int(os.getenv("CLICKHOUSE_NATIVE_PORT", "9000"))  # internal Docker port
_CH_DB       = "food_delivery"
_CH_USER     = os.getenv("CLICKHOUSE_USER", "default")
_CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")

_MINIO_ENDPOINT  = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
_MINIO_ACCESS    = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
_MINIO_SECRET    = os.getenv("MINIO_SECRET_KEY", "minioadmin")
_BUCKET          = "food-delivery-lake"

# Columns to SELECT from Parquet (excludes _ingested_at which has DEFAULT now())
_ORDERS_COLS = (
    "order_id, customer_id, restaurant_id, rider_id, city, district, status, items, "
    "subtotal_vnd, delivery_fee_vnd, discount_vnd, total_vnd, payment_method, platform, "
    "placed_at, event_timestamp"
)
_PAYMENTS_COLS = (
    "payment_id, order_id, amount_vnd, method, status, "
    "gateway_transaction_id, processed_at, event_timestamp"
)
_RIDERS_COLS = (
    "event_id, rider_id, order_id, city, latitude, longitude, "
    "speed_kmh, status, battery_pct, event_timestamp"
)


def _client():
    from clickhouse_driver import Client
    return Client(
        host=_CH_HOST,
        port=_CH_PORT,
        database=_CH_DB,
        user=_CH_USER,
        password=_CH_PASSWORD,
    )


def _load_partition(topic: str, table: str, columns: str, year: int, month: int, day: int) -> int:
    path = f"{_MINIO_ENDPOINT}/{_BUCKET}/raw/{topic}/year={year}/month={month}/day={day}/*.parquet"
    sql = (
        f"INSERT INTO {_CH_DB}.{table} ({columns}) "
        f"SELECT {columns} FROM s3('{path}', '{_MINIO_ACCESS}', '{_MINIO_SECRET}', 'Parquet')"
    )

    client = _client()
    try:
        client.execute(sql)
        rows = client.execute(
            f"SELECT count() FROM {_CH_DB}.{table} "
            f"WHERE toDate(event_timestamp) = '{year}-{month:02d}-{day:02d}'"
        )[0][0]
        logger.info("[%s] %d rows loaded for %s-%02d-%02d", table, rows, year, month, day)
        return rows
    except Exception as exc:
        # No Parquet files yet for this partition — not a hard failure
        if "No files" in str(exc) or "Cannot open file" in str(exc) or "CANNOT_OPEN_FILE" in str(exc):
            logger.warning("[%s] No files found at %s — skipping", table, path)
            return 0
        raise
    finally:
        client.disconnect()


@dag(
    schedule_interval="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": __import__("datetime").timedelta(minutes=5)},
    tags=["pipeline", "load"],
)
def load_minio_to_clickhouse():
    """Hourly load: MinIO Parquet → ClickHouse raw tables (orders, payments, rider_events)."""

    @task
    def load_orders(data_interval_start=None):
        ds = data_interval_start
        return _load_partition("orders", "raw_orders", _ORDERS_COLS, ds.year, ds.month, ds.day)

    @task
    def load_payments(data_interval_start=None):
        ds = data_interval_start
        return _load_partition("payments", "raw_payments", _PAYMENTS_COLS, ds.year, ds.month, ds.day)

    @task
    def load_rider_events(data_interval_start=None):
        ds = data_interval_start
        return _load_partition("rider_events", "raw_rider_events", _RIDERS_COLS, ds.year, ds.month, ds.day)

    # 3 topics load in parallel — no dependency between them
    load_orders()
    load_payments()
    load_rider_events()


dag = load_minio_to_clickhouse()
