import argparse
import os
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg, col, coalesce, count, current_timestamp, greatest,
    lit, sum, unix_timestamp, when,
)

GCS_BUCKET = "vn-food-delivery-lake-739a3554"

DEFAULT_DATE = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")

# parse_known_args: spark-submit injects its own flags into sys.argv
parser = argparse.ArgumentParser()
parser.add_argument("--date", default=DEFAULT_DATE,
                    help="Processing date YYYY-MM-DD (default: yesterday UTC)")
args, _ = parser.parse_known_args()

TARGET_DATE = args.date
date_obj    = datetime.strptime(TARGET_DATE, "%Y-%m-%d")
Y, M, D     = date_obj.year, date_obj.month, date_obj.day

ORDERS_PATH   = f"gs://{GCS_BUCKET}/raw/orders/year={Y}/month={M}/day={D}/"
PAYMENTS_PATH = f"gs://{GCS_BUCKET}/raw/payments/year={Y}/month={M}/day={D}/"
OUTPUT_PATH   = f"gs://{GCS_BUCKET}/batch/daily_summary/date={TARGET_DATE}/"


def build_session() -> SparkSession:
    return SparkSession.builder.appName(f"batch-daily-summary-{TARGET_DATE}").getOrCreate()


def main() -> None:
    spark = build_session()
    spark.sparkContext.setLogLevel("WARN")

    # --- Read -----------------------------------------------------------
    # Batch reads the full day partition in one shot.
    # Unlike streaming (watermark-bounded dedup), here we can deduplicate
    # exactly on order_id alone — no time approximation required.
    orders = (
        spark.read.parquet(ORDERS_PATH)
        .dropDuplicates(["order_id"])
    )

    payments = (
        spark.read.parquet(PAYMENTS_PATH)
        .select("order_id", "payment_id", col("status").alias("payment_status"), "amount_vnd", "processed_at")
        .dropDuplicates(["payment_id"])
    )

    # --- Join -----------------------------------------------------------
    # Full shuffle join is acceptable in batch; streaming avoids it via
    # watermark + append-mode to keep state bounded.
    joined = orders.join(payments, on="order_id", how="left")

    payment_delay = greatest(
        lit(0),
        (unix_timestamp("processed_at") - unix_timestamp("placed_at")).cast("int"),
    )

    # --- Aggregate ------------------------------------------------------
    summary = (
        joined
        .withColumn("payment_delay_sec", payment_delay)
        .groupBy("city", "payment_method", "platform")
        .agg(
            count("order_id")
                .alias("total_orders"),
            count(when(col("status") == "delivered",    1))
                .alias("delivered_orders"),
            count(when(col("status") == "cancelled",    1))
                .alias("cancelled_orders"),
            sum("total_vnd")
                .alias("gross_revenue_vnd"),
            avg("total_vnd")
                .alias("avg_order_vnd"),
            count(when(col("payment_status") == "success", 1))
                .alias("paid_orders"),
            avg(coalesce(col("payment_delay_sec"), lit(0)))
                .alias("avg_payment_delay_seconds"),
        )
        .withColumn("batch_date",    lit(TARGET_DATE))
        .withColumn("processed_at",  current_timestamp())
    )

    # --- Write ----------------------------------------------------------
    # Overwrite ensures re-running the same date is idempotent.
    row_count = summary.count()
    summary.write.mode("overwrite").parquet(OUTPUT_PATH)

    print(f"[batch-daily-summary] date={TARGET_DATE} rows={row_count} output={OUTPUT_PATH}")
    spark.stop()


if __name__ == "__main__":
    main()
