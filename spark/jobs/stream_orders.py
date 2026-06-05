import logging
import os
import time
import urllib.request

from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import (
    col, expr, to_json, year, month, dayofmonth,
)

logger = logging.getLogger(__name__)

KAFKA_SERVERS   = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
MINIO_ENDPOINT  = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS    = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET    = os.getenv("MINIO_SECRET_KEY", "minioadmin")
SR_URL          = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
BUCKET          = "food-delivery-lake"
OUTPUT_PATH     = f"s3a://{BUCKET}/raw/orders/"
CHECKPOINT_PATH = f"s3a://{BUCKET}/checkpoints/orders/"


def _fetch_latest_schema(subject: str) -> str:
    url = f"{SR_URL}/subjects/{subject}/versions/latest"
    with urllib.request.urlopen(url) as resp:
        import json
        data = json.loads(resp.read())
        return data["schema"]


def build_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("stream-orders")
        .config("spark.hadoop.fs.s3a.endpoint",              MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",            MINIO_ACCESS)
        .config("spark.hadoop.fs.s3a.secret.key",            MINIO_SECRET)
        .config("spark.hadoop.fs.s3a.path.style.access",     "true")
        .config("spark.hadoop.fs.s3a.impl",                  "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled","false")
        .getOrCreate()
    )


def main() -> None:
    spark = build_session()
    spark.sparkContext.setLogLevel("WARN")

    # Fetch Avro schema from Schema Registry (registered by generator on first produce)
    schema_str = _fetch_latest_schema("raw.orders-value")

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", "raw.orders")
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
        # Skip Confluent 5-byte magic header (1 magic byte + 4 schema-id bytes)
        .select(from_avro(expr("substring(value, 6)"), schema_str).alias("d"))
        .select("d.*")
    )

    orders = (
        raw
        .withColumn("event_timestamp", col("event_timestamp").cast("timestamp"))
        .withColumn("placed_at",       col("placed_at").cast("timestamp"))
        .withWatermark("event_timestamp", "10 minutes")
        .dropDuplicates(["order_id", "event_timestamp"])
        .withColumn("year",  year(col("event_timestamp")))
        .withColumn("month", month(col("event_timestamp")))
        .withColumn("day",   dayofmonth(col("event_timestamp")))
    )

    def write_batch_with_timing(batch_df, epoch_id):
        if batch_df.isEmpty():
            return
        row_count = batch_df.count()
        t0 = time.perf_counter()
        (batch_df.write
            .format("parquet")
            .option("path", OUTPUT_PATH)
            .partitionBy("year", "month", "day")
            .mode("append")
            .save())
        elapsed = time.perf_counter() - t0
        logger.warning(
            "BATCH_TIMING epoch=%d rows=%d write_sec=%.3f rows_per_sec=%.0f",
            epoch_id, row_count, elapsed, row_count / elapsed if elapsed > 0 else 0,
        )

    (
        orders.writeStream
        .foreachBatch(write_batch_with_timing)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="500 milliseconds")
        .outputMode("append")
        .start()
        .awaitTermination()
    )


if __name__ == "__main__":
    main()
