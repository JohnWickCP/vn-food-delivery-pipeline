import logging
import os
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_json, year, month, dayofmonth,
)
from pyspark.sql.types import (
    ArrayType, IntegerType, LongType, StringType, StructField,
    StructType, TimestampType,
)

logger = logging.getLogger(__name__)

KAFKA_SERVERS   = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
MINIO_ENDPOINT  = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS    = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET    = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BUCKET          = "food-delivery-lake"
OUTPUT_PATH     = f"s3a://{BUCKET}/raw/orders/"
CHECKPOINT_PATH = f"s3a://{BUCKET}/checkpoints/orders/"

ORDER_ITEM_SCHEMA = StructType([
    StructField("item_id",   StringType(),  False),
    StructField("name",      StringType(),  False),
    StructField("price_vnd", LongType(),    False),
    StructField("quantity",  IntegerType(), False),
])

ORDER_SCHEMA = StructType([
    StructField("order_id",          StringType(),                False),
    StructField("customer_id",       StringType(),                False),
    StructField("restaurant_id",     StringType(),                False),
    StructField("rider_id",          StringType(),                False),
    StructField("city",              StringType(),                False),
    StructField("district",          StringType(),                False),
    StructField("status",            StringType(),                False),
    StructField("items",             ArrayType(ORDER_ITEM_SCHEMA), False),
    StructField("subtotal_vnd",      LongType(),                  False),
    StructField("delivery_fee_vnd",  LongType(),                  False),
    StructField("discount_vnd",      LongType(),                  False),
    StructField("total_vnd",         LongType(),                  False),
    StructField("payment_method",    StringType(),                False),
    StructField("platform",          StringType(),                False),
    StructField("placed_at",         TimestampType(),             False),
    StructField("event_timestamp",   TimestampType(),             False),
])


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

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", "raw.orders")
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
        .select(from_json(col("value").cast("string"), ORDER_SCHEMA).alias("d"))
        .select("d.*")
    )

    orders = (
        raw
        .withWatermark("event_timestamp", "10 minutes")
        .dropDuplicates(["order_id", "event_timestamp"])
        # Serialize items array back to JSON string to match ClickHouse raw_orders schema
        .withColumn("items", to_json(col("items")))
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
