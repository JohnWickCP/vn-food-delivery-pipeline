import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, year, month, dayofmonth
from pyspark.sql.types import (
    LongType, StringType, StructField, StructType, TimestampType,
)

KAFKA_SERVERS   = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
MINIO_ENDPOINT  = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS    = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET    = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BUCKET          = "food-delivery-lake"
OUTPUT_PATH     = f"s3a://{BUCKET}/raw/payments/"
CHECKPOINT_PATH = f"s3a://{BUCKET}/checkpoints/payments/"

PAYMENT_SCHEMA = StructType([
    StructField("payment_id",             StringType(),    False),
    StructField("order_id",               StringType(),    False),
    StructField("amount_vnd",             LongType(),      False),
    StructField("method",                 StringType(),    False),
    StructField("status",                 StringType(),    False),
    StructField("gateway_transaction_id", StringType(),    True),
    StructField("processed_at",           TimestampType(), False),
    StructField("event_timestamp",        TimestampType(), False),
])


def build_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("stream-payments")
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
        .option("subscribe", "raw.payments")
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
        .select(from_json(col("value").cast("string"), PAYMENT_SCHEMA).alias("d"))
        .select("d.*")
    )

    payments = (
        raw
        .withWatermark("event_timestamp", "10 minutes")
        .dropDuplicates(["payment_id", "event_timestamp"])
        .withColumn("year",  year(col("event_timestamp")))
        .withColumn("month", month(col("event_timestamp")))
        .withColumn("day",   dayofmonth(col("event_timestamp")))
    )

    (
        payments.writeStream
        .format("parquet")
        .option("path", OUTPUT_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .partitionBy("year", "month", "day")
        .trigger(processingTime="500 milliseconds")
        .outputMode("append")
        .start()
        .awaitTermination()
    )


if __name__ == "__main__":
    main()
