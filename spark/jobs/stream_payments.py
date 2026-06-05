import os
import urllib.request

from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col, expr, year, month, dayofmonth

KAFKA_SERVERS   = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
MINIO_ENDPOINT  = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS    = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET    = os.getenv("MINIO_SECRET_KEY", "minioadmin")
SR_URL          = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
BUCKET          = "food-delivery-lake"
OUTPUT_PATH     = f"s3a://{BUCKET}/raw/payments/"
CHECKPOINT_PATH = f"s3a://{BUCKET}/checkpoints/payments/"


def _fetch_latest_schema(subject: str) -> str:
    url = f"{SR_URL}/subjects/{subject}/versions/latest"
    with urllib.request.urlopen(url) as resp:
        import json
        data = json.loads(resp.read())
        return data["schema"]


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

    schema_str = _fetch_latest_schema("raw.payments-value")

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", "raw.payments")
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
        .select(from_avro(expr("substring(value, 6)"), schema_str).alias("d"))
        .select("d.*")
    )

    payments = (
        raw
        .withColumn("event_timestamp", col("event_timestamp").cast("timestamp"))
        .withColumn("processed_at",    col("processed_at").cast("timestamp"))
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
