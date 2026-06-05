import io
import json
import logging
import os
import urllib.request

import fastavro
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, year, month, dayofmonth
from pyspark.sql.types import (
    DoubleType, FloatType, IntegerType, StringType, StructField, StructType, TimestampType,
)

logger = logging.getLogger(__name__)

KAFKA_SERVERS   = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
MINIO_ENDPOINT  = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS    = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET    = os.getenv("MINIO_SECRET_KEY", "minioadmin")
SR_URL          = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
BUCKET          = "food-delivery-lake"
OUTPUT_PATH     = f"s3a://{BUCKET}/raw/rider_events/"
CHECKPOINT_PATH = f"s3a://{BUCKET}/checkpoints/rider_events/"

OUTPUT_SCHEMA = StructType([
    StructField("event_id",         StringType(),    False),
    StructField("rider_id",         StringType(),    False),
    StructField("order_id",         StringType(),    True),
    StructField("city",             StringType(),    False),
    StructField("latitude",         FloatType(),     False),
    StructField("longitude",        FloatType(),     False),
    StructField("speed_kmh",        FloatType(),     False),
    StructField("status",           StringType(),    False),
    StructField("battery_pct",      IntegerType(),   False),
    StructField("event_timestamp",  TimestampType(), False),
    StructField("producer_ts",      DoubleType(),    False),
])


def _fetch_avro_schema(subject: str) -> dict:
    url = f"{SR_URL}/subjects/{subject}/versions/latest"
    with urllib.request.urlopen(url) as resp:
        data = json.loads(resp.read())
        return json.loads(data["schema"])


def build_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("stream-rider-events")
        .config("spark.hadoop.fs.s3a.endpoint",               MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",             MINIO_ACCESS)
        .config("spark.hadoop.fs.s3a.secret.key",             MINIO_SECRET)
        .config("spark.hadoop.fs.s3a.path.style.access",      "true")
        .config("spark.hadoop.fs.s3a.impl",                   "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def main() -> None:
    spark = build_session()
    spark.sparkContext.setLogLevel("WARN")

    avro_schema = _fetch_avro_schema("raw.rider_events-value")
    parsed_schema = fastavro.parse_schema(avro_schema)

    @udf(returnType=OUTPUT_SCHEMA)
    def decode_avro(raw_bytes):
        if raw_bytes is None:
            return None
        buf = io.BytesIO(bytes(raw_bytes)[5:])
        record = fastavro.schemaless_reader(buf, parsed_schema)
        from datetime import datetime
        def to_ts(s):
            if not s:
                return None
            try:
                return datetime.fromisoformat(s.replace("Z", "+00:00"))
            except Exception:
                return None
        return (
            str(record["event_id"]),
            str(record["rider_id"]),
            record.get("order_id"),
            record["city"],
            float(record["latitude"]),
            float(record["longitude"]),
            float(record["speed_kmh"]),
            record["status"],
            int(record["battery_pct"]),
            to_ts(record["event_timestamp"]),
            float(record["producer_ts"]),
        )

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", "raw.rider_events")
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
        .select(decode_avro(col("value")).alias("d"))
        .select("d.*")
    )

    events = (
        raw
        .withWatermark("event_timestamp", "10 minutes")
        .dropDuplicates(["event_id", "event_timestamp"])
        .withColumn("year",  year(col("event_timestamp")))
        .withColumn("month", month(col("event_timestamp")))
        .withColumn("day",   dayofmonth(col("event_timestamp")))
    )

    (
        events.writeStream
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
