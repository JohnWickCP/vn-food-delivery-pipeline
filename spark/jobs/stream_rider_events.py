import logging
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth
from pyspark.sql.types import (
    DoubleType, FloatType, IntegerType, StringType, StructField, StructType, TimestampType,
)

logger = logging.getLogger(__name__)

GCS_BUCKET      = "vn-food-delivery-lake-739a3554"
INPUT_PATH      = f"gs://{GCS_BUCKET}/pubsub-raw/raw-rider-events/"
OUTPUT_PATH     = f"gs://{GCS_BUCKET}/raw/rider_events/"
CHECKPOINT_PATH = f"gs://{GCS_BUCKET}/checkpoints/rider_events/"

INPUT_SCHEMA = StructType([
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


def build_session() -> SparkSession:
    return SparkSession.builder.appName("stream-rider-events").getOrCreate()


def main() -> None:
    spark = build_session()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.files.ignoreMissingFiles", "true")

    raw = (
        spark.readStream
        .format("json")
        .schema(INPUT_SCHEMA)
        .option("path", INPUT_PATH)
        .option("latestFirst", "false")
        .option("maxFilesPerTrigger", "50")
        .load()
    )

    events = (
        raw
        .withWatermark("event_timestamp", "10 minutes")
        .dropDuplicates(["event_id", "event_timestamp"])
        .withColumn("year",  year(col("event_timestamp")))
        .withColumn("month", month(col("event_timestamp")))
        .withColumn("day",   dayofmonth(col("event_timestamp")))
    )

    def write_batch_with_timing(batch_df, epoch_id):
        batch_df = batch_df.cache()
        try:
            try:
                row_count = batch_df.count()
            except Exception as exc:
                exc_str = str(exc)
                if "Item not found" in exc_str or "generation is deleted" in exc_str:
                    print(f"BATCH_SKIP epoch={epoch_id} (stale GCS file, skipping)", flush=True)
                    return
                raise

            if row_count == 0:
                print(f"BATCH_SKIP epoch={epoch_id} (empty)", flush=True)
                return

            print(f"BATCH_START epoch={epoch_id} rows={row_count}", flush=True)
            t0 = time.perf_counter()
            try:
                (batch_df.write
                    .format("parquet")
                    .option("path", OUTPUT_PATH)
                    .partitionBy("year", "month", "day")
                    .mode("append")
                    .save())
                elapsed = time.perf_counter() - t0
                msg = (
                    f"BATCH_TIMING epoch={epoch_id} rows={row_count} "
                    f"write_sec={elapsed:.3f} rows_per_sec={row_count/elapsed:.0f}"
                )
                print(msg, flush=True)
                logger.warning(msg)
            except Exception as exc:
                exc_str = str(exc)
                if "Item not found" in exc_str or "generation is deleted" in exc_str:
                    print(f"BATCH_SKIP epoch={epoch_id} rows={row_count} (stale GCS file, skipping)", flush=True)
                    return
                print(f"BATCH_ERROR epoch={epoch_id} rows={row_count} error={exc}", flush=True)
                logger.error("foreachBatch write failed epoch=%s", epoch_id, exc_info=True)
                raise
        finally:
            batch_df.unpersist()

    (
        events.writeStream
        .foreachBatch(write_batch_with_timing)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="500 milliseconds")
        .outputMode("append")
        .start()
        .awaitTermination()
    )


if __name__ == "__main__":
    main()
