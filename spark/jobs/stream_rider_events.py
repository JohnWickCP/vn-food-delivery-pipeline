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
METRICS_PATH    = f"gs://{GCS_BUCKET}/metrics/batch_skips/"


def _append_skip_metric(spark: SparkSession, job: str, count: int) -> None:
    from datetime import datetime, timezone
    ts = datetime.now(timezone.utc).isoformat()
    (spark.createDataFrame([(ts, job, count)], ["ts", "job", "skip_count"])
         .write.format("json").mode("append")
         .save(METRICS_PATH))

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
    spark.conf.set("spark.sql.shuffle.partitions", "4")

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

    batch_skip_count = 0

    def write_batch_with_timing(batch_df, epoch_id):
        nonlocal batch_skip_count
        try:
            batch_df = batch_df.localCheckpoint(eager=True)
        except Exception as exc:
            exc_str = str(exc)
            if "Item not found" in exc_str or "generation is deleted" in exc_str:
                batch_skip_count += 1
                logger.warning(
                    "BATCH_SKIP epoch=%s reason=stale_gcs_file skip_total=%d",
                    epoch_id, batch_skip_count,
                )
                if batch_skip_count > 5:
                    raise RuntimeError(
                        f"Too many stale-file skips ({batch_skip_count}), aborting stream"
                    ) from exc
                return
            raise

        row_count = batch_df.count()
        if row_count == 0:
            logger.info("BATCH_SKIP epoch=%s reason=empty", epoch_id)
            return

        logger.info("BATCH_START epoch=%s rows=%d", epoch_id, row_count)
        t0 = time.perf_counter()
        try:
            (batch_df.coalesce(1).write
                .format("parquet")
                .option("path", OUTPUT_PATH)
                .partitionBy("year", "month", "day")
                .mode("append")
                .save())
            elapsed = time.perf_counter() - t0
            logger.warning(
                "BATCH_TIMING epoch=%s rows=%d write_sec=%.3f rows_per_sec=%.0f",
                epoch_id, row_count, elapsed, row_count / elapsed,
            )
        except Exception as exc:
            logger.error("foreachBatch write failed epoch=%s rows=%d", epoch_id, row_count, exc_info=True)
            raise

    query = (
        events.writeStream
        .foreachBatch(write_batch_with_timing)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(availableNow=True)
        .outputMode("append")
        .start()
    )
    query.awaitTermination()

    if batch_skip_count > 0:
        logger.warning("RUN_COMPLETE job=stream-rider-events stale_skips=%d", batch_skip_count)
        _append_skip_metric(spark, "stream-rider-events", batch_skip_count)


if __name__ == "__main__":
    main()
