import logging
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth
from pyspark.sql.types import (
    DoubleType, LongType, StringType, StructField, StructType, TimestampType,
)

logger = logging.getLogger(__name__)

GCS_BUCKET      = "vn-food-delivery-lake-739a3554"
INPUT_PATH      = f"gs://{GCS_BUCKET}/pubsub-raw/raw-payments/"
OUTPUT_PATH     = f"gs://{GCS_BUCKET}/raw/payments/"
CHECKPOINT_PATH = f"gs://{GCS_BUCKET}/checkpoints/payments/"

INPUT_SCHEMA = StructType([
    StructField("payment_id",             StringType(),    False),
    StructField("order_id",               StringType(),    False),
    StructField("amount_vnd",             LongType(),      False),
    StructField("method",                 StringType(),    False),
    StructField("status",                 StringType(),    False),
    StructField("gateway_transaction_id", StringType(),    True),
    StructField("processed_at",           TimestampType(), False),
    StructField("event_timestamp",        TimestampType(), False),
    StructField("producer_ts",            DoubleType(),    False),
])


def build_session() -> SparkSession:
    return SparkSession.builder.appName("stream-payments").getOrCreate()


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

    payments = (
        raw
        .withWatermark("event_timestamp", "10 minutes")
        .dropDuplicates(["payment_id", "event_timestamp"])
        .withColumn("year",  year(col("event_timestamp")))
        .withColumn("month", month(col("event_timestamp")))
        .withColumn("day",   dayofmonth(col("event_timestamp")))
    )

    def write_batch_with_timing(batch_df, epoch_id):
        try:
            batch_df = batch_df.localCheckpoint(eager=True)
        except Exception as exc:
            exc_str = str(exc)
            if "Item not found" in exc_str or "generation is deleted" in exc_str:
                print(f"BATCH_SKIP epoch={epoch_id} (stale GCS file, skipping)", flush=True)
                return
            raise

        row_count = batch_df.count()
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
            print(f"BATCH_ERROR epoch={epoch_id} rows={row_count} error={exc}", flush=True)
            logger.error("foreachBatch write failed epoch=%s", epoch_id, exc_info=True)
            raise

    (
        payments.writeStream
        .foreachBatch(write_batch_with_timing)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="500 milliseconds")
        .outputMode("append")
        .start()
        .awaitTermination()
    )


if __name__ == "__main__":
    main()
