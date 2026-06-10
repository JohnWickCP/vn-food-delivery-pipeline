import argparse
import logging
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

GCS_BUCKET = "vn-food-delivery-lake-739a3554"

TABLES = {
    "orders":       f"gs://{GCS_BUCKET}/raw/orders/",
    "payments":     f"gs://{GCS_BUCKET}/raw/payments/",
    "rider_events": f"gs://{GCS_BUCKET}/raw/rider_events/",
}


def compact_partition(spark: SparkSession, table: str, base_path: str, date: str) -> None:
    year, month, day = date.split("-")
    partition_path = f"{base_path}year={year}/month={int(month)}/day={int(day)}/"

    try:
        df = spark.read.format("parquet").load(partition_path).cache()
    except Exception as exc:
        exc_str = str(exc)
        if "Path does not exist" in exc_str or "Unable to infer schema" in exc_str:
            logger.info("COMPACT_SKIP table=%s date=%s reason=no_data", table, date)
            return
        raise

    count = df.count()
    if count == 0:
        df.unpersist()
        logger.info("COMPACT_SKIP table=%s date=%s reason=empty", table, date)
        return

    # cache() + count() already materialized data into executor memory;
    # write() reads from cache, not GCS — safe to overwrite the source path
    (df.coalesce(1)
       .write
       .format("parquet")
       .mode("overwrite")
       .save(partition_path))

    df.unpersist()
    logger.warning("COMPACT_DONE table=%s date=%s rows=%d", table, date, count)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--date",
        default=None,
        help="Partition date to compact (YYYY-MM-DD). Defaults to yesterday UTC.",
    )
    parser.add_argument(
        "--table",
        default="all",
        choices=[*TABLES, "all"],
        help="Table to compact, or 'all'.",
    )
    args = parser.parse_args()

    date = args.date or (
        datetime.now(timezone.utc) - timedelta(days=1)
    ).strftime("%Y-%m-%d")

    spark = SparkSession.builder.appName("compact-parquet").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    tables = TABLES if args.table == "all" else {args.table: TABLES[args.table]}
    for table, path in tables.items():
        compact_partition(spark, table, path, date)

    spark.stop()


if __name__ == "__main__":
    main()
