-- Batch layer result table — populated daily by Airflow after Spark batch job completes.
-- Source: Spark reads s3a://food-delivery-lake/raw/orders|payments/year=.../month=.../day=.../
--         and writes aggregated Parquet to s3a://food-delivery-lake/batch/daily_summary/date=.../
-- Load:   Airflow uses ClickHouse s3() table function to INSERT from that Parquet.
--
-- Key difference from the real-time path (mv_orders_per_hour):
--   Real-time: approximate dedup (ReplacingMergeTree lazy merge + watermark-bounded Spark state)
--   Batch:     exact dedup (full-day dropDuplicates on order_id without watermark approximation)

CREATE TABLE IF NOT EXISTS food_delivery.batch_daily_city_stats (
    batch_date                  Date,
    city                        LowCardinality(String),
    payment_method              LowCardinality(String),
    platform                    LowCardinality(String),
    total_orders                UInt32,
    delivered_orders            UInt32,
    cancelled_orders            UInt32,
    gross_revenue_vnd           UInt64,
    avg_order_vnd               Float64,
    paid_orders                 UInt32,
    avg_payment_delay_seconds   Float64,
    processed_at                DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(processed_at)
PARTITION BY toYYYYMM(batch_date)
ORDER BY (batch_date, city, payment_method, platform)
SETTINGS index_granularity = 8192;
