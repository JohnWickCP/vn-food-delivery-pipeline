-- v3 real-time ingestion path: Kafka → ClickHouse Kafka Engine → ReplacingMergeTree
--
-- Pattern (3 objects required per topic):
--   kafka_<topic>_queue  — Kafka Engine table (buffer only — never SELECT from this)
--   raw_<topic>          — ReplacingMergeTree (storage — defined in 02_create_raw_tables.sql)
--   <topic>_mv           — Materialized View (auto-triggered pipeline between the two)
--
-- Consumer groups are independent of Spark consumer groups.
-- Dedup: ReplacingMergeTree deduplicates lazily on merge (or query with FINAL).

-- ─────────────────────────────────────────────────────────────────────────────
-- ORDERS
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS food_delivery.kafka_orders_queue (
    order_id          String,
    customer_id       String,
    restaurant_id     String,
    rider_id          String,
    city              String,
    district          String,
    status            String,
    items             String,   -- JSON array serialised to string via input_format_json_read_arrays_as_strings
    subtotal_vnd      UInt32,
    delivery_fee_vnd  UInt32,
    discount_vnd      UInt32,
    total_vnd         UInt32,
    payment_method    String,
    platform          String,
    placed_at         String,
    event_timestamp   String,
    producer_ts       Float64
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list                        = 'kafka:29092',
    kafka_topic_list                         = 'raw.orders',
    kafka_group_name                         = 'clickhouse-consumer-orders',
    kafka_format                             = 'JSONEachRow',
    kafka_skip_broken_messages               = 10,
    input_format_json_read_arrays_as_strings = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS food_delivery.orders_mv TO food_delivery.raw_orders AS
SELECT
    toUUID(order_id)                              AS order_id,
    toUUID(customer_id)                           AS customer_id,
    toUUID(restaurant_id)                         AS restaurant_id,
    toUUID(rider_id)                              AS rider_id,
    city,
    district,
    status,
    items,
    subtotal_vnd,
    delivery_fee_vnd,
    discount_vnd,
    total_vnd,
    payment_method,
    platform,
    parseDateTime64BestEffort(placed_at, 3)       AS placed_at,
    parseDateTime64BestEffort(event_timestamp, 3) AS event_timestamp,
    producer_ts,
    now()                                         AS _ingested_at
FROM food_delivery.kafka_orders_queue;


-- ─────────────────────────────────────────────────────────────────────────────
-- PAYMENTS
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS food_delivery.kafka_payments_queue (
    payment_id              String,
    order_id                String,
    amount_vnd              UInt32,
    method                  String,
    status                  String,
    gateway_transaction_id  Nullable(String),
    processed_at            String,
    event_timestamp         String,
    producer_ts             Float64
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list            = 'kafka:29092',
    kafka_topic_list             = 'raw.payments',
    kafka_group_name             = 'clickhouse-consumer-payments',
    kafka_format                 = 'JSONEachRow',
    kafka_skip_broken_messages   = 10;

CREATE MATERIALIZED VIEW IF NOT EXISTS food_delivery.payments_mv TO food_delivery.raw_payments AS
SELECT
    toUUID(payment_id)                              AS payment_id,
    toUUID(order_id)                                AS order_id,
    amount_vnd,
    method,
    status,
    gateway_transaction_id,
    parseDateTime64BestEffort(processed_at, 3)      AS processed_at,
    parseDateTime64BestEffort(event_timestamp, 3)   AS event_timestamp,
    producer_ts,
    now()                                           AS _ingested_at
FROM food_delivery.kafka_payments_queue;


-- ─────────────────────────────────────────────────────────────────────────────
-- RIDER EVENTS
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS food_delivery.kafka_rider_events_queue (
    event_id        String,
    rider_id        String,
    order_id        String,   -- empty string when rider has no active order (JSON null → '')
    city            String,
    latitude        Float32,
    longitude       Float32,
    speed_kmh       Float32,
    status          String,
    battery_pct     UInt8,
    event_timestamp String,
    producer_ts     Float64
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list            = 'kafka:29092',
    kafka_topic_list             = 'raw.rider_events',
    kafka_group_name             = 'clickhouse-consumer-rider-events',
    kafka_format                 = 'JSONEachRow',
    kafka_skip_broken_messages   = 10;

CREATE MATERIALIZED VIEW IF NOT EXISTS food_delivery.rider_events_mv TO food_delivery.raw_rider_events AS
SELECT
    toUUID(event_id)                                AS event_id,
    toUUID(rider_id)                                AS rider_id,
    toUUIDOrNull(order_id)                          AS order_id,   -- NULL when empty string
    city,
    latitude,
    longitude,
    speed_kmh,
    status,
    battery_pct,
    parseDateTime64BestEffort(event_timestamp, 3)   AS event_timestamp,
    producer_ts,
    now()                                           AS _ingested_at
FROM food_delivery.kafka_rider_events_queue;
