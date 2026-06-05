CREATE TABLE IF NOT EXISTS food_delivery.raw_orders (
    order_id          UUID,
    customer_id       UUID,
    restaurant_id     UUID,
    rider_id          UUID,
    city              LowCardinality(String),
    district          String,
    status            LowCardinality(String),
    items             String,                              -- raw JSON array: [{item_id, name, price_vnd, quantity}]
    subtotal_vnd      UInt32,
    delivery_fee_vnd  UInt32,
    discount_vnd      UInt32,
    total_vnd         UInt32,
    payment_method    LowCardinality(String),
    platform          LowCardinality(String),
    placed_at         DateTime64(3, 'Asia/Ho_Chi_Minh'),
    event_timestamp   DateTime64(3, 'UTC'),
    producer_ts       Float64 DEFAULT 0,
    _ingested_at      DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(_ingested_at)
PARTITION BY toYYYYMM(placed_at)
ORDER BY (city, placed_at, order_id)
SETTINGS index_granularity = 8192;


CREATE TABLE IF NOT EXISTS food_delivery.raw_payments (
    payment_id              UUID,
    order_id                UUID,
    amount_vnd              UInt32,
    method                  LowCardinality(String),
    status                  LowCardinality(String),
    gateway_transaction_id  Nullable(String),
    processed_at            DateTime64(3, 'Asia/Ho_Chi_Minh'),
    event_timestamp         DateTime64(3, 'UTC'),
    producer_ts             Float64 DEFAULT 0,
    _ingested_at            DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(_ingested_at)
PARTITION BY toYYYYMM(processed_at)
ORDER BY (processed_at, payment_id)
SETTINGS index_granularity = 8192;


CREATE TABLE IF NOT EXISTS food_delivery.raw_rider_events (
    event_id        UUID,
    rider_id        UUID,
    order_id        Nullable(UUID),
    city            LowCardinality(String),
    latitude        Float32,
    longitude       Float32,
    speed_kmh       Float32,
    status          LowCardinality(String),
    battery_pct     UInt8,
    event_timestamp DateTime64(3, 'UTC'),
    producer_ts     Float64 DEFAULT 0,
    _ingested_at    DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(_ingested_at)
PARTITION BY toYYYYMM(event_timestamp)
ORDER BY (city, event_timestamp, rider_id)
SETTINGS index_granularity = 8192;
