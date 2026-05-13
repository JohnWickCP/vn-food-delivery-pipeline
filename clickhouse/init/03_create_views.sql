-- Materialized view: orders per hour per city (pre-aggregated for Grafana)
CREATE MATERIALIZED VIEW IF NOT EXISTS food_delivery.mv_orders_per_hour
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(hour)
ORDER BY (city, hour)
AS
SELECT
    city,
    toStartOfHour(placed_at) AS hour,
    count()                  AS total_orders,
    sum(total_vnd)           AS total_revenue_vnd,
    countIf(status = 'cancelled') AS cancelled_orders
FROM food_delivery.raw_orders
GROUP BY city, hour;
