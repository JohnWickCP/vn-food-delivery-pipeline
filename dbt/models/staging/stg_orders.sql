{{ config(materialized='view') }}

SELECT
    order_id,
    customer_id,
    restaurant_id,
    rider_id,
    city,
    district,
    lower(status)                                       AS status,
    items,
    subtotal_vnd,
    delivery_fee_vnd,
    discount_vnd,
    total_vnd,
    total_vnd / 1000.0                                  AS total_k_vnd,
    payment_method,
    platform,
    toTimeZone(placed_at, 'Asia/Ho_Chi_Minh')           AS placed_at_local,
    toDate(placed_at)                                   AS placed_date,
    toHour(placed_at)                                   AS placed_hour,
    event_timestamp,
    _ingested_at
FROM {{ source('food_delivery', 'raw_orders') }}
WHERE total_vnd > 0
