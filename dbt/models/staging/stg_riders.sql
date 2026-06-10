{{ config(materialized='view') }}

SELECT
    event_id,
    rider_id,
    order_id,
    city,
    latitude,
    longitude,
    speed_kmh,
    LOWER(status)                                                  AS rider_status,
    battery_pct,
    event_timestamp,
    DATE(event_timestamp, 'Asia/Ho_Chi_Minh')                     AS event_date,
    EXTRACT(HOUR FROM DATETIME(event_timestamp, 'Asia/Ho_Chi_Minh')) AS event_hour
FROM {{ source('food_delivery', 'raw_rider_events') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY event_timestamp DESC) = 1
