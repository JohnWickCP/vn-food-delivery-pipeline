{{ config(materialized='view') }}

SELECT
    event_id,
    rider_id,
    order_id,
    city,
    latitude,
    longitude,
    speed_kmh,
    lower(status)                   AS rider_status,
    battery_pct,
    event_timestamp,
    toDate(event_timestamp)         AS event_date,
    toHour(event_timestamp)         AS event_hour,
    _ingested_at
FROM {{ source('food_delivery', 'raw_rider_events') }} FINAL
