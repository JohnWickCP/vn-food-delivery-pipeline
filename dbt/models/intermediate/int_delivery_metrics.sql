{{ config(materialized='view') }}

-- Per-rider daily metrics derived from GPS pings.
SELECT
    rider_id,
    city,
    event_date,
    count()                                                         AS total_pings,
    countIf(rider_status = 'on_delivery')                          AS delivery_pings,
    countIf(rider_status = 'available')                            AS idle_pings,
    avg(speed_kmh)                                                  AS avg_speed_kmh,
    max(speed_kmh)                                                  AS max_speed_kmh,
    avg(battery_pct)                                                AS avg_battery_pct,
    min(battery_pct)                                                AS min_battery_pct,
    round(
        countIf(rider_status = 'on_delivery') / count() * 100, 1
    )                                                               AS utilization_pct
FROM {{ ref('stg_riders') }}
GROUP BY rider_id, city, event_date
