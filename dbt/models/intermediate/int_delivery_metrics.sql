{{ config(materialized='view') }}

-- Per-rider daily metrics derived from GPS pings.
SELECT
    rider_id,
    city,
    event_date,
    COUNT(*)                                                          AS total_pings,
    COUNTIF(rider_status = 'on_delivery')                            AS delivery_pings,
    COUNTIF(rider_status = 'available')                              AS idle_pings,
    AVG(speed_kmh)                                                    AS avg_speed_kmh,
    MAX(speed_kmh)                                                    AS max_speed_kmh,
    AVG(battery_pct)                                                  AS avg_battery_pct,
    MIN(battery_pct)                                                  AS min_battery_pct,
    ROUND(
        COUNTIF(rider_status = 'on_delivery') / COUNT(*) * 100, 1
    )                                                                 AS utilization_pct
FROM {{ ref('stg_riders') }}
GROUP BY rider_id, city, event_date
