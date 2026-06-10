{{ config(
    materialized='table',
    cluster_by=['city']
) }}

SELECT
    dm.rider_id,
    dm.city,
    SUM(dm.total_pings)                             AS lifetime_pings,
    ROUND(AVG(dm.utilization_pct), 1)               AS avg_utilization_pct,
    ROUND(AVG(dm.avg_speed_kmh), 1)                 AS avg_speed_kmh,
    ROUND(AVG(dm.avg_battery_pct))                  AS avg_battery_pct,
    COUNT(DISTINCT dm.event_date)                   AS active_days
FROM {{ ref('int_delivery_metrics') }} dm
GROUP BY rider_id, city
