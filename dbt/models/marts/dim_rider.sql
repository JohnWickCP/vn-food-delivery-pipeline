{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(city, rider_id)'
) }}

SELECT
    dm.rider_id,
    dm.city,
    sum(dm.total_pings)                             AS lifetime_pings,
    round(avg(dm.utilization_pct), 1)               AS avg_utilization_pct,
    round(avg(dm.avg_speed_kmh), 1)                 AS avg_speed_kmh,
    round(avg(dm.avg_battery_pct))                  AS avg_battery_pct,
    count(DISTINCT dm.event_date)                   AS active_days
FROM {{ ref('int_delivery_metrics') }} dm
GROUP BY rider_id, city
