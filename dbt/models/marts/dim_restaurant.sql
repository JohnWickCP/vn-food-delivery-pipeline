{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(city, restaurant_id)'
) }}

SELECT
    restaurant_id,
    city,
    count()                                          AS total_orders,
    sum(total_vnd)                                   AS total_revenue_vnd,
    round(avg(total_vnd))                            AS avg_order_vnd,
    countIf(order_status = 'delivered')              AS delivered_orders,
    countIf(order_status = 'cancelled')              AS cancelled_orders,
    round(
        countIf(order_status = 'cancelled') / count() * 100, 1
    )                                                AS cancellation_rate_pct,
    min(placed_date)                                 AS first_order_date,
    max(placed_date)                                 AS last_order_date
FROM {{ ref('fct_orders') }}
GROUP BY restaurant_id, city
