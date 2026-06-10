{{ config(
    materialized='table',
    cluster_by=['city']
) }}

SELECT
    restaurant_id,
    city,
    COUNT(*)                                          AS total_orders,
    SUM(total_vnd)                                    AS total_revenue_vnd,
    ROUND(AVG(total_vnd))                             AS avg_order_vnd,
    COUNTIF(order_status = 'delivered')               AS delivered_orders,
    COUNTIF(order_status = 'cancelled')               AS cancelled_orders,
    ROUND(
        COUNTIF(order_status = 'cancelled') / COUNT(*) * 100, 1
    )                                                 AS cancellation_rate_pct,
    MIN(placed_date)                                  AS first_order_date,
    MAX(placed_date)                                  AS last_order_date
FROM {{ ref('fct_orders') }}
GROUP BY restaurant_id, city
