{{ config(
    materialized='table',
    partition_by={
        'field': 'hour_bucket',
        'data_type': 'datetime',
        'granularity': 'day'
    },
    cluster_by=['city']
) }}

-- Hourly revenue report — primary source for Grafana business metrics dashboard.
SELECT
    city,
    DATETIME_TRUNC(placed_at_local, HOUR)        AS hour_bucket,
    COUNT(*)                                     AS total_orders,
    COUNTIF(order_status = 'delivered')          AS delivered_orders,
    COUNTIF(order_status = 'cancelled')          AS cancelled_orders,
    SUM(total_vnd)                               AS total_revenue_vnd,
    ROUND(SUM(total_vnd) / 1000.0)               AS total_revenue_k_vnd,
    ROUND(AVG(total_vnd))                        AS avg_order_vnd,
    COUNTIF(payment_method = 'momo')             AS momo_orders,
    COUNTIF(payment_method = 'vnpay')            AS vnpay_orders,
    COUNTIF(payment_method = 'zalopay')          AS zalopay_orders,
    COUNTIF(payment_method = 'cash')             AS cash_orders,
    COUNTIF(meal_period = 'lunch')               AS lunch_orders,
    COUNTIF(meal_period = 'dinner')              AS dinner_orders,
    COUNTIF(meal_period = 'off_peak')            AS off_peak_orders
FROM {{ ref('fct_orders') }}
GROUP BY city, hour_bucket
