{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(city, hour_bucket)'
) }}

-- Hourly revenue report — primary source for Grafana business metrics dashboard.
SELECT
    city,
    toStartOfHour(placed_at_local)              AS hour_bucket,
    count()                                     AS total_orders,
    countIf(order_status = 'delivered')         AS delivered_orders,
    countIf(order_status = 'cancelled')         AS cancelled_orders,
    sum(total_vnd)                              AS total_revenue_vnd,
    round(sum(total_vnd) / 1000.0)              AS total_revenue_k_vnd,
    round(avg(total_vnd))                       AS avg_order_vnd,
    countIf(payment_method = 'momo')            AS momo_orders,
    countIf(payment_method = 'vnpay')           AS vnpay_orders,
    countIf(payment_method = 'zalopay')         AS zalopay_orders,
    countIf(payment_method = 'cash')            AS cash_orders,
    countIf(meal_period = 'lunch')              AS lunch_orders,
    countIf(meal_period = 'dinner')             AS dinner_orders,
    countIf(meal_period = 'off_peak')           AS off_peak_orders
FROM {{ ref('fct_orders') }}
GROUP BY city, hour_bucket
ORDER BY city, hour_bucket DESC
