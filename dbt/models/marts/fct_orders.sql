{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(city, placed_date, order_id)'
) }}

SELECT
    op.order_id,
    op.customer_id,
    op.restaurant_id,
    op.rider_id,
    op.city,
    op.district,
    op.order_status,
    op.total_vnd,
    op.total_k_vnd,
    op.payment_method,
    op.platform,
    op.meal_period,
    op.placed_at_local,
    op.placed_date,
    op.placed_hour,
    op.payment_id,
    op.payment_status,
    op.payment_amount_vnd,
    op.processed_at_local,
    op.payment_delay_seconds,
    toUInt8(op.order_status = 'cancelled')                      AS is_cancelled,
    toUInt8(op.order_status = 'delivered')                      AS is_delivered,
    toUInt8(op.payment_status = 'success')                      AS is_paid
FROM {{ ref('int_order_payments') }} op
