{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='merge',
    partition_by={
        'field': 'placed_date',
        'data_type': 'date'
    },
    cluster_by=['city'],
    on_schema_change='append_new_columns'
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
    IF(op.order_status = 'cancelled', 1, 0)                      AS is_cancelled,
    IF(op.order_status = 'delivered', 1, 0)                      AS is_delivered,
    IF(op.payment_status = 'success',  1, 0)                     AS is_paid
FROM {{ ref('int_order_payments') }} op

{% if is_incremental() %}
WHERE op.placed_at_local >= (
    SELECT DATETIME_SUB(MAX(placed_at_local), INTERVAL 10 MINUTE)
    FROM {{ this }}
)
{% endif %}
