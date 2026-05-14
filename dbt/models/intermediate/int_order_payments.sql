{{ config(materialized='view') }}

-- Join orders with their payments.
-- LEFT JOIN: some orders may not yet have a payment (e.g. still in-flight).
SELECT
    o.order_id,
    o.customer_id,
    o.restaurant_id,
    o.rider_id,
    o.city,
    o.district,
    o.status                                                        AS order_status,
    o.total_vnd,
    o.total_k_vnd,
    o.payment_method,
    o.platform,
    CASE
        WHEN o.placed_hour BETWEEN 11 AND 13 THEN 'lunch'
        WHEN o.placed_hour BETWEEN 18 AND 20 THEN 'dinner'
        ELSE 'off_peak'
    END                                                             AS meal_period,
    o.placed_at_local,
    o.placed_date,
    o.placed_hour,
    p.payment_id,
    p.payment_status,
    p.amount_vnd                                                    AS payment_amount_vnd,
    p.processed_at_local,
    -- seconds from order placed to payment processed (negative = payment before event emit, expected)
    dateDiff('second', o.placed_at_local, p.processed_at_local)    AS payment_delay_seconds
FROM {{ ref('stg_orders') }} o
LEFT JOIN {{ ref('stg_payments') }} p ON o.order_id = p.order_id
