{{ config(materialized='view') }}

SELECT
    payment_id,
    order_id,
    amount_vnd,
    amount_vnd / 1000.0                                          AS amount_k_vnd,
    method                                                       AS payment_method,
    LOWER(status)                                                AS payment_status,
    gateway_transaction_id,
    DATETIME(processed_at, 'Asia/Ho_Chi_Minh')                  AS processed_at_local,
    event_timestamp
FROM {{ source('food_delivery', 'raw_payments') }}
