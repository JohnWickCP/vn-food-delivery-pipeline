{{ config(materialized='view') }}

-- Explode JSON items array into one row per item.
-- items column in raw_orders is a JSON string: [{"item_id":...,"name":...,"price_vnd":...,"quantity":...}]
-- BigQuery: JSON_EXTRACT_ARRAY returns ARRAY<JSON>, UNNEST flattens it.
WITH exploded AS (
    SELECT
        order_id,
        city,
        placed_at,
        item_json
    FROM {{ source('food_delivery', 'raw_orders') }},
    UNNEST(JSON_EXTRACT_ARRAY(items)) AS item_json
    WHERE items IS NOT NULL AND items NOT IN ('[]', '')
)
SELECT
    order_id,
    city,
    DATE(placed_at, 'Asia/Ho_Chi_Minh')                                             AS placed_date,
    JSON_EXTRACT_SCALAR(item_json, '$.item_id')                                     AS item_id,
    JSON_EXTRACT_SCALAR(item_json, '$.name')                                        AS item_name,
    CAST(JSON_EXTRACT_SCALAR(item_json, '$.price_vnd') AS INT64)                   AS price_vnd,
    CAST(JSON_EXTRACT_SCALAR(item_json, '$.quantity')  AS INT64)                   AS quantity,
    CAST(JSON_EXTRACT_SCALAR(item_json, '$.price_vnd') AS INT64)
        * CAST(JSON_EXTRACT_SCALAR(item_json, '$.quantity') AS INT64)              AS line_total_vnd
FROM exploded
WHERE JSON_EXTRACT_SCALAR(item_json, '$.item_id') IS NOT NULL
  AND JSON_EXTRACT_SCALAR(item_json, '$.item_id') != ''
