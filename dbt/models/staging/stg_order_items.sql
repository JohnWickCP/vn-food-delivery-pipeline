{{ config(materialized='view') }}

-- Explode JSON items array into one row per item.
-- items column in raw_orders is a JSON string: [{"item_id":...,"name":...,"price_vnd":...,"quantity":...}]
-- ClickHouse JSONExtractArrayRaw returns array of raw JSON strings; arrayJoin unnests.
WITH exploded AS (
    SELECT
        order_id,
        city,
        placed_at,
        arrayJoin(JSONExtractArrayRaw(items)) AS item_json
    FROM {{ source('food_delivery', 'raw_orders') }} FINAL
    WHERE items != '[]' AND items != ''
)
SELECT
    order_id,
    city,
    toDate(placed_at)                                           AS placed_date,
    JSONExtractString(item_json, 'item_id')                    AS item_id,
    JSONExtractString(item_json, 'name')                       AS item_name,
    JSONExtractUInt(item_json, 'price_vnd')                    AS price_vnd,
    JSONExtractUInt(item_json, 'quantity')                     AS quantity,
    JSONExtractUInt(item_json, 'price_vnd')
        * JSONExtractUInt(item_json, 'quantity')               AS line_total_vnd
FROM exploded
WHERE JSONExtractString(item_json, 'item_id') != ''
