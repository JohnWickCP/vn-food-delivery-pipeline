-- Fail if any order has an unrecognised status.
SELECT COUNT(*) AS failures
FROM {{ ref('fct_orders') }}
WHERE order_status NOT IN ('placed', 'confirmed', 'preparing', 'picked_up', 'delivered', 'cancelled')
HAVING failures > 0
