-- Fail if any order has negative revenue.
SELECT count() AS failures
FROM {{ ref('fct_orders') }}
WHERE total_vnd < 0
HAVING failures > 0
