-- Fail if payment amount is more than 10% above order total (data quality check).
SELECT count() AS failures
FROM {{ ref('fct_orders') }}
WHERE payment_amount_vnd IS NOT NULL
  AND payment_amount_vnd > total_vnd * 1.1
HAVING failures > 0
