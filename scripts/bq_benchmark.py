import time
from google.cloud import bigquery

client = bigquery.Client(project="project-739a3554-aa69-4eab-9e2")

queries = [
    ("Q1_count", "SELECT COUNT(*) as total FROM `project-739a3554-aa69-4eab-9e2.food_delivery_raw.raw_orders`"),
    ("Q2_group_city", "SELECT city, COUNT(*) as c FROM `project-739a3554-aa69-4eab-9e2.food_delivery_raw.raw_orders` GROUP BY city"),
    ("Q3_join_payments", "SELECT o.city, COUNT(*) as orders, SUM(p.amount_vnd) as paid FROM `project-739a3554-aa69-4eab-9e2.food_delivery_raw.raw_orders` o JOIN `project-739a3554-aa69-4eab-9e2.food_delivery_raw.raw_payments` p ON o.order_id=p.order_id GROUP BY o.city"),
    ("Q4_hourly_bucket", "SELECT EXTRACT(HOUR FROM placed_at AT TIME ZONE 'Asia/Ho_Chi_Minh') as hr, COUNT(*) as c FROM `project-739a3554-aa69-4eab-9e2.food_delivery_raw.raw_orders` GROUP BY hr ORDER BY hr"),
    ("Q5_daily_revenue", "SELECT DATE(placed_at) as d, SUM(total_vnd) as rev, COUNT(*) as orders FROM `project-739a3554-aa69-4eab-9e2.food_delivery_raw.raw_orders` GROUP BY d ORDER BY d"),
]

jc = bigquery.QueryJobConfig(use_query_cache=False)
for name, sql in queries:
    t0 = time.time()
    job = client.query(sql, job_config=jc)
    results = list(job.result())
    wall_ms = int((time.time() - t0) * 1000)
    bq_ms = int((job.ended - job.created).total_seconds() * 1000)
    print(f"{name}: wall={wall_ms}ms  bq_job={bq_ms}ms  rows={len(results)}")
