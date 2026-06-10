import time
from google.cloud import bigquery

client = bigquery.Client(project="project-739a3554-aa69-4eab-9e2")
P = "project-739a3554-aa69-4eab-9e2"

queries = [
    ("Q1_fct_count", f"SELECT COUNT(*) FROM `{P}.food_delivery_dbt.fct_orders`"),
    ("Q2_city_revenue", f"SELECT city, COUNT(*) as orders, SUM(total_vnd)/1e6 as rev_m FROM `{P}.food_delivery_dbt.fct_orders` GROUP BY city ORDER BY rev_m DESC"),
    ("Q3_join_fct_rpt", f"SELECT f.placed_date, COUNT(*) as orders, r.total_revenue_vnd FROM `{P}.food_delivery_dbt.fct_orders` f JOIN `{P}.food_delivery_dbt.rpt_hourly_revenue` r ON f.placed_date = r.placed_date GROUP BY f.placed_date, r.total_revenue_vnd LIMIT 10"),
    ("Q4_hourly_pattern", f"SELECT placed_hour, AVG(total_vnd) as avg_order, COUNT(*) as orders FROM `{P}.food_delivery_dbt.fct_orders` WHERE city = 'hcmc' GROUP BY placed_hour ORDER BY placed_hour"),
    ("Q5_rpt_last24h", f"SELECT placed_date, hour_bucket, total_revenue_vnd, total_orders FROM `{P}.food_delivery_dbt.rpt_hourly_revenue` ORDER BY placed_date DESC, hour_bucket DESC"),
]

jc = bigquery.QueryJobConfig(use_query_cache=False)
for name, sql in queries:
    t0 = time.time()
    job = client.query(sql, job_config=jc)
    results = list(job.result())
    wall_ms = int((time.time() - t0) * 1000)
    bq_ms = int((job.ended - job.created).total_seconds() * 1000)
    print(f"{name}: wall={wall_ms}ms  bq_job={bq_ms}ms  rows={len(results)}")
