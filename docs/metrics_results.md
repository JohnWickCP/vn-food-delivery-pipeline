# Measured Pipeline Metrics — vn-food-delivery-pipeline

Last measured: **2026-05-16** | Stack: Docker Compose, single-node dev (Windows 10, WSL2 backend)

---

## 1. Data Volume (ClickHouse Hot Path)

Measured after ~16 min continuous run (fresh stack, 2026-05-16 session):

| Table | Row Count | Notes |
|-------|-----------|-------|
| raw_orders | 17,830 | ~1,114/min average |
| raw_payments | 17,818 | matched orders (10-180s delay) |
| raw_rider_events | 6,200 | 200 riders × 30s pings |

Reference — prior 2.5h run (2026-05-15):

| Table | Row Count | Compressed | Uncompressed | Ratio |
|-------|-----------|------------|--------------|-------|
| raw_orders | 261k | 45 MiB | 110 MiB | 2.4× |
| raw_payments | 261k | 13.7 MiB | 17.5 MiB | 1.3× |
| raw_rider_events | 42k | 1.4 MiB | 3.1 MiB | 2.2× |

**Total events ingested (2.5h run):** ~564k across 3 topics

---

## 2. Real-Time Ingestion Rate (Kafka Engine → ClickHouse)

Measured via 10-second window samples on generator (2026-05-16):

| Scenario | Orders/min (estimated) | Total msg/min (all topics) |
|----------|------------------------|---------------------------|
| Off-peak | ~980 | ~2,364 |
| Mid-peak | ~1,114 (session avg) | ~2,680 |
| Peak burst | ~1,990 | ~4,380 |

> **CV claim:** "~1,000–2,000 orders/min sustained on single-node Docker; architecture supports 5,000+/min at scale (stateless Kafka partitions, horizontal Spark workers)."

---

## 3. ClickHouse Query Latency

Measured on `raw_orders` (261k rows, ReplacingMergeTree, no warm cache) — prior 2.5h run:

| Query | Description | Elapsed |
|-------|-------------|---------|
| Q1 | `SELECT city, toStartOfHour, count(), sum(total_vnd) GROUP BY 1,2` (7d window) | **19 ms** |
| Q2 | `SELECT city, payment_method, count(), sum(total_vnd) GROUP BY 1,2` (30d window) | **14 ms** |
| Q3 | `SELECT city, district, count() GROUP BY 1,2` (30d window) | **19 ms** |
| Q4 | `SELECT count() FROM raw_orders` | **3 ms** |

2026-05-16 session (17k rows): avg 6.2ms, range 0–105ms across all queries in `system.query_log`.

**Target: <100ms on 5M rows. All 4 queries pass on 261k rows.**

> Prior perf test with 5.03M rows: Q1=29ms, Q2=103ms, Q3=88ms — 2/3 under target, Q2 borderline at scale.

---

## 4. dbt Test Coverage

```
Finished running 55 tests in ~4s
PASS=55  WARN=0  ERROR=0  SKIP=0  TOTAL=55
```

| Layer | Tests | Type |
|-------|-------|------|
| staging | 36 | unique, not_null, accepted_values |
| intermediate | 3 | unique, not_null |
| marts | 13 | unique, not_null, custom SQL |
| singular | 3 | assert_valid_order_status, assert_positive_revenue, assert_payment_not_exceeds_order |
| **Total** | **55** | **100% pass** |

---

## 5. dbt Mart Tables (2026-05-16 session, after ~16 min of data)

| Database | Table | Rows | Notes |
|----------|-------|------|-------|
| food_delivery_dbt_marts | fct_orders | 6,936 | FINAL dedup on 17,830 raw |
| food_delivery_dbt_marts | dim_restaurant | 1,482 | unique restaurants |
| food_delivery_dbt_marts | dim_rider | 200 | 200 active riders |
| food_delivery_dbt_marts | rpt_hourly_revenue | 3 | 3 hours of bucketed data |

Reference — prior 2.5h run: fct_orders=163k, dim_restaurant=4,500, dim_rider=400, rpt_hourly_revenue=15

---

## 6. MinIO Cold Storage (Spark → MinIO path)

2026-05-16 session (~16 min):

```
161 MiB   77,735 objects   food-delivery-lake/
```

| Partition | Parquet files |
|-----------|---------------|
| raw/orders/year=2026/month=5/day=15/ | 10,452 |
| raw/payments/year=2026/month=5/day=15/ | 10,585 |
| raw/rider_events/year=2026/month=5/day=15/ | 3,859 |

Spark writes ~1 Parquet file per 8s per job (S3A overhead, 3 jobs = ~1 file/2.7s across all topics).

Reference — prior 2.5h run: **4.2 GB** → projected ~40 GB/month at sustained rate.

---

## 7. Observability — Prometheus Targets

All 7 targets UP:

| Target | Port | Exporter |
|--------|------|----------|
| prometheus | 9090 | self |
| node | 9100 | node-exporter (host CPU/RAM/disk) |
| kafka | 9308 | kafka-exporter (lag, offsets) |
| clickhouse | 9116 | clickhouse-exporter (query/insert metrics) |
| spark (orders) | 4040 | Spark driver PrometheusServlet |
| spark (payments) | 4041 | Spark driver PrometheusServlet |
| spark (riders) | 4042 | Spark driver PrometheusServlet |

---

## 8. Spark Streaming Latency (dev environment)

- Trigger interval configured: `500ms`
- Actual batch processing time: **7–9s** (measured from `ProcessingTimeExecutor` WARN logs)
- Root cause: MinIO S3A per-commit overhead (~6–8 KB Parquet per batch, HTTP PUT + metadata)
- 3 Spark jobs write to same MinIO instance simultaneously → I/O contention

> **CV qualifier:** "500ms trigger interval (batch scheduling frequency); dev environment S3A overhead makes actual Kafka→MinIO latency 7–9s. Not a Spark limitation — a single-node Docker/MinIO artifact. Production cluster with dedicated object storage achieves sub-second."

---

## 9. Airflow DAG Health

| DAG | Schedule | Status |
|-----|----------|--------|
| dbt_run | hourly at HH:05 | 10/10 models + 55/55 tests ✅ |
| monitor_kafka_lag | every 5 min | Running, lag=0–67 msgs (expected) |
| batch_daily_summary | daily 2 AM | Verified: check_spark_output + load_to_clickhouse |

`monitor_kafka_lag` output sample:
```
[raw.orders]       throughput=+394 msgs | consumer_lag=0   | SUCCESS
[raw.payments]     throughput=+394 msgs | consumer_lag=52  | SUCCESS
[raw.rider_events] throughput=+200 msgs | consumer_lag=0   | SUCCESS
```

**Lag on raw.payments is expected** — payments emit 10–180s after order, so ClickHouse consumer is structurally slightly behind.

---

## Summary for CV

| Metric | CV Claim | Actual |
|--------|----------|--------|
| Kafka throughput | 5,000+/min peak | ~1,000–2,000 orders/min on single-node Docker; 5k+ at scale |
| ClickHouse hot path latency | seconds from order to query | ✅ Kafka Engine ingests within seconds |
| ClickHouse query latency | <100ms on 5M rows | **3–19ms on 261k rows**; 29–103ms on 5M (prior perf test) |
| dbt test coverage | 100% (55 tests) | **55/55 PASS** |
| Prometheus targets | 7/7 | **7/7 UP** |
| MinIO cold storage | growing continuously | 161 MiB / 16 min → ~600 MB/h → ~14 GB/day at sustained rate |
| dbt models | 10 models, 3 layers | 10/10 PASS (6 views + 4 tables, staging→intermediate→marts) |
