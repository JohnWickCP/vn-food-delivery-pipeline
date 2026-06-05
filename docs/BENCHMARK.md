# Benchmark Results — vn-food-delivery-pipeline

Measured on: **2026-06-05** | Environment: single-node Docker Compose, Windows 10 WSL2 backend

---

## Throughput

| Metric | Value |
|--------|-------|
| Orders ingested/min (sustained) | **1,244 orders/min** |
| Payments/min | 1,243/min |
| Rider GPS events/min | 400/min (200 riders × 30s) |
| Total rows/sec (all 3 topics → ClickHouse) | **48.4 rows/sec** |
| Peak generator config | 5,000 orders/min (not benchmarked at full load) |

---

## Hot Path Latency (Producer → Kafka → ClickHouse)

| Run | P50 (s) | P95 (s) | Samples |
|-----|---------|---------|---------|
| 1 | 3.24 | 6.54 | 500 |
| 2 | 4.58 | 7.05 | 500 |
| 3 | 3.78 | 6.81 | 500 |
| 4 | 3.37 | 6.62 | 500 |
| 5 | 2.99 | 6.10 | 500 |
| **Avg** | **3.59s** | **6.62s** | |

> `_ingested_at` has 1s precision (DateTime) → ±1s rounding on P50.

---

## ClickHouse Query Latency

### Analytical queries on raw_orders (261k rows, ReplacingMergeTree)

| Query | Description | Elapsed |
|-------|-------------|---------|
| Q1 | city + hour GROUP BY, 7-day window | **19ms** |
| Q2 | city + payment_method GROUP BY, 30-day window | **14ms** |
| Q3 | city + district GROUP BY, 30-day window | **19ms** |
| Q4 | `SELECT count()` | **3ms** |

### Concurrent query benchmark (~36k rows, N=1/5/10 threads)

| N concurrent | P50 (ms) | P95 (ms) | Max (ms) |
|-------------|----------|----------|---------|
| 1 | 67 | 249 | 249 |
| 5 | 82 | 292 | 292 |
| 10 | **93** | **116** | 118 |

1.4× P50 degradation at 10× concurrency — still under 120ms P95.

### FINAL vs non-FINAL overhead (~490k rows)

| Variant | Cold | Warm |
|---------|------|------|
| Without FINAL | 42ms | 22ms |
| With FINAL | 359ms | 152ms |
| Overhead | **8.5×** | **6.9×** |

> Both return identical row counts → no significant duplicates in dataset.

---

## ClickHouse Compression (LZ4, ReplacingMergeTree)

| Table | Compressed | Uncompressed | Ratio |
|-------|-----------|--------------|-------|
| raw_orders | 45 MiB | 110 MiB | **2.4×** |
| raw_payments | 13.7 MiB | 17.5 MiB | **1.3×** |
| raw_rider_events | 1.4 MiB | 3.1 MiB | **2.2×** |

---

## Cold Path — Spark → MinIO

| Metric | Value |
|--------|-------|
| Trigger interval (configured) | 500ms |
| Actual batch duration (steady-state) | **8–12s avg** (S3A overhead, single-node MinIO) |
| Spark write throughput to MinIO | **95 rows/sec** (large catch-up batches) |
| MinIO storage rate | ~600 MB/h → ~14 GB/day |
| Storage after 16min run | 161 MiB, 77,735 Parquet objects |

---

## dbt

| Metric | Value |
|--------|-------|
| Models | 10 (staging → intermediate → marts) |
| Test coverage | **55/55 PASS** (unique, not_null, accepted_values, 3 custom SQL) |
| dbt run time (internal SQL) | **1.96s** (10 models) |
| dbt test time (internal SQL) | **3.56s** (55 tests) |
| fct_orders incremental run | **1.25s** (10-min window) |
| Wall time (Python startup overhead) | ~9–11s per step |

---

## Kafka Failure Recovery

### Graceful stop (SIGTERM / docker stop)

| Metric | Value |
|--------|-------|
| Kafka downtime | 10s |
| Spark recovery | **~40s** |
| Messages lost | **0** |

### Hard kill (SIGKILL / docker kill)

| Metric | Value |
|--------|-------|
| Kafka downtime | **~77s** |
| Spark detection latency | **<1s** |
| Spark first batch after recovery | **~116s** after kill |
| First catch-up batch size | 122,349 rows |
| Messages lost | **0** (OS page cache survives SIGKILL on same host; real hardware crash with RF=1 would risk loss) |

---

## Observability

| Metric | Value |
|--------|-------|
| Prometheus scrape targets | **7/7 UP** |
| Grafana dashboards | 2 (Business + Infrastructure) |
| Airflow DAGs | 3 (dbt_run, monitor_kafka_lag, batch_daily_summary) |

---

## Summary for CV

| Claim | Measured |
|-------|---------|
| Real-time ingestion | P50 **3.6s**, P95 **6.6s** end-to-end (producer → ClickHouse) |
| Analytical query speed | **3–19ms** on 261k rows; **93ms P50 at N=10 concurrent** |
| Compression | **2.4× LZ4** on raw_orders |
| dbt test coverage | **55/55 tests PASS** in 3.56s |
| dbt transformation speed | full run **1.96s**, incremental **1.25s** |
| Kafka resilience | **0 message loss** on SIGTERM + SIGKILL (single-node Docker) |
| Monitoring | **7/7** Prometheus targets UP |
| Throughput | **1,244 orders/min** sustained on single-node Docker |
