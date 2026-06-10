# Measured Pipeline Metrics — vn-food-delivery-pipeline

Last measured: **2026-06-05** (benchmark session) | Stack: Docker Compose, single-node dev (Windows 10, WSL2 backend)

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

**2026-06-05 benchmark session** — measured by table count delta over 3 × 1-min intervals:

| Metric | Value |
|--------|-------|
| Orders/min (avg over 3 runs) | **1,244/min** |
| Payments/min (avg) | **1,243/min** |
| Rider events/min | **400/min** (200 riders × 2 events) |
| Total rows/sec (all 3 tables combined) | **48.4 rows/sec** |

Prior session estimates (2026-05-16):

| Scenario | Orders/min (estimated) | Total msg/min (all topics) |
|----------|------------------------|---------------------------|
| Off-peak | ~980 | ~2,364 |
| Mid-peak | ~1,114 (session avg) | ~2,680 |
| Peak burst | ~1,990 | ~4,380 |

> **CV claim:** "1,244 orders/min sustained on single-node Docker (benchmarked). Generator configured to 5,000/min peak — pipeline not benchmarked at that load."

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

### 3a. FINAL vs non-FINAL overhead benchmark (2026-06-05)

Dataset: **489,933 rows** in `raw_orders` (ReplacingMergeTree, un-merged duplicates present).

Query: `SELECT city, count() FROM food_delivery.raw_orders [FINAL] GROUP BY city`

| Variant | Run 1 (cold) | Run 2 (warm) | Notes |
|---------|-------------|-------------|-------|
| Without FINAL | 42 ms | 22 ms | Reads raw parts, may count duplicates |
| With FINAL | 359 ms | 152 ms | Forces merge-on-read dedup |
| **Overhead** | **8.5×** | **6.9×** | FINAL forces single-threaded part merge |

**Both queries return identical row counts** (163,009 / 163,541 / 163,080 per city) — confirms no significant duplicate volume in current dataset.

> **FINAL trade-off:** Use FINAL only in batch/dbt queries where exact dedup matters. Hot-path Grafana dashboards skip FINAL for speed; ReplacingMergeTree handles eventual dedup during background merges. dbt `fct_orders` uses `FINAL` explicitly in the staging SELECT to guarantee dedup before mart aggregation.

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
| spark (orders) | 4040 | Spark driver PrometheusServlet (spark-streaming-orders:4040) |
| spark (payments) | 4040 | Spark driver PrometheusServlet (spark-streaming-payments:4040) |
| spark (riders) | 4040 | Spark driver PrometheusServlet (spark-streaming-rider-events:4040) |

---

## 8. Spark Streaming Latency (dev environment)

**2026-06-05 benchmark session:**

| Metric | Value |
|--------|-------|
| Trigger interval (configured) | 500ms |
| Actual batch duration (steady-state, from WARN logs) | **8–12s avg** (min 8.1s, max 43.6s) |
| foreachBatch write time (catch-up batches, 3,341–5,592 rows) | **34–47s avg** |
| foreachBatch rows/sec to MinIO | **95 rows/sec** (large batches) |
| Root cause | S3A per-commit overhead, 3 concurrent jobs, single-node MinIO |

> **CV qualifier:** "500ms trigger configured; dev S3A overhead 8–12s actual. Production with dedicated object storage: sub-second."

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

## 10. Hot Path E2E Latency (2026-06-05)

Measured using `producer_ts` field (Unix epoch at message creation) vs `_ingested_at` (DateTime, 1s precision):

| Run | P50 (s) | P95 (s) | Samples |
|-----|---------|---------|---------|
| 1 | 3.24 | 6.54 | 500 |
| 2 | 4.58 | 7.05 | 500 |
| 3 | 3.78 | 6.81 | 500 |
| 4 | 3.37 | 6.62 | 500 |
| 5 | 2.99 | 6.10 | 500 |
| **Final** | **3.59s** | **6.62s** | |

Path: Python generator → Kafka `raw.orders` → ClickHouse Kafka Engine → `raw_orders` table.

> Note: `_ingested_at = DateTime` (1s precision) introduces ±1s rounding; P50 is conservative.

---

## 11. Concurrent Query Benchmark (2026-06-05)

Query: `SELECT city, toStartOfHour, count(), sum(total_vnd) GROUP BY 1,2 WHERE 7d window` on ~36k rows:

| N concurrent | P50 (ms) | P95 (ms) | Max (ms) |
|-------------|----------|----------|---------|
| 1 | 67 | 249 | 249 |
| 5 | 82 | 292 | 292 |
| 10 | **93** | **116** | 118 |

ClickHouse handles 10 concurrent analytical queries at P50=93ms, P95=116ms. 1.4× P50 degradation at 10× concurrency.

> **Note on dataset difference:** Section 3 shows 19ms on 261k rows (simple GROUP BY, no ORDER BY, warm merged parts). This benchmark shows 67ms on 36k rows (includes `ORDER BY h DESC` + measured under concurrent load). 67ms > 19ms despite fewer rows because: (1) ORDER BY adds sort step, (2) different cache state, (3) concurrent execution. Not an apples-to-apples comparison — report separately.

---

## 12. Kafka Broker Failure Recovery (2026-06-05)

### 12a. Graceful shutdown (SIGTERM / docker stop)

| Metric | Value |
|--------|-------|
| Downtime | 10s (docker stop → graceful SIGTERM) |
| Spark recovery time | **~40s** after Kafka restart |
| Messages lost | **0** |
| Checkpoint honored | Yes — Spark resumed from last committed offset |

### 12b. Hard kill (SIGKILL / docker kill) — 2026-06-05

| Metric | Value |
|--------|-------|
| Kafka downtime | **~77s** (kill 20:30:05 → healthy 20:31:22) |
| Spark detection latency | **<1s** — `NetworkClient` WARN within 1s of kill |
| Spark crash + Docker restart | **86s** after kill (exit code 137 → restart policy) |
| Spark first batch start | **~116s** after kill (~34s after Kafka healthy) |
| First catch-up batch | **122,349 rows** (epoch=0; all backlog since last checkpoint) |
| Messages lost from Kafka log | **0** |
| ClickHouse Kafka Engine recovery | **~immediate** — consumer group reconnected automatically |
| Checkpoint honored | Yes — Spark resumed from last committed offset |
| `failOnDataLoss=false` behavior | Job retried NetworkClient ~60s then crashed; Docker restart policy auto-recovered |

**Why 0 message loss on SIGKILL:** Docker SIGKILL kills the process but does not discard the OS page cache. Kafka writes go to the Linux page cache first; those writes persist after the process is killed and are recovered on restart. This differs from a real hardware failure (power-off, kernel panic) where the page cache IS lost. With RF=1 and a real server crash, any unfsynced messages (O(10–100ms) of writes = ~0.2–2 messages at 1,244/min) would be permanently lost.

**Mitigation for production:** RF≥2 + `min.insync.replicas=2` + `acks=all` on producer — survives single-broker hard crash with zero loss.

---

## 13. dbt Pipeline Timing (2026-06-05)

| Step | dbt internal | wall time |
|------|-------------|-----------|
| dbt run (10 models) | 1.96s | 9.70s |
| dbt test (55 tests) | 3.56s | 10.68s |
| fct_orders incremental run | **1.25s** | 9.27s |

Wall time dominated by Python interpreter startup (~8s). dbt internal SQL time is 2–4s total.

---

---

## 14. GCP Migration Benchmarks (2026-06-11)

Stack: GCE VM `e2-standard-2` (2 vCPU, 8 GB), `asia-southeast1-b` · Pub/Sub · GCS `asia-southeast1` · BigQuery · Airflow + Spark on Docker Compose

### 14a. BigQuery Query Latency

Dataset: `fct_orders` (603,275 rows, partitioned by `placed_date`, clustered by `city`).  
Measured via Python SDK with `use_query_cache=False` from GCE VM (same region).

| Query | Description | wall_ms | bq_job_ms |
|-------|-------------|---------|-----------|
| Q1 | `COUNT(*)` on fct_orders (603K rows) | 775 | 196 |
| Q2 | City revenue GROUP BY (603K rows) | 449 | 170 |
| Q3 | Hourly pattern HCMC filtered + GROUP BY | 484 | 207 |
| Q4 | Joined fct + rpt (top-N) | 695 | 408 |
| Cold (first query of session) | Any query, no slot warm-up | ~4,300 | ~3,900 |

BQ slot warm-up adds ~4s on the first query per session (cold slot). After warm-up, P50 bq_job = **170–408ms** on 600K rows.

> **vs ClickHouse local:** BQ 170–408ms bq_job vs ClickHouse 14–19ms. BQ is 10–20× slower on analytical queries but fully managed, no ops. ClickHouse wins on raw speed; BQ wins on scale, governance, and zero maintenance.

### 14b. BigQuery Raw Table Sizes (after load pipeline)

| Table | Rows | Notes |
|-------|------|-------|
| food_delivery_raw.raw_orders | 602,000 | WRITE_APPEND, dedup at dbt layer |
| food_delivery_raw.raw_payments | 603,000 | WRITE_APPEND, dedup at dbt layer |
| food_delivery_raw.raw_rider_events | 236,000 | WRITE_APPEND, dedup at dbt layer |

### 14c. dbt Run on BigQuery

| Step | Duration | Notes |
|------|----------|-------|
| dbt deps (first run, downloads packages) | 25s | dbt-utils download |
| dbt run — 6 views (stg_*, int_*) | 1.5–2.9s each | BQ view creation overhead |
| dbt run — fct_orders incremental (603K rows, 181 MiB) | 7.92s | merge strategy, 1.6 GB scanned |
| dbt run — dim_rider (400 rows) | 4.44s | |
| dbt run — dim_restaurant (3K rows) | 2.68s | |
| dbt run — rpt_hourly_revenue (30 rows) | 3.12s | |
| **dbt run total (10 models)** | **17.14s** | vs 1.96s ClickHouse local |
| dbt test (55 tests) | ~15s | 55/55 PASS after staging QUALIFY dedup |

BQ has ~2–4s overhead per model vs ClickHouse due to slot scheduling. At scale (100s of models), difference amortizes.

### 14d. Pub/Sub Throughput

| Topic | Sustained rate | Measured interval |
|-------|---------------|-------------------|
| raw-orders | ~1,200 msgs/min | 5-min window |
| raw-payments | ~1,200 msgs/min | 5-min window |
| raw-rider-events | ~400 msgs/min | 5-min window |

Pub/Sub subscriber pulls MAX_MESSAGES=1000 every 2s, writes JSONL to GCS. Files bucketed by UTC write date (not event date) — see WRITE_APPEND design note.

### 14e. GCS Write Latency (Spark streaming → GCS)

| Scenario | Latency |
|----------|---------|
| Pub/Sub subscriber JSONL batch write | <2s per batch |
| Spark streaming batch (300s trigger) | 3–13s per batch write to GCS Parquet |
| Cold path (first batch after restart) | up to 30s (GCS auth + credential fetch) |

Spark GCS latency is 3–13s due to GCS consistency guarantees + per-part-file rename overhead. Comparable to MinIO S3A on local Docker (8–12s).

### 14f. Airflow Load DAG Performance

| DAG run | Date | Duration | Rows loaded |
|---------|------|----------|-------------|
| load_pubsub_to_bigquery | day=2026-06-07 | ~15s | ~0 (cold, few files) |
| load_pubsub_to_bigquery | day=2026-06-10 | ~74s | 602K orders + 603K payments + 236K riders |

Load time dominated by BQ slot wait (first topic loaded is slowest; parallel topics contend for same slot pool on free tier).

### 14g. GCP Monitoring

| Service | Endpoint | Status |
|---------|----------|--------|
| Grafana | `http://34.21.213.106:3000` | HTTP 200 ✅ |
| Prometheus | `http://34.21.213.106:9090` | HTTP 200 ✅ |
| Prometheus targets | node-exporter + spark UIs | 3 targets scraped ✅ |

---

## Summary for CV

### Local Docker (2026-06-05)

| Metric | CV Claim | Actual |
|--------|----------|--------|
| Kafka throughput | 1,244 orders/min sustained | **1,244 orders/min** benchmarked; 5k+/min = generator config |
| Hot path E2E latency | seconds | **P50=3.6s, P95=6.6s** (producer → ClickHouse) |
| ClickHouse query latency | <100ms | **67ms P50 at N=1, 93ms P50 at N=10 concurrent** |
| ClickHouse compression | 2× | **2.4× raw_orders** |
| dbt test coverage | 100% (55 tests) | **55/55 PASS in 3.56s dbt-internal** |
| dbt run | fast refresh | **full 1.96s, incremental 1.25s** (dbt-internal) |
| Kafka recovery (SIGKILL) | resilient | **116s to first batch, 0 msgs lost** |
| Prometheus targets | 7/7 | **7/7 UP** |

### GCP (2026-06-11)

| Metric | Value | Notes |
|--------|-------|-------|
| BigQuery query latency (warm) | **P50=170–408ms bq_job** | 600K rows, no cache, same-region VM |
| BigQuery cold slot warm-up | ~4.3s | First query per session |
| dbt run (10 models, BQ) | **17.14s wall** | vs 1.96s local — BQ slot overhead |
| dbt tests | **55/55 PASS** | QUALIFY dedup at staging layer |
| Pub/Sub throughput | **~1,200 msgs/min** per topic | Consistent with local Kafka rate |
| Airflow load DAG | **15–74s** per day | Depends on file count; BQ slot contention on free tier |
| GCS Spark write latency | **3–13s** per batch | Comparable to MinIO S3A |
| BQ raw table volume | **1.44M rows loaded** | 602K orders + 603K payments + 236K riders |
| Monitoring | **Grafana + Prometheus UP** | `http://34.21.213.106:3000` + `:9090` |
