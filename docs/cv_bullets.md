# CV Bullets — vn-food-delivery-pipeline

All numbers are directly measured from the running pipeline (2026-06-05 benchmark session).
Source column references the task that produced the number.

---

## One-liner (for headline)

**Built a real-time food delivery analytics pipeline processing 1,200+ orders/min end-to-end from Kafka through ClickHouse to Grafana, with P50=3.6s hot-path latency and 55/55 dbt tests passing.**

---

## Detailed Bullets (pick and mix for job descriptions)

### Data Engineering / Real-Time Systems

- Designed and implemented a **real-time event streaming pipeline** (GrabFood/ShopeeFood scale) using Kafka 3.6, PySpark 3.5 Structured Streaming, ClickHouse 24.x, and Airflow 2.8 on Docker Compose
- Achieved **1,244 orders/min** sustained ingestion rate on single-node Docker (benchmarked via 3 × 1-min count delta); generator configured to 5,000/min peak — pipeline throughput at that load not yet benchmarked
- Measured **end-to-end hot-path latency P50=3.59s, P95=6.62s** (producer→Kafka→ClickHouse Kafka Engine→ReplacingMergeTree) using epoch-timestamp instrumentation
- Implemented **dual ingestion paths**: Kafka Engine (real-time, <7s, approximate) + Spark→MinIO (cold, exact dedup, ~40s MinIO write), exposing the streaming vs batch trade-off in a Lambda architecture

### ClickHouse / OLAP

- Configured ClickHouse **ReplacingMergeTree** with Kafka Engine (3-object pattern: queue table + MV + storage) for sub-10s real-time ingestion; zero-downtime dedup on merge
- Benchmarked **concurrent query performance**: P50=67ms at N=1, P50=93ms at N=10 concurrent analytical queries (GROUP BY city+hour, 7-day window, ~36k rows, with ORDER BY) — 1.4× degradation at 10× load; separate measurement on 261k rows without ORDER BY yields 19ms (warm parts, no sort overhead)
- Achieved **2.4× LZ4 compression** on raw_orders (MiB→MiB), **1.9× on rider events** — validated via `system.parts`
- Optimized ClickHouse schema: `LowCardinality(String)` for enum fields, `Float64` for sub-second timestamps, `parseDateTime64BestEffort` for ISO 8601 ingestion

### dbt / Data Transformation

- Built **10 dbt models across 3 layers** (staging → intermediate → marts) targeting ClickHouse via dbt-clickhouse 1.7.7; 55/55 data quality tests passing
- Migrated `fct_orders` from full-table to **incremental materialization** (`delete+insert` strategy, 10-min lookback window), reducing dbt internal run time from full refresh to **1.25s** per incremental cycle
- Enforced dbt layering rules: staging = rename+cast only, intermediate = joins+logic, marts = analytics-ready; custom SQL tests assert business invariants (non-negative revenue, valid order status)

### Reliability / Observability

- Demonstrated **Kafka broker restart recovery in ~40s** with zero message loss under graceful SIGTERM shutdown; Spark resumes from checkpoint offset — hard-crash (SIGKILL) scenario not tested, RF=1 would risk in-flight loss
- Instrumented **7/7 Prometheus targets** (Kafka exporter, ClickHouse exporter, node exporter, 3× Spark Prometheus endpoints, Prometheus self-scrape) with Grafana dashboards; `raw.payments` consumer lag structurally ~52 msgs by design (payments trail orders by 10–180s)
- Added **Data Freshness SLA panel** in Grafana: ClickHouse query measures seconds since last order, with green/yellow/red thresholds at 60s/300s
- Configured **Airflow email failure alerting** across all 3 DAGs (dbt_run, monitor_kafka_lag, batch_daily_summary) via SMTP

### Spark Streaming

- Instrumented `foreachBatch` with per-epoch timing: **95 rows/sec average MinIO write throughput** on large catch-up batches (3,400–5,600 rows); steady-state 8–12s total batch time
- Applied Spark Structured Streaming best practices: watermark + dropDuplicates (bounded state), `failOnDataLoss=false`, `startingOffsets=earliest`, checkpoint for exactly-once MinIO writes
- Documented and root-caused S3A overhead (8–18s latency on dev): per-file HTTP multipart commit overhead, not a Spark limitation; production mitigation strategies documented in `IMPLEMENTATION_NOTES.md`

---

## Numbers Quick Reference

| Metric | Value | Source |
|--------|-------|--------|
| Hot path E2E latency P50 | 3.59s | P2-T06 (5 runs × 500 samples) |
| Hot path E2E latency P95 | 6.62s | P2-T06 |
| Orders ingested/min (sustained) | 1,244 | P1-T02 (3 × 1-min delta) |
| Total rows/sec (all 3 topics, ClickHouse) | 48.4 | P1-T02 |
| ClickHouse query P50 (N=1) | 67ms | P3-T07 |
| ClickHouse query P50 (N=10) | 93ms | P3-T07 |
| ClickHouse query P95 (N=10) | 116ms | P3-T07 |
| LZ4 compression raw_orders | 2.4× | P1-T01 |
| dbt run (10 models, internal) | 1.96s | P1-T03 |
| dbt test (55 tests, internal) | 3.56s | P1-T03 |
| dbt incremental fct_orders | 1.25s | P6-T12 |
| Kafka failure recovery | ~40s | P3-T08 |
| Messages lost on broker kill | 0 | P3-T08 |
| Spark foreachBatch avg write | 39.6s (large batches) | P3-T09 |
| MinIO throughput (large batches) | 95 rows/sec | P3-T09 |
| Prometheus targets | 7/7 UP | prior session |
| dbt tests | 55/55 PASS | P1-T03 |
