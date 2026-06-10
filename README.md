# Vietnam Food Delivery Pipeline

End-to-end real-time food delivery analytics pipeline on GCP, modeled after GrabFood/ShopeeFood architecture. Processes ~1,200–5,000 orders/min with a Lambda Architecture: hot path (Pub/Sub → real-time) and cold path (GCS → Spark → BigQuery → dbt).

> **Migration note:** This project was originally built on a local Docker stack (Kafka · MinIO · ClickHouse · Spark). That phase is complete with all benchmarks recorded. The current phase migrates to GCP. Architecture and metrics below reflect the GCP state.

---

## Architecture

```
┌────────────────────────────────────────────────────────────────────┐
│  DATA GENERATION                                                    │
│  Python generator · asyncio · Pydantic v2                          │
│  ~1,200–5,000 orders/min  ·  200 riders × GPS every 30s           │
└───────────────────────────┬────────────────────────────────────────┘
                            │
                            ▼
┌────────────────────────────────────────────────────────────────────┐
│  Google Pub/Sub                                                     │
│  raw-orders · raw-payments · raw-rider-events                      │
└───────────────────────────┬────────────────────────────────────────┘
                            │
                            ▼
┌────────────────────────────────────────────────────────────────────┐
│  Pub/Sub → GCS Bridge  (pubsub-subscriber)                         │
│  Pulls messages, writes JSONL files to GCS                         │
│  gs://bucket/pubsub-raw/<topic>/                                   │
└───────────────────────────┬────────────────────────────────────────┘
                            │
                 ┌──────────┴──────────┐
                 │                     │
                 ▼                     ▼
┌──────────────────────┐  ┌───────────────────────────────────────────┐
│  HOT PATH            │  │  COLD PATH                                 │
│  (real-time)         │  │                                            │
│                      │  │  Spark Structured Streaming (3 jobs)       │
│  [pending]           │  │  trigger(availableNow=True) · every 5 min  │
│  BigQuery streaming  │  │  watermark(10min) + dropDuplicates         │
│  inserts or          │  │  localCheckpoint(eager=True)               │
│  Looker Studio       │  │  coalesce(1) per output partition          │
│  real-time views     │  │             │                              │
│                      │  │             ▼                              │
│                      │  │  GCS  gs://bucket/raw/*/                  │
│                      │  │  Parquet · partitioned year/month/day      │
│                      │  │             │                              │
│                      │  │  spark-compact-daily (every 24h)           │
│                      │  │  merges small files → 1 file/partition     │
└──────────────────────┘  └──────────────┬──────────────────────────-─┘
                                         │
                                         ▼
┌────────────────────────────────────────────────────────────────────┐
│  BigQuery                                                           │
│  food_delivery_raw  — external tables on GCS Parquet               │
│  food_delivery_dbt  — dbt-transformed models                       │
└───────────────────────────┬────────────────────────────────────────┘
                            │
                            ▼
┌────────────────────────────────────────────────────────────────────┐
│  dbt-bigquery  (Airflow-triggered, hourly)                         │
│  staging → intermediate → marts                                    │
│  fct_orders · dim_restaurant · dim_rider · rpt_hourly_revenue      │
└───────────────────────────┬────────────────────────────────────────┘
                            │
                            ▼
┌────────────────────────────────────────────────────────────────────┐
│  Grafana + Looker Studio                                            │
│  Business metrics · Infra dashboards · Prometheus (7 targets)      │
└────────────────────────────────────────────────────────────────────┘
```

---

## Performance

### Spark Cold Path — 57× Write Improvement

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Spark write latency | ~1,100s/batch | ~16–19s/batch | **57×** |
| GCS `_temporary/` dirs per batch | 200 | 4 | 50× |
| Daily Parquet files per table | ~23,000 | ~3 | 7,600× |

**Root cause analysis:**

`cache()` in `foreachBatch` does not prevent GCS double-scan. When `write()` is called, Spark creates a new `QueryExecution` from the original logical plan — the optimizer does not substitute `InMemoryRelation` into the write physical plan. Result: GCS is scanned twice per batch (once for `count()`, once for `write()`).

Fix: `localCheckpoint(eager=True)` materializes the full DataFrame to executor-local disk and **replaces the logical plan entirely** — all downstream actions read from the local checkpoint, never touching GCS again.

`spark.sql.shuffle.partitions=200` (default) caused `dropDuplicates` to create 200 shuffle tasks → 200 GCS `_temporary/` directories → commit and cleanup took 20–30 minutes per batch. Reduced to 4.

### Local Docker Phase (historical)

| Metric | Value |
|--------|-------|
| Kafka throughput | 1,244 orders/min sustained (benchmarked 2026-06-05, single-node Docker) |
| ClickHouse ingestion latency | P50=3.6s, P95=6.6s (producer → ClickHouse) |
| ClickHouse query latency | 3–19ms on 261k rows; P50=67ms at N=10 concurrent |
| dbt tests | 55/55 pass |
| Prometheus targets | 7/7 up |

---

## Tech Stack

| Component | Local Phase | GCP Phase |
|-----------|-------------|-----------|
| Message broker | Apache Kafka 3.5 | Google Pub/Sub |
| Object storage | MinIO (S3-compatible) | Google Cloud Storage |
| Stream processing | PySpark 3.5 Structured Streaming | PySpark 3.5 Structured Streaming |
| Data warehouse | ClickHouse 24.x | BigQuery |
| Transformation | dbt-core 1.7 (dbt-clickhouse) | dbt-core 1.7 (dbt-bigquery) |
| Orchestration | Airflow 2.8 | Airflow 2.8 (self-hosted on GCE) |
| Monitoring | Grafana 10 + Prometheus | Grafana 10 + Cloud Monitoring |
| Infrastructure | Docker Compose (local) | GCE e2-standard-2 · asia-southeast1 |

---

## Quick Start (GCP)

### Prerequisites

- GCP project with Pub/Sub topics created: `raw-orders`, `raw-payments`, `raw-rider-events`
- GCS bucket: `gs://vn-food-delivery-lake-<suffix>/`
- Application Default Credentials configured: `gcloud auth application-default login`

```bash
cp .env.example .env
# Edit .env: GOOGLE_ADC_PATH, GCP_PROJECT_ID, GCS_BUCKET

# Start full pipeline (generator + subscriber + Spark streaming + Airflow)
docker compose up -d

# Start daily compaction job
docker compose --profile compact up -d spark-compact-daily

# Run batch daily summary manually
docker compose --profile batch run --rm spark-batch-daily
docker compose --profile batch run --rm spark-batch-daily --date 2026-06-07

# Compact a specific date manually
docker compose --profile compact run --rm spark-compact-daily
```

**Service URLs:**

| Service | URL |
|---------|-----|
| Airflow | http://localhost:8080 (admin / admin) |
| Spark Master UI | http://localhost:8081 |
| Grafana | http://localhost:3000 (admin / admin) |
| Prometheus | http://localhost:9090 |

---

## Project Structure

```
vn-food-delivery-pipeline/
├── generator/                   # Async Pub/Sub producers (orders, payments, riders)
│   ├── producers/               # OrderProducer, PaymentProducer, RiderProducer
│   ├── schemas/                 # Pydantic v2 models
│   └── main.py
├── pubsub_subscriber/           # Pulls from Pub/Sub, writes JSONL to GCS
├── spark/
│   ├── Dockerfile               # apache/spark:3.5.0 + GCS Hadoop connector
│   ├── jobs/
│   │   ├── stream_orders.py     # foreachBatch · localCheckpoint · availableNow
│   │   ├── stream_payments.py
│   │   ├── stream_rider_events.py
│   │   ├── batch_daily_summary.py
│   │   └── compact_parquet.py   # Daily small-file compaction (coalesce → 1 file/partition)
│   └── conf/spark-defaults.conf
├── airflow/
│   ├── Dockerfile
│   └── dags/
│       ├── dbt_run.py           # Hourly dbt run + test
│       ├── monitor_pubsub_lag.py
│       └── batch_daily_summary.py
├── dbt/
│   └── models/
│       ├── staging/
│       ├── intermediate/
│       └── marts/               # fct_orders · dim_restaurant · dim_rider · rpt_hourly_revenue
├── monitoring/
│   ├── prometheus/
│   └── grafana/
├── docker-compose.yml
├── .env.example
└── Makefile
```

---

## Key Engineering Decisions

### `localCheckpoint(eager=True)` vs `cache()` in `foreachBatch`

`cache()` registers an `InMemoryRelation` node in the logical plan but does not replace the plan. When `foreachBatch` calls `write()`, Spark creates a fresh `QueryExecution` from the **original** logical plan — GCS sources get scanned again. `localCheckpoint(eager=True)` materializes to executor-local disk **and truncates the plan**, replacing it with a `LocalCheckpointRDD`. Every subsequent action reads from local disk. Zero GCS re-reads.

Trade-off: local disk is not fault-tolerant (executor death = data loss for that batch). Acceptable here because Structured Streaming will retry the batch from checkpoint offset on restart.

### `trigger(availableNow=True)` + 5-minute bash loop

Cold path does not need sub-minute latency. `availableNow=True` drains all pending files then exits cleanly. The container restarts on a 5-minute loop via `bash -c "while true; do spark-submit ...; sleep 300; done"`. This avoids idle JVM resource consumption between runs and produces a clean separation between "processing window" and "idle window" — easier to reason about in logs.

### Small files: `coalesce(1)` + daily compaction

Without coalescing, each batch writes `shuffle.partitions` files per output partition. At ~4 batches/min × 4 files × 1440 min = **23,000+ files/day** per table. BigQuery external table scans scale poorly with file count (per-file metadata overhead, LIST API calls). `coalesce(1)` limits new file creation; `spark-compact-daily` rewrites prior-day partitions into a single file each night.

### Skip counter persistence across runs

`nonlocal batch_skip_count` accumulates stale-file skips within a single `spark-submit` run. After `awaitTermination()`, if any skips occurred, the count is persisted to `gs://bucket/metrics/batch_skips/` as a JSON record (ts, job, skip_count). This makes cross-run skip rates queryable via BigQuery — e.g., `SELECT SUM(skip_count) FROM batch_skips WHERE DATE(ts) = CURRENT_DATE`.

---

## Known Limitations

1. **Hot path pending** — Real-time BigQuery streaming inserts (or Looker Studio live views) not yet implemented. Hot path architecture is designed but awaiting Phase 3.

2. **Airflow does not yet orchestrate Spark jobs** — Streaming jobs run as Docker services with bash loops; Airflow manages dbt and monitoring only. Wiring Spark into Airflow requires either mounting Docker socket into the Airflow container (DockerOperator) or installing Spark binary + `apache-airflow-providers-apache-spark` in the Airflow image. Tracked as next task.

3. **Single GCE VM** — `e2-standard-2` (2 vCPU, 8GB). Spark driver + 3 streaming jobs + Airflow + generator all share the same machine. Sufficient for portfolio; production would separate concerns across VMs or use managed services.

4. **`spark.local.dir` defaults to `/tmp`** — `localCheckpoint` writes to executor local disk. With 3 concurrent jobs, `/tmp` can fill on the 10GB boot disk. Mitigate by increasing boot disk or setting `spark.local.dir` to a dedicated mount.

5. **Stale-file error matching is string-based** — `"Item not found"` and `"generation is deleted"` are GCS connector error message strings. If the connector version changes the message format, the guard silently stops working. Should be monitored via the `batch_skips` GCS metric.

6. **Airflow LocalExecutor** — Production uses CeleryExecutor or KubernetesExecutor. Sufficient for single-VM demo.
