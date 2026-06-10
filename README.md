# Vietnam Food Delivery Pipeline — GCP Stack

> **Branch:** `main` — GCP-hosted pipeline (Pub/Sub · GCS · BigQuery · Spark on GCE).
> Requires GCP credentials. No local Kafka or MinIO.
>
> Want to run everything locally with no cloud account? → [`docker-local` branch](../../tree/docker-local)

---

## What this project does

End-to-end real-time food delivery analytics pipeline on GCP, modeled after GrabFood/ShopeeFood architecture. Processes ~1,200–5,000 orders/min with Lambda Architecture: hot path (Pub/Sub → BigQuery streaming) and cold path (GCS → Spark → BigQuery → dbt).

The generator produces realistic Vietnamese food delivery data: orders peaking at 11–13h and 18–20h, 200 riders sending GPS pings every 30s, payments processed 10–180s after placement.

---

## Architecture

```
┌────────────────────────────────────────────────────────────────────┐
│  DATA GENERATION                                                    │
│  Python · asyncio · Pydantic v2                                    │
│  ~1,200–5,000 orders/min  ·  200 riders × GPS every 30s           │
└───────────────────────────┬────────────────────────────────────────┘
                            │
                            ▼
┌────────────────────────────────────────────────────────────────────┐
│  Google Pub/Sub                                                     │
│  raw-orders · raw-payments · raw-rider-events                      │
│  Pull subscriptions · 7-day retention                              │
└───────────────────────────┬────────────────────────────────────────┘
                            │
                            ▼
┌────────────────────────────────────────────────────────────────────┐
│  Pub/Sub → GCS Bridge  (pubsub-subscriber Docker service)          │
│  Pulls messages every 2s · writes JSONL to GCS                    │
│  gs://vn-food-delivery-lake-739a3554/pubsub-raw/<topic>/           │
└───────────────────────────┬────────────────────────────────────────┘
                            │
                 ┌──────────┴──────────┐
                 │                     │
                 ▼                     ▼
┌──────────────────────┐  ┌──────────────────────────────────────────┐
│  HOT PATH            │  │  COLD PATH                                │
│                      │  │                                           │
│  [Phase 4]           │  │  PySpark 3.5 Structured Streaming        │
│  Pub/Sub →           │  │  3 jobs · trigger(availableNow=True)     │
│  BigQuery            │  │  every 5min loop · watermark 10min       │
│  subscription        │  │  localCheckpoint(eager=True)             │
│  (native GCP         │  │  coalesce(1) per output partition        │
│  managed ingest,     │  │             │                             │
│  no code required)   │  │             ▼                             │
│                      │  │  GCS  gs://bucket/raw/*/                 │
│                      │  │  Parquet · partitioned year/month/day    │
│                      │  │             │                             │
│                      │  │  spark-compact-daily (every 24h)         │
│                      │  │  merges small files → 1 file/partition   │
└──────────────────────┘  └──────────────┬─────────────────────────-─┘
                                         │
                                         ▼
┌────────────────────────────────────────────────────────────────────┐
│  BigQuery                                                           │
│  food_delivery_raw  — raw tables (Parquet load from GCS)           │
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
│  Grafana (GCE)  +  Looker Studio                                    │
│  Grafana: infra metrics · Prometheus scrape targets                │
│  Looker Studio: business dashboard · public share link             │
└────────────────────────────────────────────────────────────────────┘
```

---

## Migration Status

| Phase | What | Status |
|-------|------|--------|
| Phase 0 — GCP Foundation | Project, APIs, ADC, billing alerts | ✅ Done |
| Phase 1 — GCS | Replace MinIO with GCS, update Spark | ✅ Done |
| Phase 2 — Pub/Sub | Replace Kafka with Pub/Sub, subscriber bridge | ✅ Done |
| Phase 3 — GCE VM | Spark + Airflow self-hosted on VM | ✅ Done |
| Phase 4 — BigQuery + dbt | BQ datasets, dbt-bigquery port, 55 tests | ✅ Done |
| Phase 5 — Monitoring | Grafana + Prometheus on GCE VM | ✅ Done |
| Phase 6 — Benchmark | BQ latency, dbt timing, throughput measured | ✅ Done |

---

## Benchmarks

### GCP — Spark Cold Path (Phase 1–2)

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Spark write latency | ~1,100s/batch | ~16–19s/batch | **57×** |
| GCS `_temporary/` dirs per batch | 200 | 4 | **50×** |
| Daily Parquet files per table | ~23,000 | ~3 | **7,600×** |

**Root cause:** `cache()` in `foreachBatch` does not prevent GCS double-scan — `write()` creates a new `QueryExecution` from the original logical plan, so GCS is scanned twice. `localCheckpoint(eager=True)` materializes to executor-local disk and replaces the logical plan entirely. All downstream actions read from local disk, never touching GCS again.

`spark.sql.shuffle.partitions=200` (default) → 200 shuffle tasks → 200 GCS `_temporary/` dirs → commit and cleanup took 20–30 min/batch. Reduced to 4.

### Local Docker Phase (historical, `docker-local` branch)

| Metric | Value |
|--------|-------|
| Kafka throughput | **1,244 orders/min** sustained (single-node Docker, benchmarked 2026-06-05) |
| ClickHouse ingestion latency | **P50 = 3.6s, P95 = 6.6s** (producer → ClickHouse) |
| ClickHouse query latency | **3–19ms** on 261k rows · **P50 = 67ms, P95 = 116ms** at N=10 concurrent |
| dbt tests | **55/55 pass** |
| Prometheus targets | **7/7 up** |
| Spark → MinIO throughput | **95 rows/sec** · ~14 GB/day |

### GCP Phase 3–6 (measured 2026-06-11, GCE `e2-standard-2`, `asia-southeast1-b`)

| Metric | Value | Notes |
|--------|-------|-------|
| BigQuery query latency (warm) | **170–408ms** bq_job | 600K rows, no cache, same-region VM |
| BigQuery cold slot warm-up | ~4.3s | First query per session |
| dbt run — 10 models | **17.1s** wall | vs 1.96s local (BQ slot scheduling overhead) |
| dbt tests | **55/55 PASS** | QUALIFY dedup at staging layer |
| Pub/Sub throughput | **~1,200 msgs/min** per topic | Consistent with local Kafka benchmark |
| GCS Spark batch write | **3–13s** per batch | 300s trigger loop; equivalent to local S3A |
| Airflow load DAG | **15–74s** per day | File count and BQ slot contention dependent |
| BQ raw tables loaded | **1.44M rows** | 602K orders · 603K payments · 236K riders |
| Monitoring | **Grafana + Prometheus UP** | `http://34.21.213.106:3000` · `:9090` |

Full benchmark numbers: [`docs/metrics_results.md`](docs/metrics_results.md) section 14.

> **BigQuery vs ClickHouse:** BQ warm query latency is 10–20× slower (170–408ms vs 14–19ms) on the same dataset. Trade-off: zero ops, serverless scaling, native GCP integration vs. ClickHouse's sub-20ms OLAP performance on dedicated hardware.

---

## Tech Stack

| Component | Local Phase (`docker-local`) | GCP Phase (`main`) |
|-----------|------------------------------|---------------------|
| Message broker | Apache Kafka 3.5 | Google Pub/Sub |
| Object storage | MinIO (S3-compatible) | Google Cloud Storage |
| Stream processing | PySpark 3.5 | PySpark 3.5 (on GCE VM) |
| Data warehouse | ClickHouse 24.x | BigQuery |
| Transformation | dbt-core 1.7 (dbt-clickhouse) | dbt-core 1.7 (dbt-bigquery) |
| Orchestration | Airflow 2.8 (Docker) | Airflow 2.8 (self-hosted on GCE) |
| Monitoring | Grafana 10 + Prometheus | Grafana 10 + Looker Studio |
| Infrastructure | Docker Compose (local, ~8GB RAM) | GCE e2-standard-2 · asia-southeast1 |

**GCP resources used:**
- GCE: `e2-standard-2` · `ubuntu-22.04-lts` · `asia-southeast1-b`
- GCS bucket: `gs://vn-food-delivery-lake-739a3554` · region `asia-southeast1`
- Pub/Sub: 3 topics + 3 pull subscriptions
- BigQuery: `food_delivery_raw` + `food_delivery_dbt` datasets

**Estimated cost:** ~$50–55/month (GCE $48 + GCS/Pub/Sub/BQ ~$2–5) after $300 free credit.

---

## Quick Start (GCP)

### Prerequisites

```bash
# 1. GCP project with APIs enabled: Compute Engine, Pub/Sub, Cloud Storage, BigQuery, IAM
# 2. Application Default Credentials
gcloud auth application-default login

# 3. Pub/Sub topics + subscriptions (already created in this project)
#    raw-orders, raw-payments, raw-rider-events

# 4. GCS bucket: gs://vn-food-delivery-lake-739a3554
```

### Run locally (hybrid mode — Docker local, GCP as backend)

```bash
git clone https://github.com/JohnWickCP/vn-food-delivery-pipeline.git
cd vn-food-delivery-pipeline

cp .env.example .env
# Edit .env:
#   GOOGLE_ADC_PATH=/path/to/application_default_credentials.json
#   GCP_PROJECT_ID=your-project-id
#   GCS_BUCKET=vn-food-delivery-lake-739a3554

docker compose up -d

# Daily compaction (optional)
docker compose --profile compact up -d spark-compact-daily

# Manual batch run
docker compose --profile batch run --rm spark-batch-daily
docker compose --profile batch run --rm spark-batch-daily --date 2026-06-11
```

**Service URLs:**

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow | http://localhost:8080 | admin / admin |
| Spark Master UI | http://localhost:8081 | — |
| Grafana | http://localhost:3000 | admin / admin |
| Prometheus | http://localhost:9090 | — |

---

## Project Structure

```
vn-food-delivery-pipeline/
├── generator/                   # Async Pub/Sub producers (orders, payments, riders)
│   ├── producers/               # OrderProducer, PaymentProducer, RiderProducer
│   ├── schemas/                 # Pydantic v2 models
│   └── main.py
├── pubsub_subscriber/           # Pulls Pub/Sub → writes JSONL to GCS
├── spark/
│   ├── Dockerfile               # apache/spark:3.5.0 + GCS Hadoop connector
│   ├── jobs/
│   │   ├── stream_orders.py     # foreachBatch · localCheckpoint · availableNow
│   │   ├── stream_payments.py
│   │   ├── stream_rider_events.py
│   │   ├── batch_daily_summary.py
│   │   └── compact_parquet.py   # Daily small-file compaction
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

### Spark on GCE instead of Dataflow
Dataflow uses Apache Beam API — migrating from PySpark would be a full rewrite (PCollection + Transform vs. DataFrame, different windowing model). GCE VM keeps the existing PySpark code unchanged. Cost: GCE `e2-standard-2` = $48/month flat vs. Dataflow Streaming Engine pay-per-vCPU-hour running 24/7 = $80–120/month.

### Airflow self-hosted instead of Cloud Composer
Cloud Composer minimum ~$400/month (managed GKE cluster underneath). With LocalExecutor and 3 DAGs, a single-node setup is sufficient — no need for CeleryExecutor or Kubernetes.

### Pub/Sub → GCS bridge instead of native Spark-Pub/Sub connector
The `spark-pubsub-connector` does not support Spark 3.5. Rather than downgrade Spark or rewrite jobs with a different API, a lightweight subscriber service pulls from Pub/Sub and writes JSONL files to GCS. Spark then uses `readStream.format("json")` — unchanged from the MinIO pattern.

### `trigger(availableNow=True)` + 5-minute bash loop
Cold path does not need sub-minute latency. `availableNow=True` drains all pending GCS files then exits cleanly. The container restarts on a 5-minute loop — avoids idle JVM resource consumption and produces clean separation between processing window and idle window.

### `localCheckpoint(eager=True)` vs `cache()` in `foreachBatch`
`cache()` registers an `InMemoryRelation` node but does not replace the logical plan. `write()` creates a fresh `QueryExecution` from the original plan — GCS sources are scanned again. `localCheckpoint(eager=True)` materializes to executor-local disk **and truncates the plan** with a `LocalCheckpointRDD`. Every subsequent action reads from local disk. Zero GCS re-reads. Trade-off: local disk is not fault-tolerant, but Structured Streaming retries the batch from checkpoint offset on restart.

### BigQuery subscription for hot path (Phase 4)
ClickHouse Kafka Engine is ClickHouse-specific. BigQuery has a native Pub/Sub subscription that automatically pulls messages and inserts into a BQ table — no code required. Trade-off: ~30s ingestion lag vs. <1s for Kafka Engine, but acceptable for analytics workload.

---

## Screenshots

### GCP Phase (Phase 3–6, pending)

| Screenshot | File | Status |
|-----------|------|--------|
| Pub/Sub console — message throughput, topics | `screenshots/gcp_pubsub_topics.png` | [ ] |
| GCS console — bucket structure, Parquet layout | `screenshots/gcp_gcs_bucket.png` | [ ] |
| GCE VM — instance details, uptime | `screenshots/gcp_gce_vm.png` | [ ] |
| BigQuery — datasets, table row counts | `screenshots/gcp_bigquery_tables.png` | [ ] |
| BigQuery — query result on fct_orders | `screenshots/gcp_bigquery_query.png` | [ ] |
| Airflow — DAG list + successful runs (on GCE) | `screenshots/airflow_dags_gce.png` | [ ] |
| Grafana — dashboard on GCE | `screenshots/grafana_dashboard_gce.png` | [ ] |
| Looker Studio — business dashboard | `screenshots/looker_studio_dashboard.png` | [ ] |
| GCP Billing — cost breakdown by service | `screenshots/gcp_billing_report.png` | [ ] |

### Local Phase (historical, from `docker-local` branch)

| Screenshot | File | Status |
|-----------|------|--------|
| Grafana — business dashboard | `screenshots/grafana_business_dashboard.png` | [ ] |
| Grafana — infra dashboard | `screenshots/grafana_infra_dashboard.png` | [ ] |
| Kafka UI — topics, throughput | `screenshots/kafka_ui_topics.png` | [ ] |
| MinIO — bucket structure | `screenshots/minio_bucket.png` | [ ] |
| Airflow — DAG runs | `screenshots/airflow_dags.png` | [ ] |
| Prometheus — 7/7 targets up | `screenshots/prometheus_targets.png` | [ ] |

---

## Known Limitations

1. **No true hot path on GCP** — BigQuery streaming inserts / Pub/Sub → BQ direct subscription not implemented. Current GCP architecture is cold path only: Pub/Sub → GCS JSONL (subscriber) → Airflow daily load → BQ. Ingest latency ~24h. Local `docker-local` branch has a true hot path via ClickHouse Kafka Engine (<5s P50 end-to-end).
2. **Airflow does not yet orchestrate Spark jobs** — Streaming jobs run as Docker services with bash loops; Airflow manages dbt and monitoring only.
3. **Single GCE VM** — `e2-standard-2` (2 vCPU, 8GB). Spark + Airflow + generator share one machine. Sufficient for portfolio; production separates concerns.
4. **`spark.local.dir` defaults to `/tmp`** — `localCheckpoint` writes to executor local disk. With 3 concurrent jobs on a 10GB boot disk, `/tmp` can fill. Set `spark.local.dir` to a dedicated mount.
5. **Stale-file error matching is string-based** — `"Item not found"` and `"generation is deleted"` are GCS connector error strings. If connector version changes the message format, the guard silently stops working. Monitored via `gs://bucket/metrics/batch_skips/`.
6. **Airflow LocalExecutor** — Production uses CeleryExecutor or KubernetesExecutor.
