# Vietnam Food Delivery Pipeline — Local Docker Stack

> **Branch:** `docker-local` — fully self-contained, zero cloud dependency.
> Clone, run `make up`, and the entire pipeline starts locally.
>
> Looking for the GCP version (Pub/Sub · GCS · BigQuery)?  → [`main` branch](../../tree/main)

---

## What this project does

A production-style data engineering pipeline simulating a food delivery platform (GrabFood/ShopeeFood scale) in Vietnam — real-time streaming, Lambda Architecture, batch orchestration, and analytical modeling, end-to-end in Docker.

The generator produces ~1,200–5,000 orders/min with realistic patterns: peak at 11–13h and 18–20h, 200 riders sending GPS pings every 30s, payments processed 10–180s after order placement.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│  DATA GENERATION                                                      │
│  Python · asyncio · Pydantic v2                                      │
│  OrderProducer  ~1,000–5,000 orders/min (off-peak → peak)           │
│  PaymentProducer  async, linked to order state machine               │
│  RiderProducer  200 riders × GPS ping every 30s                     │
└────────────────┬─────────────────────────────────────────────────────┘
                 │
                 ▼
┌──────────────────────────────────────────────────────────────────────┐
│  Apache Kafka 3.5 (Confluent Platform 7.5.0)                         │
│  3 topics × 3 partitions                                             │
│  raw.orders  ·  raw.payments  ·  raw.rider_events                   │
└────────────────┬─────────────────────────────────────────────────────┘
                 │
        ┌────────┴──────────┐
        │                   │
        ▼                   ▼
┌───────────────┐  ┌─────────────────────────────────────────────────┐
│  HOT PATH     │  │  COLD PATH                                       │
│               │  │                                                   │
│  ClickHouse   │  │  PySpark 3.5 Structured Streaming               │
│  Kafka Engine │  │  3 always-on Docker services                     │
│  + MV →       │  │  watermark 2min · trigger 500ms                 │
│  ReplacingMT  │  │              │                                    │
│               │  │              ▼                                    │
│  Latency: ~3s │  │  MinIO (S3-compatible)                           │
│               │  │  s3a://food-delivery-lake/raw/{topic}/           │
│               │  │  Parquet · partitioned year/month/day/hour       │
│               │  │              │                                    │
│               │  │  Airflow @daily 1AM                              │
│               │  │  Spark batch → exact dedup → ClickHouse          │
│               │  │  Latency: ~24h (next morning)                    │
└───────┬───────┘  └──────────────┬──────────────────────────────────┘
        │                         │
        └───────────┬─────────────┘
                    ▼
┌──────────────────────────────────────────────────────────────────────┐
│  ClickHouse 24.x  ·  columnar OLAP                                   │
│  raw_orders / raw_payments / raw_rider_events (ReplacingMergeTree)  │
│  batch_daily_city_stats (MergeTree)                                  │
│                                                                       │
│  dbt-core 1.7 + dbt-clickhouse  (Airflow-triggered, hourly)         │
│  staging → intermediate → marts                                      │
│  fct_orders · dim_restaurant · dim_rider · rpt_hourly_revenue        │
│                                                                       │
│  Apache Airflow 2.8                                                   │
│  dbt_run (hourly) · monitor_kafka_lag (5min) · batch_daily (1AM)    │
└──────────────────────────────────────────────────────────────────────┘
                    │
                    ▼
┌──────────────────────────────────────────────────────────────────────┐
│  Prometheus 2.45.6  ·  7 scrape targets                              │
│  Kafka JMX · ClickHouse · Node · Spark drivers (×3) · self          │
│                                                                       │
│  Grafana 10.2.0  ·  2 dashboards                                     │
│  Business: orders/min · revenue/hr · city breakdown                  │
│  Infra: Kafka lag · Spark batch duration · ClickHouse queries        │
└──────────────────────────────────────────────────────────────────────┘
```

### How data flows

**Hot path — seconds latency:** Generator publishes JSON to Kafka. ClickHouse Kafka Engine reads continuously, pipes rows through a Materialized View into a `ReplacingMergeTree` table. Order queryable within ~3s of production — no ETL, no scheduling.

**Cold path — next-day, exact:** Same Kafka event consumed by PySpark Structured Streaming. Spark applies 2-minute watermark, deduplicates on `order_id + event_timestamp`, writes Parquet to MinIO partitioned by `year/month/day/hour`. At 2AM, Airflow triggers a Spark batch job that reads the full previous day from MinIO, applies exact deduplication (no watermark approximation, no dropped late events), inserts aggregate stats into ClickHouse via the `s3()` table function. Any date reprocessable on demand.

---

## Benchmarks

| Metric | Result |
|--------|--------|
| Kafka throughput | **1,244 orders/min** sustained (single-node Docker, benchmarked 2026-06-05) |
| ClickHouse ingestion latency | **P50 = 3.6s, P95 = 6.6s** (producer → ClickHouse, measured via `producer_ts`) |
| ClickHouse query latency | **3–19ms** on 261k rows · **P50 = 67ms, P95 = 116ms** at N=10 concurrent |
| ClickHouse query latency (scale) | **29–103ms** on 5M rows (perf test) |
| dbt tests | **55/55 pass** (100%) |
| Prometheus targets | **7/7 up** |
| Spark → MinIO throughput | **95 rows/sec** · ~14 GB/day at sustained rate |
| Events in 2.5h run | 17,830 orders + 17,818 payments + 6,200 GPS pings = **564k events** |

**Kafka SIGKILL recovery:** After a hard kill, producer reconnects within ~5s (measured 3 runs). No message loss with idempotent producer (`acks=all`).

---

## Tech Stack

| Tool | Version | Role |
|------|---------|------|
| Apache Kafka | 3.5 (Confluent 7.5.0) | Message broker, 3 topics × 3 partitions |
| PySpark | 3.5 | Structured Streaming → MinIO cold path |
| MinIO | 2024-01 | S3-compatible cold storage / replay archive |
| ClickHouse | 24.1 | OLAP DWH — Kafka Engine hot ingest + MergeTree |
| Apache Airflow | 2.8.0 | dbt orchestration + Kafka lag monitoring |
| dbt-core | 1.7.19 | SQL transformation (staging → intermediate → marts) |
| dbt-clickhouse | 1.7.7 | ClickHouse adapter |
| Grafana | 10.2.0 | Business + infra dashboards |
| Prometheus | 2.45.6 | Metrics scraping (7 targets) |
| Docker Compose | v2 | Full local infra (20 core + 5 monitoring containers) |
| Python | 3.11 | Async generator + Pydantic v2 schemas |

---

## Quick Start

**Requirements:** Docker Desktop with ~8GB RAM allocated, ~20GB free disk.

```bash
git clone https://github.com/JohnWickCP/vn-food-delivery-pipeline.git
cd vn-food-delivery-pipeline
git checkout docker-local

cp .env.example .env          # credentials already defaulted for local dev
make up                       # starts core stack (Kafka + Spark + ClickHouse + Airflow + generator)
make up-mon                   # starts monitoring stack (Prometheus + Grafana)
```

**Service URLs:**

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow | http://localhost:8080 | admin / admin |
| Spark Master UI | http://localhost:8081 | — |
| Grafana | http://localhost:3000 | admin / admin |
| Prometheus | http://localhost:9090 | — |
| Kafka UI | http://localhost:8090 | — |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| ClickHouse HTTP | http://localhost:8123 | — |

> Full setup guide with troubleshooting → **[SETUP.md](SETUP.md)**

### Stopping & cleanup

The pipeline generates ~600 MB/h of Parquet files in MinIO. Stop before disk fills.

```bash
make down          # stop all containers — data preserved in Docker volumes
make clean-data    # delete MinIO Parquet + Spark checkpoints only
make purge         # full reset: delete ALL volumes (ClickHouse, Kafka, Airflow metadata)
```

> **Windows WSL2:** Docker's `.vhdx` does not shrink after deleting data. After `make purge`, compact with `diskpart` to reclaim disk space (tested: 61 GB → 3.5 GB). Steps → [SETUP.md § 7](SETUP.md#7-stop--disk-management).

---

## Project Structure

```
vn-food-delivery-pipeline/
├── generator/
│   ├── producers/          # OrderProducer, PaymentProducer, RiderProducer
│   ├── schemas/            # Pydantic v2 models + Avro schemas (.avsc)
│   └── main.py             # asyncio.gather entry point
├── spark/
│   ├── Dockerfile          # PySpark 3.5 + Kafka + S3A jars
│   ├── jobs/               # stream_orders · stream_payments · stream_rider_events
│   │                       # batch_daily_summary (MinIO → ClickHouse via s3())
│   └── conf/spark-defaults.conf
├── clickhouse/
│   ├── init/               # 01_database · 02_raw_tables (ReplacingMergeTree)
│   │                       # 03_views · 04_kafka_engine · 05_batch_tables
│   └── bench_concurrent.py
├── airflow/
│   ├── Dockerfile
│   └── dags/               # dbt_run.py · monitor_kafka_lag.py · batch_daily_summary.py
├── dbt/
│   ├── models/
│   │   ├── staging/        # stg_orders · stg_payments · stg_riders · stg_order_items
│   │   ├── intermediate/   # int_order_payments · int_delivery_metrics
│   │   └── marts/          # fct_orders · dim_restaurant · dim_rider · rpt_hourly_revenue
│   └── profiles.yml
├── monitoring/
│   ├── prometheus/prometheus.yml
│   └── grafana/provisioning/
├── docker-compose.yml
├── docker-compose.monitoring.yml
├── Makefile
├── SETUP.md
└── .env.example
```

---

## Data Model

Star schema in `food_delivery_dbt_marts` ClickHouse database:

```
dim_restaurant ──┐
dim_rider      ──┼──► fct_orders ──► rpt_hourly_revenue
```

- `staging/` — rename + cast + `FINAL` dedup. No business logic.
- `intermediate/` — LEFT JOIN orders × payments, CASE WHEN business fields, rider daily metrics.
- `marts/` — analytics-ready. `fct_orders` = MergeTree materialized. Dimensions + reports aggregate from fct.

---

## Screenshots

| Screenshot | File | Status |
|-----------|------|--------|
| Grafana — business dashboard (orders/min, revenue by city) | `screenshots/grafana_business_dashboard.png` | [ ] |
| Grafana — infra dashboard (Kafka lag, Spark batch duration) | `screenshots/grafana_infra_dashboard.png` | [ ] |
| Kafka UI — topics, message throughput | `screenshots/kafka_ui_topics.png` | [ ] |
| MinIO console — bucket structure, Parquet file layout | `screenshots/minio_bucket.png` | [ ] |
| Airflow — DAG list + successful runs | `screenshots/airflow_dags.png` | [ ] |
| ClickHouse — row count + query on fct_orders | `screenshots/clickhouse_query.png` | [ ] |
| Prometheus — targets page (7/7 up) | `screenshots/prometheus_targets.png` | [ ] |

---

## Architecture Decisions

### Why Lambda Architecture (dual path)?

| | ClickHouse Kafka Engine (hot) | Spark → MinIO → ClickHouse (cold) |
|-|-------------------------------|-------------------------------------|
| Latency | ~3s | ~24h |
| Role | Real-time Grafana dashboards | Exact daily stats + historical reprocessing |
| Dedup | ReplacingMergeTree (lazy) + FINAL | Full-day `dropDuplicates` — no watermark approximation |
| Reprocessing | Not possible (Kafka TTL 24h) | Any date: `make batch DATE=YYYY-MM-DD` |

### Why ReplacingMergeTree?
Kafka consumers redeliver on restart. `ReplacingMergeTree` deduplicates lazily (background merge) or on-demand (`SELECT ... FINAL`), giving at-least-once delivery with eventual dedup without blocking ingestion.

### Why ClickHouse native port 9900 instead of 9000?
MinIO S3 API uses `9000`. Both services share the same Docker network — mapping ClickHouse native to `9900` avoids the conflict.

### Why fastavro UDF instead of `from_avro()`?
Confluent's `from_avro()` throws a `ClassCastException` with the Confluent Avro deserializer in a non-Confluent Spark cluster. A fastavro-based Python UDF deserializes bytes cleanly with no dependency on Confluent's Java libs.

---

## Known Limitations

1. **Single Kafka broker** — `replication-factor=1`. Production needs 3+ brokers for HA.
2. **Spark latency ~10s in dev** — S3A overhead per micro-batch on single-machine Docker. Trigger = 500ms but actual end-to-end = 8–18s.
3. **ClickHouse dedup is lazy** — Duplicates visible between background merges. Mitigated by `FINAL` on all staging views and dbt models.
4. **Non-atomic batch load** — `batch_daily_summary` does DELETE then INSERT. If INSERT fails mid-way, data is gone until rerun.
5. **Airflow LocalExecutor** — Production uses CeleryExecutor or KubernetesExecutor.
