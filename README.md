# Vietnam Food Delivery Real-Time Analytics Pipeline

A production-style data engineering portfolio project simulating a food delivery platform (GrabFood/ShopeeFood scale) in Vietnam — demonstrating real-time streaming, dual-path ingestion, batch orchestration, and analytical modeling end-to-end.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        INGESTION LAYER                          │
│                                                                 │
│  [Python Generator — asyncio, Pydantic v2]                      │
│    OrderProducer  (5,000/min peak · 1,200/min base)             │
│    PaymentProducer (async queue from OrderProducer)             │
│    RiderProducer  (200 riders, 30s GPS pings)                   │
│           │                                                     │
│           ▼                                                     │
│  [Apache Kafka 3.6]  ── Topics:                                 │
│    Broker × 1            - raw.orders      (3 partitions)       │
│    Zookeeper × 1         - raw.payments    (3 partitions)       │
│                          - raw.rider_events (3 partitions)      │
└─────────────────────────────────────────────────────────────────┘
           │
           ├──── HOT PATH (real-time) ─────────────────────────────┐
           │                                                        │
           │   [ClickHouse 24.x — Kafka Engine]                     │
           │     kafka_orders_queue  → orders_mv  → raw_orders      │
           │     kafka_payments_queue → payments_mv → raw_payments  │
           │     kafka_rider_events_queue → ... → raw_rider_events  │
           │     Engine: ReplacingMergeTree(_ingested_at)           │
           │     Latency: seconds                                   │
           │                                                        │
           └── COLD PATH (archival) ──────────────────────────────┐
                                                                   │
               [PySpark 3.5 Structured Streaming]                  │
                 stream_orders / stream_payments / stream_riders   │
                 watermark + dropDuplicates → Parquet              │
                        │                                          │
                        ▼                                          │
               [MinIO — S3-compatible]                             │
                 s3a://food-delivery-lake/raw/{topic}/             │
                 partitioned by year/month/day                     │
                 Role: cold storage · replay · audit               │
                                                                   │
└──────────────────────────────────────────────────────────────────┘
           │
           ▼
┌─────────────────────────────────────────────────────────────────┐
│                    TRANSFORMATION LAYER                         │
│                                                                 │
│  [dbt-core 1.7 + dbt-clickhouse]                                │
│    staging/   — rename + cast + FINAL dedup                     │
│    intermediate/ — joins + business logic                       │
│    marts/     — fct_orders, dim_*, rpt_hourly_revenue           │
│                                                                 │
│  [Apache Airflow 2.8]                                           │
│    dbt_run DAG        — hourly at HH:05                         │
│    monitor_kafka_lag  — every 5 min, alert on lag/silence       │
└─────────────────────────────────────────────────────────────────┘
           │
           ▼
┌─────────────────────────────────────────────────────────────────┐
│                    OBSERVABILITY LAYER                          │
│                                                                 │
│  Prometheus 2.45.6  ──scrape──►  Kafka JMX Exporter            │
│       │                          ClickHouse Exporter            │
│       │                          Node Exporter                  │
│       │                          Spark Driver (4040)            │
│       ▼                                                         │
│  Grafana 10  ── Business Dashboard (orders/min, revenue/hr)     │
│               ── Infra Dashboard   (Kafka lag, Spark batches)   │
└─────────────────────────────────────────────────────────────────┘
```

## Tech Stack

| Tool | Version | Role |
|------|---------|------|
| Apache Kafka | 3.6 | Message broker, 3 topics × 3 partitions |
| PySpark | 3.5 | Structured Streaming → MinIO cold path |
| MinIO | 2024-01 | S3-compatible cold storage / replay archive |
| ClickHouse | 24.1 | OLAP DWH — Kafka Engine hot ingestion + MergeTree |
| Apache Airflow | 2.8 | dbt orchestration + Kafka lag monitoring |
| dbt-core | 1.7.19 | SQL transformation (staging → intermediate → marts) |
| dbt-clickhouse | 1.7.7 | ClickHouse adapter for dbt |
| Grafana | 10 | Business + infra dashboards |
| Prometheus | 2.45.6 | Metrics scraping (7 targets) |
| Docker Compose | v2 | Full local infra (20 services) |
| Python | 3.11 | Async data generator + Pydantic v2 schemas |

## Measured Results

| Metric | Target | Actual |
|--------|--------|--------|
| Kafka throughput | 5,000+ orders/min (peak) | ~1,500/min sustained on single-node dev; architecture supports 5k+ at scale |
| ClickHouse query latency | <100ms on 5M rows | 3–19ms on 257k rows |
| dbt test coverage | 100% (30+ tests) | **55/55 tests pass** |
| Prometheus targets | 7/7 up | 7/7 up |
| MinIO cold storage | ~20GB / 30d | 4.2GB after 2h (on track) |
| Events ingested | 7M+/day theoretical | ~257k orders + ~257k payments + ~40k GPS pings |

> Full measured output → [`docs/metrics_results.md`](docs/metrics_results.md)

## Quick Start

```bash
git clone https://github.com/JohnWickCP/vn-food-delivery-pipeline.git
cd vn-food-delivery-pipeline
cp .env.example .env          # credentials already defaulted for local dev
make up                       # starts core stack (Kafka, Spark, ClickHouse, Airflow, generator)
make up-mon                   # starts monitoring stack (Prometheus + Grafana)
make metrics                  # print CV metrics report
```

**Service URLs after `make up` + `make up-mon`:**

| Service | URL | Notes |
|---------|-----|-------|
| Airflow | http://localhost:8080 | admin/admin |
| Spark Master | http://localhost:8081 | |
| Grafana | http://localhost:3000 | admin/admin |
| Prometheus | http://localhost:9090 | |
| Kafka UI | http://localhost:8090 | |
| MinIO Console | http://localhost:9001 | minioadmin/minioadmin |

## Project Structure

```
vn-food-delivery-pipeline/
├── generator/                # Async Kafka producers (orders, payments, riders)
│   ├── producers/            # OrderProducer, PaymentProducer, RiderProducer
│   ├── schemas/              # Pydantic v2 models (Order, Payment, RiderEvent)
│   ├── config.py             # Shared RIDER_POOL + Kafka config
│   └── main.py               # asyncio.gather entry point
├── spark/
│   ├── Dockerfile            # PySpark 3.5 + Kafka + S3A jars
│   ├── jobs/                 # stream_orders, stream_payments, stream_rider_events
│   └── conf/spark-defaults.conf
├── clickhouse/
│   └── init/                 # 01_create_database, 02_raw_tables (ReplacingMergeTree),
│                             # 03_views, 04_kafka_engine (Kafka Engine + MVs)
├── airflow/
│   ├── Dockerfile            # python3.9, dbt-core==1.7.19, dbt-clickhouse==1.7.7
│   └── dags/                 # dbt_run.py, monitor_kafka_lag.py
├── dbt/
│   ├── models/
│   │   ├── staging/          # stg_orders, stg_payments, stg_riders, stg_order_items
│   │   ├── intermediate/     # int_order_payments (LEFT JOIN), int_delivery_metrics
│   │   └── marts/            # fct_orders, dim_restaurant, dim_rider, rpt_hourly_revenue
│   └── profiles.yml          # ClickHouse HTTP connection via env vars
├── monitoring/
│   ├── prometheus/prometheus.yml   # 7 scrape targets
│   └── grafana/provisioning/       # datasources + dashboard JSON
├── minio/init_buckets.sh     # Creates food-delivery-lake bucket
├── scripts/measure_metrics.sh
├── docs/
│   ├── IMPLEMENTATION_NOTES.md     # Architecture decisions + per-phase gotchas
│   ├── STUDY_GUIDE.md
│   └── metrics_results.md          # Measured CV metrics
├── docker-compose.yml
├── docker-compose.monitoring.yml
├── Makefile
└── .env.example
```

## Data Model

Star schema in the `food_delivery_dbt_marts` ClickHouse database:

```
dim_restaurant ──┐
dim_rider      ──┤──► fct_orders ──► rpt_hourly_revenue
```

**dbt layering:**
- `staging/` — rename + cast + `FINAL` (dedup on ReplacingMergeTree). No business logic.
- `intermediate/` — LEFT JOIN orders × payments, CASE WHEN business fields (meal_period), rider daily metrics.
- `marts/` — analytics-ready tables. `fct_orders` = MergeTree, materialized table. Dimensions and reports aggregate from fct.

## Architecture Decisions

**Why dual-path (Kafka Engine + Spark→MinIO)?**

| | ClickHouse Kafka Engine (hot) | Spark → MinIO (cold) |
|-|-------------------------------|----------------------|
| Latency | seconds | minutes (S3A overhead) |
| Role | real-time Grafana dashboards | long-term archive + replay |
| Dedup | ReplacingMergeTree (lazy) + FINAL | watermark + dropDuplicates |
| Retention | unlimited (disk-bound) | unlimited (object storage) |

**Why ReplacingMergeTree instead of plain MergeTree?**
Kafka consumers can redeliver messages on restart. ReplacingMergeTree deduplicates lazily (background merge) or on-demand (`SELECT ... FINAL`), giving at-least-once delivery with eventual dedup.

**Why ClickHouse native port mapped to 9900?**
MinIO S3 API already uses `9000`. Both services in the same Docker network would conflict on the same port.

## Known Limitations

1. **Single Kafka broker** — `replication-factor=1`. Production needs 3+ brokers for HA.
2. **Spark latency ~10s in dev** — S3A overhead per micro-batch on single-machine Docker. Trigger interval = 500ms but actual end-to-end = 8–18s. On a real cluster with proper S3A connection pooling, 500ms is achievable.
3. **No schema registry** — Avro + Confluent Schema Registry is production standard. Omitted to reduce complexity.
4. **ClickHouse dedup is lazy** — Duplicates visible between background merges. Mitigated by `FINAL` keyword on all staging views and dbt models.
5. **Airflow LocalExecutor** — production uses CeleryExecutor or KubernetesExecutor. Sufficient for single-machine demo.

## What I Learned

- **Dual-path Lambda Architecture**: Speed layer (Kafka→ClickHouse Kafka Engine) for <10s latency dashboards; batch layer (Spark→MinIO Parquet) for durable cold storage and replay. Both paths consume from the same Kafka topics using separate consumer groups.
- **ClickHouse Kafka Engine pattern**: Requires exactly 3 objects — Kafka Engine table (buffer), ReplacingMergeTree table (storage), Materialized View (pipeline). Missing any one causes silent data loss.
- **dbt layering discipline**: Staging = rename+cast only. Business logic (CASE WHEN, date derivations) belongs in intermediate. Marts reference only intermediate or other marts — never staging directly.
- **Spark Structured Streaming dedup**: `dropDuplicates` must include the watermark column (`event_timestamp`) to bound state size. Without it, state grows unbounded and OOM is inevitable.
- **Docker Compose networking**: Kafka needs `INTERNAL://kafka:29092` for container-to-container and `EXTERNAL://localhost:9092` for host tools. ClickHouse native port remapped to 9900 to avoid conflict with MinIO 9000.
- **Prometheus version matters**: 2.49.0 sends `q=2` in Accept headers (RFC 7231 violation) → Spark Jetty returns 400. Pinned to 2.45.6 LTS.
