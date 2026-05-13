# Vietnam Food Delivery Real-Time Analytics Pipeline

A production-style data engineering pipeline simulating a food delivery platform (GrabFood/ShopeeFood scale) in Vietnam — built to demonstrate real-time streaming, batch orchestration, and analytical modeling end-to-end.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    INGESTION LAYER                          │
│                                                             │
│  [Python Faker Generator]                                   │
│      orders, riders, restaurants, payments                  │
│           │                                                 │
│           ▼                                                 │
│  [Apache Kafka 3.6] ──── Topics:                            │
│      Broker × 1           - raw.orders                     │
│      Zookeeper × 1        - raw.riders                     │
│                           - raw.payments                   │
│                           - raw.restaurant_events          │
└─────────────────────────────────────────────────────────────┘
           │
           ▼
┌─────────────────────────────────────────────────────────────┐
│                   PROCESSING LAYER                          │
│                                                             │
│  [PySpark 3.5 Structured Streaming]                         │
│      - Consume from Kafka topics                            │
│      - Schema validation + deduplication (watermark)        │
│      - Enrich: join orders + restaurants                    │
│      - Write to PostgreSQL (append mode)                    │
│                                                             │
│  [Apache Airflow 2.8 DAGs]                                  │
│      - Schedule batch jobs (hourly)                         │
│      - Trigger dbt runs                                     │
│      - Monitor pipeline health + alert on failure           │
└─────────────────────────────────────────────────────────────┘
           │
           ▼
┌─────────────────────────────────────────────────────────────┐
│                  TRANSFORMATION LAYER                       │
│                                                             │
│  PostgreSQL 15                                              │
│  ├── raw schema  (from Spark write)                         │
│  │   ├── orders_raw                                         │
│  │   ├── payments_raw                                       │
│  │   └── rider_events_raw                                   │
│  └── dbt models                                             │
│      ├── staging/   (clean + cast types)                    │
│      ├── intermediate/  (business logic + joins)            │
│      └── marts/     (analytics-ready, Grafana source)       │
└─────────────────────────────────────────────────────────────┘
           │
           ▼
┌─────────────────────────────────────────────────────────────┐
│                 OBSERVABILITY LAYER                         │
│                                                             │
│  Prometheus ──scrape──► Kafka JMX Exporter                  │
│       │                 Spark metrics endpoint               │
│       ▼                                                     │
│  Grafana 10 Dashboards:                                     │
│      - Kafka: messages/sec, consumer lag, topic size        │
│      - Spark: processing time, batch duration, records/s    │
│      - Business: orders/min, revenue/hour, city heatmap     │
└─────────────────────────────────────────────────────────────┘
```

## Tech Stack

| Tool | Version | Role |
|------|---------|------|
| Apache Kafka | 3.6 | Message broker / streaming ingest |
| PySpark | 3.5 | Structured Streaming engine |
| Apache Airflow | 2.8 | Orchestration + scheduling |
| dbt-core | 1.7 | SQL transformation + testing |
| PostgreSQL | 15 | Data warehouse (local) |
| Grafana | 10 | Observability dashboards |
| Prometheus | 2.x | Metrics collection |
| Docker Compose | v2 | Full local infra |
| Python | 3.11 | Data generator + ETL scripts |

## Key Metrics

| Metric | Target | How Measured |
|--------|--------|--------------|
| Kafka throughput | 5,000+ orders/min | `kafka-consumer-groups.sh --describe` |
| Spark latency | < 500ms avg | Spark UI → Streaming tab |
| dbt test coverage | 100% (30+ tests) | `dbt test` |
| Pipeline uptime | ≥ 99% | Airflow DAG success/total ratio |
| Daily event volume | ~5M events/day | PostgreSQL `COUNT(*)` by day |
| 30-day data size | ~20 GB | `du -sh` postgres data dir |

> Measured results → [`docs/metrics_results.md`](docs/metrics_results.md) *(added after pipeline runs)*

## Quick Start

```bash
git clone https://github.com/JohnWickCP/vn-food-delivery-pipeline.git
cd vn-food-delivery-pipeline
cp .env.example .env          # fill in credentials
make up                       # starts all services
make logs                     # tail all container logs
```

**Service URLs after `make up`:**

| Service | URL |
|---------|-----|
| Airflow UI | http://localhost:8080 |
| Spark Master | http://localhost:8081 |
| Grafana | http://localhost:3000 |
| Prometheus | http://localhost:9090 |

## Project Structure

```
vn-food-delivery-pipeline/
├── kafka/                    # Topic creation scripts
├── generator/                # Python Faker-based data producer
│   ├── producers/            # order / payment / rider producers
│   └── models/               # Pydantic schemas
├── spark/                    # PySpark Structured Streaming jobs
│   ├── jobs/                 # stream_orders, stream_payments, stream_riders
│   └── utils/                # SparkSession factory, Kafka + DB helpers
├── airflow/
│   ├── dags/                 # pipeline_orchestrator, dbt_runner, health_check
│   └── plugins/operators/    # Custom SparkSubmitOperator
├── dbt/
│   ├── models/
│   │   ├── staging/          # 1-to-1 with raw tables, rename + cast only
│   │   ├── intermediate/     # joins + business logic
│   │   └── marts/            # fct_orders, dim_*, rpt_hourly_revenue
│   └── tests/                # custom singular tests
├── monitoring/
│   ├── prometheus/           # prometheus.yml scrape config
│   └── grafana/              # provisioning + dashboard JSON
├── postgres/init/            # schema + raw table DDL
├── scripts/                  # setup.sh, measure_metrics.sh, load_test.sh
├── docs/                     # architecture + data model diagrams
├── docker-compose.yml
├── docker-compose.monitoring.yml
├── Makefile
└── .env.example
```

## Data Model

Star schema in the `marts` layer:

```
dim_restaurant ──┐
dim_rider      ──┤──► fct_orders ──► rpt_hourly_revenue
dim_time       ──┘
```

**Key fields in `fct_orders`:** `order_id`, `restaurant_id`, `rider_id`, `city`, `status`, `total_vnd`, `delivery_fee`, `placed_at`, `delivered_at`, `delivery_min`

**Kafka topics:** `raw.orders` (5,000 msg/min), `raw.payments`, `raw.rider_events` (GPS ping every 30s)

**Cities covered:** Hanoi · Ho Chi Minh City · Da Nang

## Dashboard Screenshots

*(Added after Grafana dashboards are built — commit 38–40)*

## Commit History

42 commits across 6 phases, each commit = one concrete deliverable:

| Phase | Commits | Focus |
|-------|---------|-------|
| 1 | 1–10 | Infrastructure (Docker Compose, all services) |
| 2 | 11–17 | Data generator (Kafka producers, Pydantic models) |
| 3 | 18–25 | Spark Streaming (consume Kafka → write PostgreSQL) |
| 4 | 26–31 | Airflow orchestration (DAGs, alerts, tests) |
| 5 | 32–37 | dbt transformation (staging → intermediate → marts) |
| 6 | 38–42 | Observability (Grafana dashboards, metrics, final docs) |

## What I Learned

- **Exactly-once semantics** in Kafka → Spark requires coordinating manual offset commits with watermark-based deduplication; auto-commit is not enough for streaming guarantees.
- **Watermarks** in PySpark Structured Streaming control state retention — setting them too long wastes memory, too short drops legitimate late events.
- **dbt layering** (staging / intermediate / marts) enforces a clean separation of concerns: staging is a thin translation layer, intermediate owns the business logic, marts expose only what dashboards need.
- **Docker Compose networking** — Kafka needs both `INTERNAL` and `EXTERNAL` listeners; containers use the internal listener, host tools use external. A single listener causes silent connection failures.
- **Observability as code** — Grafana dashboard JSON + Prometheus scrape config in version control means the monitoring layer is reproducible, not a manual click-ops artifact.
