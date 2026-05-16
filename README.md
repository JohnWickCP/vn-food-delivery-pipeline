# Vietnam Food Delivery Real-Time Analytics Pipeline

## Dự án này làm gì?

Hãy tưởng tượng bạn đang điều hành một ứng dụng đặt đồ ăn như GrabFood hay ShopeeFood tại Việt Nam. Mỗi phút có hàng nghìn đơn hàng được đặt ở Hà Nội, TP.HCM, Đà Nẵng. Câu hỏi đặt ra là:

- Giờ này đang có bao nhiêu đơn/phút? Thành phố nào đang bận nhất?
- Shipper nào đang di chuyển chậm bất thường? Pin sắp hết chưa?
- Doanh thu giờ ăn trưa hôm nay so với hôm qua thế nào?
- Nếu hệ thống thanh toán chậm, bao nhiêu đơn đang bị tồn?

**Dự án này là hệ thống xử lý và trả lời những câu hỏi đó — trong vài giây, liên tục 24/7.**

Thay vì dùng dữ liệu thật (cần làm việc với doanh nghiệp thực tế), dự án tự sinh ra ~1,500 đơn hàng/phút giả lập hoàn toàn giống hành vi người dùng thật: cao điểm lúc 11–13h và 18–20h, 200 shipper chạy GPS mỗi 30 giây, thanh toán xử lý sau 10–180 giây.

---

## Hệ thống hoạt động như thế nào?

```
Dữ liệu được sinh ra → Truyền qua hàng đợi → Lưu trữ → Phân tích → Hiển thị
```

Nói cụ thể hơn:

**1. Sinh dữ liệu** — Python tạo ra đơn hàng, thanh toán, vị trí GPS của shipper giống thật. Đơn hàng có món ăn, giá tiền, quận huyện, nền tảng (iOS/Android/web). Thanh toán qua MoMo, VNPay, ZaloPay, tiền mặt.

**2. Hàng đợi tin nhắn (Apache Kafka)** — Toàn bộ dữ liệu đi qua Kafka, giống như một băng chuyền vận chuyển hàng hóa tốc độ cao. Kafka đảm bảo không mất dữ liệu dù phần sau của hệ thống tạm thời chậm hay gặp sự cố.

**3. Hai con đường lưu trữ song song:**
- **Con đường nóng:** Dữ liệu vào ClickHouse trong vài giây — dùng để hiển thị dashboard real-time trên Grafana
- **Con đường lạnh:** Spark xử lý và lưu file Parquet vào MinIO (giống S3) — mỗi đêm Airflow chạy batch job đọc lại toàn bộ ngày hôm qua, tính toán chính xác (không bị giới hạn bởi cửa sổ thời gian như real-time), rồi nạp kết quả vào ClickHouse

**4. Biến đổi dữ liệu (dbt)** — Mỗi giờ, dbt chạy các câu SQL để tính toán: doanh thu theo giờ, hiệu suất từng shipper, tỷ lệ hủy đơn theo nhà hàng, giờ cao điểm. Kết quả lưu thành các bảng sẵn sàng để query.

**5. Điều phối (Airflow)** — Giống người quản lý ca, đảm bảo các bước chạy đúng giờ, tự động báo lỗi nếu có vấn đề.

**6. Quan sát hệ thống (Prometheus + Grafana)** — Dashboard hiển thị số đơn/phút, lag của hàng đợi, thời gian xử lý của Spark, tình trạng tất cả 20 service đang chạy.

---

## Kết quả đo được

| Chỉ số | Giá trị |
|--------|---------|
| Tốc độ xử lý | ~1,000–2,000 đơn/phút (off-peak đến peak, single-node Docker) |
| Độ trễ vào ClickHouse | vài giây kể từ khi đặt đơn (Kafka Engine) |
| Tốc độ query phân tích | 3–19ms trên 261,000+ bản ghi |
| Kiểm thử dữ liệu | 55/55 test pass, 100% |
| Hệ thống giám sát | 7/7 service được theo dõi |
| Lưu trữ lạnh (MinIO) | 161 MiB / 16 phút → ~14 GB/ngày ở throughput ổn định |

---

*Phần kỹ thuật chi tiết bên dưới — dành cho kỹ sư muốn hiểu sâu stack và thiết kế.*

---

A production-style data engineering portfolio project simulating a food delivery platform (GrabFood/ShopeeFood scale) in Vietnam — demonstrating real-time streaming, dual-path Lambda Architecture ingestion, batch orchestration, and analytical modeling end-to-end. Fully Dockerized, zero cloud cost.

## Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│  INGESTION LAYER                                                      │
│                                                                       │
│  Python Generator (asyncio + Pydantic v2)                            │
│  ├─ OrderProducer    ~1,000–5,000 orders/min (off-peak → peak)      │
│  ├─ PaymentProducer  async, linked to order state machine            │
│  └─ RiderProducer    200 riders × GPS ping every 30 s               │
│                │                                                      │
│                ▼                                                      │
│  Apache Kafka 3.5  ·  3 topics × 3 partitions                       │
│  raw.orders  ·  raw.payments  ·  raw.rider_events                    │
└────────────────┬─────────────────────────────────────────────────────┘
                 │
        ┌────────┴──────────┐
        │                   │
        ▼                   ▼
┌───────────────┐  ┌─────────────────────────────────────────────────┐
│  HOT PATH     │  │  COLD PATH                                       │
│  (real-time)  │  │  (batch, nightly)                                │
│               │  │                                                   │
│  ClickHouse   │  │  PySpark Structured Streaming                    │
│  Kafka Engine │  │  3 always-on Docker services                     │
│               │  │  watermark 2 min · trigger 500 ms               │
│  Kafka queue  │  │              │                                    │
│  table        │  │              ▼                                    │
│    │  (MV)    │  │  MinIO — S3-compatible data lake                 │
│    ▼           │  │  s3a://food-delivery-lake/raw/{topic}/          │
│  raw tables   │  │  partitioned year / month / day / hour          │
│  (Replacing   │  │              │                                    │
│  MergeTree)   │  │  Airflow @daily 2 AM                             │
│               │  │  └─ Spark batch: exact dedup per day            │
│  Latency: s   │  │     INSERT → ClickHouse via s3() function        │
│               │  │  Latency: ~24 h (next morning)                   │
└───────┬───────┘  └──────────────┬──────────────────────────────────┘
        │                         │
        └───────────┬─────────────┘
                    ▼
┌──────────────────────────────────────────────────────────────────────┐
│  WAREHOUSE + TRANSFORMATION LAYER                                     │
│                                                                       │
│  ClickHouse 24.x  ·  columnar OLAP                                   │
│  raw_orders / raw_payments / raw_rider_events  (ReplacingMergeTree)  │
│  batch_daily_city_stats                        (MergeTree)           │
│                                                                       │
│  dbt-core 1.7 + dbt-clickhouse  (Airflow-triggered, hourly)         │
│  staging/      → rename + cast + FINAL dedup                         │
│  intermediate/ → joins, business logic (meal_period, metrics)        │
│  marts/        → fct_orders · dim_restaurant · dim_rider             │
│                   rpt_hourly_revenue                                  │
│                                                                       │
│  Apache Airflow 2.8                                                   │
│  ├─ dbt_run             hourly at HH:05                              │
│  ├─ monitor_kafka_lag   every 5 min, alert on lag / silence          │
│  └─ batch_daily_summary @daily 2 AM                                  │
└──────────────────────────────────────────────────────────────────────┘
                    │
                    ▼
┌──────────────────────────────────────────────────────────────────────┐
│  OBSERVABILITY LAYER                                                  │
│                                                                       │
│  Prometheus 2.45.6  ── 7 scrape targets:                             │
│  Kafka JMX · ClickHouse · Node · Spark drivers (×3) · self          │
│                    │                                                  │
│  Grafana 10  ── 2 dashboards:                                        │
│  Business: orders/min · revenue/hr · city breakdown                  │
│  Infra:    Kafka lag · Spark batch duration · ClickHouse queries     │
└──────────────────────────────────────────────────────────────────────┘
```

### How data flows

Each food order takes two parallel routes from generator to dashboard:

**Hot path — seconds latency:** The generator publishes a JSON event to Kafka. ClickHouse's built-in Kafka Engine table reads from the topic continuously and pipes rows through a Materialized View into a `ReplacingMergeTree` table. The order is queryable in ClickHouse within seconds of being produced — no ETL job, no scheduling, no intermediate storage.

**Cold path — next-day, exact:** The same Kafka event is also consumed by PySpark Structured Streaming (three always-on Docker services, one per topic). Spark applies a 2-minute watermark, deduplicates on `order_id + event_timestamp`, and writes compressed Parquet files to MinIO partitioned by `year/month/day/hour`. At 2 AM, Airflow triggers a Spark batch job that reads the full previous day from MinIO, applies exact deduplication (no watermark approximation, no dropped late events), and inserts aggregate stats into ClickHouse via the built-in `s3()` table function. Any date can be reprocessed on demand.

**Both paths feed the same ClickHouse instance.** Hot tables answer "what is happening right now?". Batch tables answer "what exactly happened yesterday?". dbt runs hourly on top of both, producing `fct_orders`, dimension tables, and `rpt_hourly_revenue` that Grafana queries directly.

## Tech Stack

| Tool | Version | Role |
|------|---------|------|
| Apache Kafka | 3.5 (Confluent Platform 7.5.0) | Message broker, 3 topics × 3 partitions |
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
| Kafka throughput | 5,000+ orders/min (peak) | ~1,000–2,000 orders/min on single-node Docker; architecture supports 5,000+/min at scale |
| ClickHouse query latency | <100ms on 5M rows | **3–19ms on 261k rows**; 29–103ms on 5M (prior perf test) |
| dbt test coverage | 100% (55 tests) | **55/55 tests pass** |
| Prometheus targets | 7/7 up | **7/7 up** |
| MinIO cold storage | ~14GB / day | 161 MiB in 16 min → ~600 MB/h → ~14 GB/day at sustained rate |
| Events ingested | 5M+/day theoretical | 17,830 orders + 17,818 payments + 6,200 GPS pings in 16 min |
| dbt models | 10 models, 3 layers | 10/10 PASS (staging → intermediate → marts) |

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

> Full setup guide with troubleshooting → **[SETUP.md](SETUP.md)**

**Service URLs after `make up` + `make up-mon`:**

| Service | URL | Notes |
|---------|-----|-------|
| Airflow | http://localhost:8080 | admin / admin |
| Spark Master | http://localhost:8081 | |
| Grafana | http://localhost:3000 | admin / admin |
| Prometheus | http://localhost:9090 | |
| Kafka UI | http://localhost:8090 | |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |

## Stopping & Cleanup

The pipeline generates ~600 MB/h of Parquet files in MinIO. Stop before disk fills up.

```bash
make down          # stop all containers — data preserved in Docker volumes
make clean-data    # delete MinIO Parquet + Spark checkpoints only (fastest disk recovery)
make purge         # full reset: delete ALL volumes (ClickHouse, Kafka, Airflow metadata)
```

> **Windows WSL2:** Docker's `.vhdx` file does not shrink after deleting data inside it. After `make purge`, compact it manually with `diskpart` to reclaim disk space (tested: 61 GB → 3.5 GB). Full steps → [SETUP.md § 7](SETUP.md#7-stop--disk-management).

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
│   │                         # batch_daily_summary (reads MinIO → aggregates → MinIO)
│   └── conf/spark-defaults.conf
├── clickhouse/
│   └── init/                 # 01_create_database, 02_raw_tables (ReplacingMergeTree),
│                             # 03_views, 04_kafka_engine (Kafka Engine + MVs)
│                             # 05_batch_tables (batch_daily_city_stats)
├── airflow/
│   ├── Dockerfile            # python3.9, dbt-core==1.7.19, dbt-clickhouse==1.7.7
│   └── dags/                 # dbt_run.py, monitor_kafka_lag.py, batch_daily_summary.py
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

**Why dual-path (Lambda Architecture)?**

| | ClickHouse Kafka Engine (hot) | Spark → MinIO → ClickHouse (batch) |
|-|-------------------------------|-------------------------------------|
| Latency | seconds | ~24h (runs next day at 2 AM) |
| Role | real-time Grafana dashboards | exact daily stats + historical reprocessing |
| Dedup | ReplacingMergeTree (lazy) + FINAL | full-day `dropDuplicates(["order_id"])` — no watermark approximation |
| Reprocessing | not possible (Kafka retention 24h) | re-run any date: `docker compose --profile batch run --rm spark-batch-daily --date YYYY-MM-DD` |
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
6. **Hardcoded credentials in batch DAG** — `airflow/dags/batch_daily_summary.py` hardcodes MinIO/ClickHouse host+credentials. Production: use Airflow Connections (Admin → Connections UI) and retrieve via `BaseHook.get_connection()`.
7. **Spark batch not wired via SparkSubmitOperator** — Streaming jobs run as `restart: unless-stopped` Docker services. Batch job runs via `docker compose --profile batch run`. Airflow DAG assumes Spark finished by 2 AM; no actual SparkSubmitOperator. Production: use `SparkSubmitOperator` or `KubernetesPodOperator`.
8. **Non-atomic batch load** — `batch_daily_summary` DAG does DELETE then INSERT as separate statements. If INSERT fails mid-way, the day's data is gone until rerun. Production: use ReplacingMergeTree version column (insert new version, rely on FINAL for dedup) instead of DELETE+INSERT.
