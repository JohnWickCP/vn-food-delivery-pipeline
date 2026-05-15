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
| Tốc độ xử lý | ~1,500 đơn/phút liên tục (có thể mở rộng lên 5,000+/phút) |
| Độ trễ vào ClickHouse | vài giây kể từ khi đặt đơn |
| Tốc độ query phân tích | 3–19ms trên 260,000+ bản ghi |
| Kiểm thử dữ liệu | 55/55 test pass, 100% |
| Hệ thống giám sát | 7/7 service được theo dõi |
| Lưu trữ lạnh | 4.2 GB sau 2.5 giờ chạy |

---

*Phần kỹ thuật chi tiết bên dưới — dành cho kỹ sư muốn hiểu sâu stack và thiết kế.*

---

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
           └── COLD PATH (batch layer) ─────────────────────────┐
                                                                   │
               [PySpark 3.5 Structured Streaming]                  │
                 stream_orders / stream_payments / stream_riders   │
                 watermark + dropDuplicates → Parquet              │
                        │                                          │
                        ▼                                          │
               [MinIO — S3-compatible]                             │
                 s3a://food-delivery-lake/raw/{topic}/             │
                 partitioned by year/month/day                     │
                        │                                          │
               [Airflow @daily — batch_daily_summary]              │
                 Spark reads full day → exact dedup on order_id    │
                 writes s3a://.../batch/daily_summary/date=.../    │
                        │                                          │
                        ▼                                          │
               [ClickHouse — batch_daily_city_stats]               │
                 INSERT via s3() table function                     │
                 Role: exact daily stats · historical reprocessing  │
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

> Full setup guide with troubleshooting → **[SETUP.md](SETUP.md)**

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

**Why dual-path (Lambda Architecture)?**

| | ClickHouse Kafka Engine (hot) | Spark → MinIO → ClickHouse (batch) |
|-|-------------------------------|-------------------------------------|
| Latency | seconds | ~24h (runs next day at 1 AM) |
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

## What I Learned

- **True Lambda Architecture**: Speed layer (Kafka→ClickHouse Kafka Engine) for <10s latency dashboards; batch layer (Spark→MinIO→ClickHouse) for exact daily stats. Speed layer trades accuracy for latency (watermark drops late events, ReplacingMergeTree dedup is lazy). Batch layer trades latency for correctness (full-day exact dedup, re-runnable for any date). Both paths feed separate ClickHouse tables; Grafana queries whichever fits the use case.
- **ClickHouse Kafka Engine pattern**: Requires exactly 3 objects — Kafka Engine table (buffer), ReplacingMergeTree table (storage), Materialized View (pipeline). Missing any one causes silent data loss.
- **dbt layering discipline**: Staging = rename+cast only. Business logic (CASE WHEN, date derivations) belongs in intermediate. Marts reference only intermediate or other marts — never staging directly.
- **Spark Structured Streaming dedup**: `dropDuplicates` must include the watermark column (`event_timestamp`) to bound state size. Without it, state grows unbounded and OOM is inevitable.
- **Docker Compose networking**: Kafka needs `INTERNAL://kafka:29092` for container-to-container and `EXTERNAL://localhost:9092` for host tools. ClickHouse native port remapped to 9900 to avoid conflict with MinIO 9000.
- **Prometheus version matters**: 2.49.0 sends `q=2` in Accept headers (RFC 7231 violation) → Spark Jetty returns 400. Pinned to 2.45.6 LTS.
