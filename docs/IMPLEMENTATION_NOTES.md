# Implementation Notes — Vietnam Food Delivery Pipeline

> Ghi lại các thay đổi kiến trúc, technical gotchas, và bẫy mà bất kỳ ai build project này đều có thể gặp.
> Update file này mỗi khi gặp vấn đề mới hoặc quyết định thay đổi design.

---

## Architecture Changes Log

### v2 → v3: Thêm ClickHouse Kafka Engine (dual-path ingestion)

**Thay đổi:**
- Thêm path thứ 2: `Kafka → ClickHouse Kafka Engine` cho real-time dashboard
- MinIO giữ nguyên nhưng role đổi: cold storage / replay only, không dùng cho analytics
- Loại bỏ Airflow DAG `load_minio_clickhouse` — không còn cần thiết
- Airflow giờ có 3 DAGs: `dbt_run` (hourly) + `monitor_kafka_lag` (every 5 min) + `batch_daily_summary` (daily 2 AM, load Spark batch output vào ClickHouse)
- Đổi MergeTree → ReplacingMergeTree cho real-time ClickHouse tables
- Thêm `items String` column vào raw_orders (lưu JSON raw của items array)

**Lý do thay đổi:**
- Kiến trúc cũ: Grafana dashboard lag 1 giờ (phải chờ Airflow batch load)
- Kiến trúc mới: data vào ClickHouse trong vài giây sau khi Kafka nhận
- Loại bỏ DAG phức tạp nhất (file tracking, idempotency, S3 function)
- Đúng với Lambda Architecture: speed layer (Kafka→ClickHouse) + batch/cold layer (Spark→MinIO)

**ClickHouse Kafka Engine — pattern bắt buộc (3 objects):**
```sql
-- 1. Buffer table: Kafka Engine (không lưu data, chỉ là queue)
CREATE TABLE kafka_orders_queue (...) ENGINE = Kafka SETTINGS ...;

-- 2. Storage table: ReplacingMergeTree (lưu data thật)
CREATE TABLE raw_orders_rt (...) ENGINE = ReplacingMergeTree(_ingested_at) ...;

-- 3. Materialized View: pipeline giữa 2 tables trên (tự động trigger)
CREATE MATERIALIZED VIEW orders_mv TO raw_orders_rt AS SELECT * FROM kafka_orders_queue;
```
Nếu thiếu bất kỳ 1 trong 3 → data không flow. Đây là bẫy hay gặp nhất.

---

### Staging layer fix: meal_period moved to intermediate

**Thay đổi:** `meal_period` CASE WHEN bị remove khỏi `stg_orders.sql`, move sang `int_delivery_metrics.sql`

**Lý do:** Staging = rename + cast only. Business logic trong staging vi phạm dbt layering principle.

---

## Technical Gotchas — Phase by Phase

### Phase 1: Infrastructure

| Vấn đề | Biểu hiện | Fix |
|--------|-----------|-----|
| `cp-zookeeper` không có `netcat` | healthcheck `nc -z` fail ngay | Dùng `bash -c "nc -z localhost 2181 \|\| exit 1"` hoặc `/bin/bash -c ':'>/dev/tcp/localhost/2181` |
| MinIO healthcheck không có `curl` | container never healthy | Dùng `bash -c ':>/dev/tcp/127.0.0.1/9000' 2>/dev/null` |
| ClickHouse native port conflict MinIO | cả 2 cùng dùng 9000 | Map ClickHouse ra ngoài: `"9900:9000"` trong docker-compose |
| `docker-compose version` field | warning/error trên Docker mới | Bỏ hẳn `version:` field — không cần thiết từ Compose v2 |
| Multiline command trong docker-compose | syntax error | Dùng `>-` (block scalar) thay vì `|` hoặc list |
| kafka-init chạy trước khi Kafka ready | topic creation fails | `depends_on: kafka: condition: service_healthy` |
| Airflow FERNET_KEY rỗng | warning nhưng không crash | Generate bằng `python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"` |

### Phase 2: Data Generator

| Vấn đề | Biểu hiện | Fix |
|--------|-----------|-----|
| `confluent-kafka` là sync library | không native asyncio | Wrap `producer.poll(0)` sau mỗi `produce()`, không dùng `await` |
| `asyncio.Queue` full | `put()` block event loop | Dùng `put_nowait()` + try/except `QueueFull`, hoặc check `not queue.full()` trước |
| Faker `vi_VN` locale | một số faker method không support | Fallback về `Faker()` cho các field không cần locale VN |
| Pydantic v2 `model.dict()` deprecated | `AttributeError` | Dùng `model.model_dump()` thay thế |
| `uuid4()` object không JSON serializable | `TypeError` khi produce | Convert về `str` trong `to_kafka_dict()` |
| Rate control không chính xác | asyncio overhead + produce time > sleep interval | Dùng `time.monotonic()` để tính actual elapsed, adjust sleep accordingly |

### Phase 3: Spark Streaming

| Vấn đề | Biểu hiện | Fix |
|--------|-----------|-----|
| S3A jar compatibility | `ClassNotFoundException` hoặc version conflict | Phải dùng đúng bộ 3: `spark-sql-kafka-0-10_2.12:3.5.0` + `hadoop-aws:3.3.4` + `aws-java-sdk-bundle:1.12.262` |
| MinIO cần path-style access | `InvalidBucketName` hoặc 403 | Set `spark.hadoop.fs.s3a.path.style.access=true` — bắt buộc cho MinIO |
| `dropDuplicates` không có watermark column | Spark warning + state unbounded | `dropDuplicates(["order_id", "event_timestamp"])` — phải include `event_timestamp` |
| Checkpoint không persist qua restart | reprocess toàn bộ data từ đầu | Mount volume: `spark-checkpoints:/tmp/checkpoints` |
| Spark connect MinIO bằng localhost | `Connection refused` | Endpoint phải là `http://minio:9000` (Docker service name) |
| Items array không map được sang Parquet schema | `StructType` mismatch | Define `ArrayType(StructType([...]))` đúng trong schema registry |
| trigger 500ms nhưng latency **thực tế 8–18s** | S3A overhead per micro-batch — xem note chi tiết bên dưới | Chấp nhận cho cold path; xem Production Fix options bên dưới |

#### Deep-dive: Spark Streaming 8–18s Latency (S3A Small File Problem)

**Symptom:** `WARN ProcessingTimeExecutor: Current batch is falling behind. The trigger interval is 500 milliseconds, but spent 8000–18000 milliseconds`

**Root cause (3 layers):**

1. **`trigger(processingTime="500ms")` ≠ latency.** Trigger interval chỉ là *khoảng thời gian Spark cố gắng schedule một batch mới*. Nếu batch trước chưa xong, Spark đợi, không tạo batch mới. Wall-clock latency = thời gian thực tế xử lý 1 batch, không phải trigger interval.

2. **S3A small-file write overhead.** Ở throughput thấp (dev), mỗi 500ms batch chỉ có vài chục KB data → 1 Parquet file ~6–7 KB. Mỗi lần write S3A thực hiện:
   - HTTP PUT negotiation
   - Multipart upload setup (kể cả file nhỏ nếu `fs.s3a.multipart.threshold` thấp)
   - Commit protocol (rename + metadata update trên MinIO)
   - Tổng cộng: ~3–8s per file write, bất kể file lớn hay nhỏ
   
3. **3 Spark drivers cùng write S3A đồng thời** vào cùng 1 MinIO instance trên cùng 1 Docker host → MinIO CPU + I/O contention → mỗi write chậm hơn thêm 2–3x.

**Tại sao không fix ngay:**  
Đây không phải bug — đây là limitation của cold path. MinIO/Spark là *archival layer*, không phải real-time layer. Dashboard Grafana đọc từ ClickHouse Kafka Engine (latency vài giây), không phải từ MinIO. 8–18s là hoàn toàn chấp nhận được cho cold storage.

**Production fixes (khi cần thực sự):**

| Fix | Cách làm | Impact |
|-----|----------|--------|
| Tăng trigger interval lên 30s–60s | `trigger(processingTime="30 seconds")` | File lớn hơn (~500KB–2MB), S3A overhead amortized. Phù hợp cold path. |
| Bypass multipart cho file nhỏ | `spark.hadoop.fs.s3a.multipart.threshold=128M` | File < 128MB dùng single PUT, bỏ qua multipart negotiation overhead |
| Tăng S3A connection pool | `fs.s3a.connection.maximum=200` + `fs.s3a.fast.upload=true` | Giảm connection setup time khi 3 jobs viết đồng thời |
| Fast upload buffer | `fs.s3a.fast.upload.buffer=bytebuffer` + `fs.s3a.fast.upload.active.blocks=4` | Spark buffer trong RAM trước khi flush, giảm số HTTP round-trips |
| Tách MinIO ra host riêng | MinIO trên separate VM/node | Loại bỏ CPU/network contention với Spark trên cùng host |
| Dùng Delta Lake thay Parquet raw | `format("delta")` + auto-compaction | Delta Lake compact small files tự động; checkpoint + dedup tốt hơn |

**CV/Interview note:**  
> "500ms is the target trigger interval — how often Spark tries to start a new micro-batch. On single-machine dev, S3A write overhead per micro-batch is 8–18s due to small file I/O. This is a cold-path archival limitation, not the real-time latency. The actual real-time path (Kafka Engine → ClickHouse) has sub-10s end-to-end latency. On a production cluster with dedicated object storage and S3A tuning, the cold path would easily hit sub-second trigger intervals."

---

### Phase 4: ClickHouse + Kafka Engine

| Vấn đề | Biểu hiện | Fix |
|--------|-----------|-----|
| Malformed JSON từ Kafka | ClickHouse consumer bị stuck, không tiến | Thêm `kafka_skip_broken_messages = 10` trong Kafka Engine settings |
| Duplicate rows khi ClickHouse restart | data nhân đôi | Dùng `ReplacingMergeTree(_ingested_at)`, query với `FINAL` hoặc dedup logic |
| Materialized View không trigger | data vào kafka_queue nhưng không sang storage table | Check MV definition: `TO raw_orders_rt` phải đúng tên table, không phải view |
| Consumer group conflict với Spark | ClickHouse và Spark cùng consume một group | ClickHouse dùng `kafka_group_name = 'clickhouse-consumer-orders'`, Spark dùng group riêng |
| `items` JSON array không parse được | ClickHouse không support nested object trong JSONEachRow trực tiếp | Lưu `items String` — ClickHouse JSONEachRow sẽ lưu nguyên chuỗi JSON |
| Kafka Engine table không phải dùng để SELECT | Query trực tiếp kafka_queue_table = consume offset | Chỉ SELECT từ storage table (ReplacingMergeTree), không từ Kafka Engine table |
| ClickHouse init SQL chạy theo thứ tự | file 03 chạy trước file 01 | Đặt số prefix: `01_`, `02_`, `03_` — ClickHouse init chạy theo alphabetical order |

### Phase 5: dbt

| Vấn đề | Biểu hiện | Fix |
|--------|-----------|-----|
| `dbt-clickhouse` khác dbt standard | một số macro không work | Xem docs của `ClickHouse/dbt-clickhouse`, không assume tất cả dbt-core features có sẵn |
| ClickHouse không có `SERIAL`/`AUTOINCREMENT` | surrogate key phải generate khác | Dùng `generateUUIDv4()` hoặc `cityHash64(concat(...))` |
| `dbt run` tạo View mặc định | performance kém, không phù hợp ClickHouse | Set `materialized: table` hoặc `materialized: incremental` trong config |
| Incremental model với ClickHouse | merge không hoạt động như PostgreSQL | Dùng `unique_key` + `ReplacingMergeTree` hoặc `is_incremental()` macro |
| `profiles.yml` expose password | security issue | Dùng env var: `password: "{{ env_var('DBT_CH_PASSWORD') }}"` |
| dbt test `relationships` trên ClickHouse | foreign key không enforced | Test sẽ pass/fail dựa trên data, không phải constraint — chạy sau khi có đủ data |

### Phase 6: Observability

| Vấn đề | Biểu hiện | Fix |
|--------|-----------|-----|
| Kafka JMX Exporter config | metrics không xuất hiện trong Prometheus | Check port 9999 exposed trong docker-compose, kafka-exporter config file đúng path |
| Grafana datasource Prometheus URL | "Data source not found" | Dùng `http://prometheus:9090` (service name), không phải `localhost` |
| Spark metrics không có sẵn | Prometheus không scrape được Spark | Cần enable Spark PrometheusServlet: `spark.ui.prometheus.enabled=true` |
| ClickHouse exporter version mismatch | metrics schema khác | Pin version của `clickhouse-exporter` image |
| Grafana dashboard import | JSON format thay đổi giữa versions | Export từ chính Grafana đang chạy, không copy từ internet random |

---

## Known Limitations (Chấp nhận cho portfolio)

1. **Single Kafka broker** — production cần ít nhất 3 brokers cho HA. `replication-factor=1` = no redundancy.
2. **Spark Streaming dedup không perfect** — `dropDuplicates(["order_id", "event_timestamp"])` pass duplicate nếu timestamp lệch nhau (clock jitter giữa producer và Kafka).
3. **No schema registry** — Avro + Confluent Schema Registry là production standard nhưng add complexity cho portfolio project.
4. **ClickHouse ReplacingMergeTree dedup lazy** — dedup chỉ xảy ra khi merge (background) hoặc khi query với `FINAL`. Đây là design của ClickHouse, không phải bug.
5. **Airflow LocalExecutor** — production dùng CeleryExecutor hoặc KubernetesExecutor. LocalExecutor đủ cho single-machine demo.
6. **Generator asyncio rate không chính xác 100%** — asyncio overhead + produce time làm rate thực tế ~4800-5100 msg/min, không phải đúng 5000. Ghi thực tế vào CV.

---

## MinIO Role trong Kiến trúc v3

**Dùng để làm gì:**
- Cold storage: lưu Parquet files cho long-term retention (>24h Kafka retention)
- Replay: nếu ClickHouse bị corrupt, có thể re-ingest từ MinIO
- Audit: raw immutable data, không bị overwrite bởi dbt transformations

**Không dùng để làm gì (sau v3):**
- Không còn là nguồn load vào ClickHouse cho analytics
- Không được query trực tiếp cho Grafana

**Câu trả lời khi interviewer hỏi "tại sao vẫn giữ MinIO?":**
> "MinIO là cold path trong Lambda Architecture — lưu raw immutable data để replay hoặc reprocess khi schema thay đổi. ClickHouse Kafka Engine là hot path cho real-time queries. Hai layer phục vụ mục đích khác nhau: một cái cho tốc độ, một cái cho durability."
