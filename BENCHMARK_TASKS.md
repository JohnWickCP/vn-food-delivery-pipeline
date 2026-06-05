# Benchmark Session — vn-food-delivery-pipeline
<!-- Branch: main | Updated: 2026-06-05 -->

## Quyền và phạm vi
Toàn quyền bypass — không cần xin phép trước mỗi bước:
- `docker compose up/down/restart` bất kỳ service nào, bao nhiêu lần
- `docker compose down -v` (xóa volumes) khi cần fresh stack — ghi rõ trước khi làm
- Inject data trực tiếp, chạy benchmark bất kỳ bao lâu
- Commit sau mỗi task — không để Claude có tên trong commit message
- Session này được chạy lâu — không tóm tắt sớm

## Bối cảnh đã biết (đừng đo lại)
| Metric | Giá trị | Source |
|--------|---------|--------|
| ClickHouse query latency (261k rows) | 14–19ms | metrics_results.md |
| ClickHouse query latency (5M rows) | 88–103ms | perf_test.sql |
| Throughput sustained | ~1,740 orders/min avg | 261k/150min |
| dbt tests | 55/55 PASS | metrics_results.md |
| Spark trigger interval | 500ms configured, 7–9s actual | metrics_results.md |
| Kafka consumer lag | 0 msgs (orders/riders), 52 (payments) | metrics_results.md |
| Compression (raw_orders) | 2.4× (45 MiB / 110 MiB) | metrics_results.md |
| Prometheus targets | 7/7 UP | metrics_results.md |

## Chưa có — mục tiêu session này
- Hot path E2E latency P50/P95 (số thực, không ước tính)
- ClickHouse ingestion rate rows/sec
- ClickHouse compression ratio chính xác từ system.columns
- dbt run time (chỉ có test time ~4s, chưa có run time)
- Spark micro-batch stats (rows/batch, inputRate)
- Dedup ratio SQL proof
- Concurrent query benchmark N=1/5/10
- Failure recovery time (Kafka broker kill)
- Schema Registry + Avro toàn stack
- SparkSubmitOperator thay cho time-based assumption
- Incremental dbt model
- Data freshness SLA panel Grafana

---

## Phase 0 — Stack startup

```powershell
cd "d:\DE_Project\vn-food-delivery-pipeline"
docker compose up -d
```

Đợi 30 giây rồi verify:

```powershell
# Tất cả containers phải Up
docker compose ps

# Kiểm tra 3 topics tồn tại
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Kiểm tra ClickHouse nhận data
docker exec clickhouse clickhouse-client --port 9000 `
  --query "SELECT count() FROM food_delivery.raw_orders"

# Kiểm tra 7 Prometheus targets Up
# Mở http://localhost:9090/targets
```

**Chỉ tiếp tục khi:** tất cả containers Up và raw_orders có rows tăng dần.

---

## Phase 1 — Pure Measurements (không đổi code)

### P1-T01: ClickHouse Compression Ratio

```powershell
docker exec clickhouse clickhouse-client --port 9000 --query "
SELECT
    table,
    formatReadableSize(sum(data_compressed_bytes))   AS compressed,
    formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed,
    round(sum(data_uncompressed_bytes) / sum(data_compressed_bytes), 1) AS ratio
FROM system.columns
WHERE database IN ('food_delivery', 'food_delivery_dbt_marts')
GROUP BY table
ORDER BY ratio DESC
FORMAT PrettyCompact"
```

**Kết quả:**
| Table | Compressed | Uncompressed | Ratio |
|-------|-----------|--------------|-------|
| raw_orders | | | |
| raw_payments | | | |
| raw_rider_events | | | |
| fct_orders | | | |
| (others) | | | |

**Commit:** `perf(clickhouse): document MergeTree LZ4 compression ratios`

---

### P1-T02: ClickHouse Ingestion Rate (rows/sec)

Đảm bảo generator đang chạy. Đợi 3 phút sau khi pipeline stable, rồi:

```powershell
docker exec clickhouse clickhouse-client --port 9000 --query "
SELECT
    toStartOfMinute(event_time)                      AS minute,
    sumIf(written_rows, tables[1] = 'food_delivery.raw_orders')   AS orders_inserted,
    sumIf(written_rows, tables[1] = 'food_delivery.raw_payments') AS payments_inserted,
    sum(written_rows)                                AS total_inserted,
    round(sum(written_rows) / 60, 0)                 AS rows_per_sec
FROM system.query_log
WHERE query_kind = 'Insert'
  AND event_time >= now() - INTERVAL 10 MINUTE
  AND arrayExists(t -> t LIKE 'food_delivery.raw%', tables)
GROUP BY minute
ORDER BY minute
FORMAT PrettyCompact"
```

Chạy 3 lần cách nhau 1 phút, lấy average rows_per_sec.

**Kết quả:**
| Run | orders/min | payments/min | total rows/sec |
|-----|-----------|-------------|----------------|
| 1 | | | |
| 2 | | | |
| 3 | | | |
| **Average** | | | |

**Commit:** `perf(clickhouse): measure Kafka Engine insert rate rows/sec`

---

### P1-T03: dbt Full Pipeline Timing

```powershell
# Chạy trong Airflow container (đã có dbt cài sẵn)
docker exec airflow-webserver bash -c "
  cd /opt/airflow && \
  echo '=== dbt deps ===' && \
  time dbt deps --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt --quiet 2>&1 | tail -2 && \
  echo '=== dbt run ===' && \
  time dbt run --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt 2>&1 | tail -4 && \
  echo '=== dbt test ===' && \
  time dbt test --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt 2>&1 | tail -4
"
```

**Kết quả:**
| Step | Time |
|------|------|
| dbt deps | |
| dbt run (10 models) | |
| dbt test (55 tests) | |
| **Total** | |

**Commit:** `docs(dbt): add full pipeline run timing`

---

### P1-T04: Spark Micro-Batch Stats

Khi pipeline đang chạy, query Spark REST API cho 3 jobs:

```powershell
# orders job (port 4040)
$apps = Invoke-RestMethod "http://localhost:4040/api/v1/applications"
$appId = $apps[0].id
$stats = Invoke-RestMethod "http://localhost:4040/api/v1/applications/$appId/streaming/statistics"
$stats | ConvertTo-Json

# payments job (port 4041)
$apps2 = Invoke-RestMethod "http://localhost:4041/api/v1/applications"
$appId2 = $apps2[0].id
Invoke-RestMethod "http://localhost:4041/api/v1/applications/$appId2/streaming/statistics" | ConvertTo-Json

# rider_events job (port 4042)
$apps3 = Invoke-RestMethod "http://localhost:4042/api/v1/applications"
$appId3 = $apps3[0].id
Invoke-RestMethod "http://localhost:4042/api/v1/applications/$appId3/streaming/statistics" | ConvertTo-Json
```

**Kết quả:**
| Job | avgInputRate (rows/s) | avgProcessingRate (rows/s) | batchDuration (ms) |
|-----|-----------------------|---------------------------|--------------------|
| stream-orders | | | |
| stream-payments | | | |
| stream-rider-events | | | |

**Commit:** `docs(spark): document micro-batch throughput and batch duration`

---

### P1-T05: Dedup Efficiency + FINAL Overhead

```powershell
# Dedup ratio
docker exec clickhouse clickhouse-client --port 9000 --query "
SELECT
    count()                     AS raw_rows,
    uniqExact(order_id)         AS unique_orders,
    raw_rows - unique_orders    AS duplicates,
    round(duplicates / raw_rows * 100, 1) AS dup_pct
FROM food_delivery.raw_orders
FORMAT PrettyCompact"

# FINAL vs non-FINAL latency comparison
docker exec clickhouse clickhouse-client --port 9000 --query "
SELECT 'without_FINAL' AS mode, count() AS rows, elapsed FROM (
    SELECT count() AS rows FROM food_delivery.raw_orders
) AS t CROSS JOIN (SELECT elapsed FROM system.query_log
    WHERE query LIKE '%raw_orders%' AND query NOT LIKE '%FINAL%'
    ORDER BY event_time DESC LIMIT 1) AS e
UNION ALL
SELECT 'with_FINAL' AS mode, count() AS rows, elapsed FROM (
    SELECT count() AS rows FROM food_delivery.raw_orders FINAL
) AS t CROSS JOIN (SELECT elapsed FROM system.query_log
    WHERE query LIKE '%raw_orders FINAL%'
    ORDER BY event_time DESC LIMIT 1) AS e
FORMAT PrettyCompact"
```

Nếu query phức tạp trên không chạy được, dùng cách đơn giản: chạy 2 query riêng và note thời gian thủ công.

**Kết quả:**
| Metric | Giá trị |
|--------|---------|
| Total raw rows | |
| Unique orders | |
| Duplicates | |
| Dup % | |
| Query time WITHOUT FINAL | ms |
| Query time WITH FINAL | ms |
| FINAL overhead | × slower |

**Commit:** `docs(metrics): add deduplication efficiency and FINAL overhead stats`

---

## Phase 2 — Hot Path E2E Latency

**⚠️ Cần fresh stack:** `docker compose down -v && docker compose up -d`

### P2-T06: Thêm producer_ts, đo P50/P95

**Bước 1 — Thêm producer_ts vào 3 schema files**

`generator/schemas/order.py` — thêm vào class Order:
```python
import time
# Thêm field này vào cuối danh sách fields, trước to_kafka_dict:
producer_ts: float = Field(default_factory=time.time)
```

`generator/schemas/payment.py` — thêm vào class Payment:
```python
import time
producer_ts: float = Field(default_factory=time.time)
```

`generator/schemas/rider_event.py` — thêm vào class RiderEvent:
```python
import time
producer_ts: float = Field(default_factory=time.time)
```

**Bước 2 — Thêm producer_ts vào raw tables** (`clickhouse/init/02_create_raw_tables.sql`)

Trong `raw_orders`: thêm sau dòng `_ingested_at`:
```sql
producer_ts   Float64 DEFAULT 0
```
Làm tương tự cho `raw_payments` và `raw_rider_events`.

**Bước 3 — Thêm producer_ts vào Kafka queue tables và MVs** (`clickhouse/init/04_create_kafka_engine.sql`)

Trong `kafka_orders_queue`: thêm `producer_ts Float64` vào column list.

Trong `orders_mv` SELECT: thêm `producer_ts` (passthrough, không cần cast).

Làm tương tự cho payments và rider_events.

**Bước 4 — Fresh stack và measure**

```powershell
docker compose down -v
docker compose up -d
# Đợi 2 phút để pipeline stable
Start-Sleep 120

# Verify producer_ts đang flow vào ClickHouse
docker exec clickhouse clickhouse-client --port 9000 --query "
SELECT order_id, producer_ts, _ingested_at,
       round(toUnixTimestamp64Milli(_ingested_at)/1000 - producer_ts, 2) AS ingestion_sec
FROM food_delivery.raw_orders
WHERE producer_ts > 0
ORDER BY _ingested_at DESC
LIMIT 5
FORMAT PrettyCompact"
```

**Bước 5 — Đo latency (chạy 5 lần, mỗi lần cách nhau 2 phút)**

```powershell
docker exec clickhouse clickhouse-client --port 9000 --query "
SELECT
    round(quantile(0.50)(now_ts - producer_ts), 2) AS p50_sec,
    round(quantile(0.95)(now_ts - producer_ts), 2) AS p95_sec,
    round(min(now_ts - producer_ts), 2)            AS min_sec,
    round(max(now_ts - producer_ts), 2)            AS max_sec,
    count()                                         AS samples
FROM (
    SELECT producer_ts,
           toUnixTimestamp64Milli(_ingested_at) / 1000.0 AS now_ts
    FROM food_delivery.raw_orders
    WHERE producer_ts > 0
    ORDER BY _ingested_at DESC
    LIMIT 500
)
FORMAT PrettyCompact"
```

**Kết quả (5 runs):**
| Run | P50 (s) | P95 (s) | Min (s) | Max (s) | Samples |
|-----|---------|---------|---------|---------|---------|
| 1 | | | | | |
| 2 | | | | | |
| 3 | | | | | |
| 4 | | | | | |
| 5 | | | | | |
| **Final** | | | | | |

**Commits:**
1. `feat(generator): add producer_ts field for E2E latency measurement`
2. `feat(clickhouse): add producer_ts column to raw tables and Kafka Engine MVs`
3. `docs(metrics): record hot path E2E latency P50/P95`

---

## Phase 3 — Additional Benchmarks

### P3-T07: Concurrent Query Benchmark

Tạo `clickhouse/bench_concurrent.py`:

```python
import concurrent.futures
import time
from clickhouse_driver import Client

QUERY = """
    SELECT city, toStartOfHour(placed_at) AS h, count(), sum(total_vnd)
    FROM food_delivery.raw_orders
    WHERE placed_at >= now() - INTERVAL 7 DAY
    GROUP BY city, h ORDER BY h DESC
"""

def run_query(_):
    client = Client("localhost", port=9900)
    t0 = time.perf_counter()
    client.execute(QUERY)
    return (time.perf_counter() - t0) * 1000  # ms

print(f"{'N':>4}  {'P50 (ms)':>10}  {'P95 (ms)':>10}  {'Max (ms)':>10}")
for n in [1, 5, 10]:
    results = []
    for _ in range(3):  # 3 rounds
        with concurrent.futures.ThreadPoolExecutor(max_workers=n) as ex:
            times = list(ex.map(run_query, range(n)))
        results.extend(times)
    results.sort()
    p50 = results[len(results) // 2]
    p95 = results[int(len(results) * 0.95)]
    print(f"{n:>4}  {p50:>10.0f}  {p95:>10.0f}  {max(results):>10.0f}")
```

```powershell
# Cài clickhouse-driver nếu chưa có
pip install clickhouse-driver

python clickhouse/bench_concurrent.py
```

**Kết quả:**
| N concurrent | P50 (ms) | P95 (ms) | Max (ms) |
|-------------|----------|----------|---------|
| 1 | | | |
| 5 | | | |
| 10 | | | |

**Commit:** `perf(clickhouse): concurrent query benchmark N=1,5,10`

---

### P3-T08: Kafka Broker Failure Recovery

```powershell
# Bước 1: Ghi baseline lag và offset
docker exec kafka kafka-consumer-groups `
  --bootstrap-server localhost:9092 `
  --describe --group clickhouse-consumer-orders 2>$null

# Bước 2: Ghi timestamp kill
$kill_time = Get-Date
Write-Host "Killing kafka at $kill_time"
docker compose stop kafka

# Bước 3: Quan sát trong 30s — Spark logs sẽ show reconnect attempts
Start-Sleep 5
docker logs spark-streaming-orders --tail 20
Start-Sleep 10
docker logs spark-streaming-orders --tail 20

# Bước 4: Restart kafka
$restart_time = Get-Date
docker compose start kafka
Write-Host "Kafka restarted at $restart_time"

# Bước 5: Monitor recovery — đợi đến khi Spark logs thấy "Assigned partitions"
$recovered = $false
$t0 = Get-Date
while (-not $recovered) {
    $logs = docker logs spark-streaming-orders --tail 5 2>&1
    if ($logs -match "Assigned|resumed|Started") {
        $recovered = $true
        $recovery_sec = [int]((Get-Date) - $restart_time).TotalSeconds
        Write-Host "Spark recovered in ${recovery_sec}s"
    }
    Start-Sleep 5
}

# Bước 6: Kiểm tra messages lost
docker exec kafka kafka-consumer-groups `
  --bootstrap-server localhost:9092 `
  --describe --group clickhouse-consumer-orders 2>$null

# Bước 7: Kiểm tra ClickHouse tiếp tục nhận data
docker exec clickhouse clickhouse-client --port 9000 `
  --query "SELECT count() FROM food_delivery.raw_orders"
```

**Kết quả:**
| Metric | Giá trị |
|--------|---------|
| Kill time | |
| Restart time | |
| Spark recovery time | s |
| ClickHouse recovery time | s |
| Messages lost (RF=1 expected) | |
| Checkpoint honored? | Y/N |

**Expected finding:** RF=1 → một số messages in-flight bị mất khi broker kill; Spark resume từ checkpoint khi broker lên lại.

**Commit:** `test(reliability): Kafka broker failure recovery — document RF=1 behavior`

---

### P3-T09: Cold Path Exact Latency (foreachBatch timing)

Sửa `spark/jobs/stream_orders.py` — thêm foreachBatch timing thay vì writeStream trực tiếp:

```python
import time
import logging

logger = logging.getLogger(__name__)

def write_batch_with_timing(batch_df, epoch_id):
    if batch_df.isEmpty():
        return
    row_count = batch_df.count()
    t0 = time.perf_counter()
    (batch_df.write
        .format("parquet")
        .option("path", OUTPUT_PATH)
        .partitionBy("year", "month", "day")
        .mode("append")
        .save())
    elapsed = time.perf_counter() - t0
    logger.warning(
        "BATCH_TIMING epoch=%d rows=%d write_sec=%.3f rows_per_sec=%.0f",
        epoch_id, row_count, elapsed, row_count / elapsed if elapsed > 0 else 0
    )

# Thay .writeStream...start() bằng:
(orders.writeStream
    .foreachBatch(write_batch_with_timing)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime="500 milliseconds")
    .outputMode("append")
    .start()
    .awaitTermination())
```

Chạy 5 phút, extract timing:

```powershell
docker logs spark-streaming-orders 2>&1 | Select-String "BATCH_TIMING" | Select-Object -Last 20
```

**Kết quả:**
| Metric | Giá trị |
|--------|---------|
| Avg rows/batch | |
| Avg write time (s) | |
| P50 write time (s) | |
| P95 write time (s) | |
| Min write time (s) | |
| Max write time (s) | |
| Avg rows/sec to MinIO | |

**Commit:** `perf(spark): instrument foreachBatch timing for cold path latency`

---

## Phase 4 — Schema Registry + Avro

**⚠️ Cần fresh stack sau khi xong code changes:**
`docker compose down -v && docker compose up -d`

### P4-T10: Confluent Schema Registry + Avro full stack

**Bước 1 — Thêm schema-registry vào docker-compose.yml**

Thêm service mới sau `kafka-init`:
```yaml
  schema-registry:
    image: confluentinc/cp-schema-registry:7.5.0
    container_name: schema-registry
    depends_on:
      - kafka
    ports:
      - "8082:8081"
    environment:
      SCHEMA_REGISTRY_HOST_NAME: schema-registry
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: "kafka:29092"
      SCHEMA_REGISTRY_LISTENERS: "http://0.0.0.0:8081"
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8081/subjects"]
      interval: 10s
      timeout: 5s
      retries: 10
```

Thêm `SCHEMA_REGISTRY_URL: http://schema-registry:8081` vào env của `generator`.

**Bước 2 — Tạo Avro schema files**

`generator/schemas/avro/order.avsc`:
```json
{
  "type": "record",
  "name": "Order",
  "namespace": "vn.fooddelivery",
  "fields": [
    {"name": "order_id",         "type": "string"},
    {"name": "customer_id",      "type": "string"},
    {"name": "restaurant_id",    "type": "string"},
    {"name": "rider_id",         "type": "string"},
    {"name": "city",             "type": "string"},
    {"name": "district",         "type": "string"},
    {"name": "status",           "type": "string"},
    {"name": "items",            "type": "string"},
    {"name": "subtotal_vnd",     "type": "long"},
    {"name": "delivery_fee_vnd", "type": "long"},
    {"name": "discount_vnd",     "type": "long"},
    {"name": "total_vnd",        "type": "long"},
    {"name": "payment_method",   "type": "string"},
    {"name": "platform",         "type": "string"},
    {"name": "placed_at",        "type": "string"},
    {"name": "event_timestamp",  "type": "string"},
    {"name": "producer_ts",      "type": "double"}
  ]
}
```

Tạo tương tự `payment.avsc` và `rider_event.avsc` theo đúng fields trong Pydantic models.

**Bước 3 — Cập nhật generator/requirements.txt**

```
confluent-kafka[avro]>=2.3.0
fastavro>=1.9.0
```

**Bước 4 — Refactor base_producer.py để dùng AvroProducer**

Thay `Producer` bằng `SerializingProducer` + `AvroSerializer`:
```python
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka import SerializingProducer
import json, os

SR_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")

class BaseProducer(ABC):
    def __init__(self, avsc_path: str) -> None:
        sr_client = SchemaRegistryClient({"url": SR_URL})
        with open(avsc_path) as f:
            schema_str = f.read()
        avro_serializer = AvroSerializer(sr_client, schema_str,
                                         lambda obj, ctx: obj)
        producer_config = {**config.KAFKA_PRODUCER_CONFIG,
                           "key.serializer":   StringSerializer("utf_8"),
                           "value.serializer": avro_serializer}
        # Remove JSON-only keys incompatible with SerializingProducer
        producer_config.pop("compression.type", None)
        self.producer = SerializingProducer(producer_config)
        ...
```

Cập nhật `OrderProducer.__init__` để pass đường dẫn đến `order.avsc`.

**Bước 5 — Cập nhật ClickHouse Kafka Engine** (`clickhouse/init/04_create_kafka_engine.sql`)

Thay `kafka_format = 'JSONEachRow'` bằng:
```sql
kafka_format                         = 'AvroConfluent',
format_avro_schema_registry_url      = 'http://schema-registry:8081'
```

Xóa `input_format_json_read_arrays_as_strings = 1` (không áp dụng cho Avro).

**Bước 6 — Cập nhật Spark streaming jobs**

Trong `stream_orders.py`, thêm Avro deserialization:
```python
from pyspark.sql.avro.functions import from_avro

# Đọc Avro schema từ Registry tại runtime
import urllib.request
schema_id = 1  # orders schema ID, verify sau khi register
sr_schema = urllib.request.urlopen(
    f"http://schema-registry:8082/schemas/ids/{schema_id}"
).read().decode()

# Thay from_json bằng from_avro (bỏ 5 bytes Confluent magic header)
raw = (spark.readStream
    .format("kafka")
    ...
    .load()
    .select(from_avro(
        expr("substring(value, 6)"),  # skip magic byte + schema ID (5 bytes)
        sr_schema
    ).alias("d"))
    .select("d.*")
)
```

**Bước 7 — Verify sau fresh stack**

```powershell
# Fresh stack
docker compose down -v
docker compose up -d
Start-Sleep 60

# Kiểm tra schema registered
Invoke-RestMethod "http://localhost:8082/subjects"
# Expected: ["raw.orders-value", "raw.payments-value", "raw.rider_events-value"]

# Kiểm tra ClickHouse nhận Avro
docker exec clickhouse clickhouse-client --port 9000 `
  --query "SELECT count() FROM food_delivery.raw_orders"
# Chạy lại sau 30s và 60s — số phải tăng

# Kiểm tra Spark viết Parquet
docker exec minio mc ls minio/food-delivery-lake/raw/orders/ 2>$null
```

**Monitoring trong lúc setup:**
```powershell
# Chạy loop này trong terminal riêng
while ($true) {
    $ts = Get-Date -Format "HH:mm:ss"
    $sr = try { (Invoke-RestMethod "http://localhost:8082/subjects").Count } catch { "DOWN" }
    $ch = docker exec clickhouse clickhouse-client --port 9000 `
          --query "SELECT count() FROM food_delivery.raw_orders" 2>$null
    Write-Host "$ts | schema-registry subjects=$sr | clickhouse raw_orders=$ch"
    Start-Sleep 15
}
```

**Kết quả:**
| Metric | Giá trị |
|--------|---------|
| Schemas registered | 3 (orders, payments, rider_events) |
| ClickHouse ingesting Avro | Y/N |
| Spark reading Avro | Y/N |
| Any schema evolution error | |

**Commits:**
1. `feat(docker-compose): add Confluent Schema Registry on port 8082`
2. `feat(generator): migrate to Avro serialization with Schema Registry`
3. `feat(clickhouse): update Kafka Engine tables to AvroConfluent format`
4. `feat(spark): update streaming jobs to deserialize Avro via Schema Registry`

---

## Phase 5 — SparkSubmitOperator

### P5-T11: Thay time-based assumption bằng real dependency

**Bước 1 — Thêm provider vào `airflow/requirements.txt`**

```
apache-airflow-providers-apache-spark>=4.1.0
```

**Bước 2 — Thêm Spark connection vào docker-compose.yml** (airflow-webserver env):

```yaml
AIRFLOW_CONN_SPARK_DEFAULT: "spark://spark-master:7077"
```

**Bước 3 — Rewrite `airflow/dags/batch_daily_summary.py`**

Giữ nguyên `check_spark_output` và `load_to_clickhouse` tasks. Thêm SparkSubmitOperator ở đầu chain:

```python
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

run_spark_batch = SparkSubmitOperator(
    task_id="run_spark_batch",
    application="/opt/spark/jobs/batch_daily_summary.py",
    conn_id="spark_default",
    application_args=["--date", "{{ ds }}"],
    conf={
        "spark.hadoop.fs.s3a.endpoint":            "http://minio:9000",
        "spark.hadoop.fs.s3a.access.key":          "minioadmin",
        "spark.hadoop.fs.s3a.secret.key":          "minioadmin",
        "spark.hadoop.fs.s3a.path.style.access":   "true",
        "spark.hadoop.fs.s3a.impl":
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    },
    dag=dag,
)

run_spark_batch >> check_spark_output() >> load_to_clickhouse()
```

Đổi schedule từ `"0 2 * * *"` thành `"0 1 * * *"` (Spark chạy đến xong mới trigger check).

**Bước 4 — Rebuild Airflow và verify**

```powershell
docker compose build airflow-webserver airflow-scheduler
docker compose up -d airflow-webserver airflow-scheduler

# Verify DAG load
docker exec airflow-webserver airflow dags list 2>$null | Select-String "batch"

# Trigger manual test run
docker exec airflow-webserver airflow dags trigger batch_daily_summary 2>$null

# Monitor task states
docker exec airflow-webserver airflow tasks states-for-dag-run `
  batch_daily_summary `
  $(docker exec airflow-webserver airflow dags list-runs -d batch_daily_summary --output json 2>$null `
    | ConvertFrom-Json | Select-Object -First 1 -ExpandProperty run_id) 2>$null
```

**Kết quả:**
| Task | Status |
|------|--------|
| run_spark_batch | success/failed |
| check_spark_output | success/failed |
| load_to_clickhouse | success/failed |

**Commits:**
1. `feat(airflow): add SparkSubmitOperator to batch DAG for real dependency tracking`
2. `feat(airflow): shift batch DAG schedule to 1 AM with chained task execution`

---

## Phase 6 — dbt + Observability

### P6-T12: Incremental dbt model (fct_orders)

Sửa `dbt/models/marts/fct_orders.sql` — thêm config block ở đầu:

```sql
{{ config(
    materialized  = 'incremental',
    engine        = 'ReplacingMergeTree()',
    order_by      = '(city, placed_date, order_id)',
    unique_key    = 'order_id',
    incremental_strategy = 'delete+insert',
    on_schema_change     = 'append_new_columns'
) }}

-- ... existing SELECT ...

{% if is_incremental() %}
WHERE placed_at >= (
    SELECT max(placed_at) FROM {{ this }}
) - toIntervalMinute(10)
{% endif %}
```

10 phút lookback = khớp Spark watermark.

```powershell
# Test full-refresh
docker exec airflow-webserver bash -c "
  cd /opt/airflow && \
  time dbt run --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt \
    --select fct_orders --full-refresh 2>&1 | tail -5"

# Test incremental (chạy lần 2, không full-refresh)
docker exec airflow-webserver bash -c "
  cd /opt/airflow && \
  time dbt run --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt \
    --select fct_orders 2>&1 | tail -5"
```

**Kết quả:**
| Mode | Time | Rows processed |
|------|------|----------------|
| Full refresh | s | |
| Incremental | s | |
| Speedup | | × faster |

**Commit:** `feat(dbt): incremental materialization for fct_orders with 10min late-event lookback`

---

### P6-T13: Data Freshness SLA Panel (Grafana)

Thêm panel mới vào `monitoring/grafana/dashboards/food_delivery_infra.json`.

Panel spec (thêm vào mảng `panels[]`):
```json
{
  "id": 99,
  "title": "Data Freshness (seconds since last order)",
  "type": "stat",
  "datasource": "ClickHouse",
  "targets": [{
    "rawSql": "SELECT toUnixTimestamp(now()) - max(toUnixTimestamp(placed_at)) AS seconds_stale FROM food_delivery.raw_orders",
    "format": "table"
  }],
  "thresholds": {
    "mode": "absolute",
    "steps": [
      {"color": "green",  "value": null},
      {"color": "yellow", "value": 60},
      {"color": "red",    "value": 300}
    ]
  },
  "unit": "s"
}
```

```powershell
# Reload dashboard trong Grafana
Invoke-RestMethod -Method Post `
  -Uri "http://localhost:3000/api/admin/provisioning/dashboards/reload" `
  -Headers @{ Authorization = "Basic " + [Convert]::ToBase64String(
      [Text.Encoding]::ASCII.GetBytes("admin:admin")) }

# Verify panel visible at:
# http://localhost:3000 → food_delivery_infra dashboard
```

**Commit:** `feat(monitoring): add data freshness SLA panel with 60s warn / 300s critical`

---

### P6-T14: Airflow Failure Email Alerting

Cập nhật `default_args` trong cả 3 DAG files (`dbt_run.py`, `monitor_kafka_lag.py`, `batch_daily_summary.py`):

```python
default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": True,
    "email_on_retry": False,
    "email": ["caophon9ats2018@gmail.com"],
}
```

Thêm SMTP config vào docker-compose.yml (airflow-webserver + airflow-scheduler env):

```yaml
AIRFLOW__EMAIL__EMAIL_BACKEND: "airflow.utils.email.send_email_smtp"
AIRFLOW__SMTP__SMTP_HOST: "smtp.gmail.com"
AIRFLOW__SMTP__SMTP_STARTTLS: "True"
AIRFLOW__SMTP__SMTP_SSL: "False"
AIRFLOW__SMTP__SMTP_PORT: "587"
AIRFLOW__SMTP__SMTP_USER: "${SMTP_USER}"
AIRFLOW__SMTP__SMTP_PASSWORD: "${SMTP_PASSWORD}"
AIRFLOW__SMTP__SMTP_MAIL_FROM: "${SMTP_USER}"
```

Thêm `SMTP_USER` và `SMTP_PASSWORD` vào `.env` (không commit).

Test: trigger failure thủ công:

```powershell
# Tạm thời inject lag > threshold để monitor_kafka_lag fail
# Hoặc đơn giản hơn: trigger DAG với task fail
docker exec airflow-webserver airflow tasks test `
  monitor_kafka_lag check_kafka_health 2024-01-01 2>$null
```

**Commit:** `feat(airflow): configure email alerting on DAG failure for all 3 DAGs`

---

## Phase 7 — Documentation

### P7-T15: Update metrics_results.md

Ghi tất cả số mới từ T01–T14 vào `docs/metrics_results.md`.
Cập nhật bảng Summary for CV ở cuối file.

**Commit:** `docs(metrics): update with benchmark session results`

### P7-T16: CV Bullets

Tạo `docs/cv_bullets.md` với toàn bộ CV bullets bằng tiếng Anh.
Mỗi số phải map về 1 task result cụ thể.

**Commit:** `docs: add final CV bullets with all measured metrics`

---

## Cleanup sau mỗi phase

```powershell
# Sau Phase 2 (trước Phase 4):
docker compose down -v

# Sau Phase 4 (trước Phase 5):
docker compose down -v && docker compose up -d

# Sau toàn bộ session:
docker compose down  # giữ volumes nếu muốn giữ data
# HOẶC
docker compose down -v  # clean slate
```

---

## Câu hỏi cần trả lời cuối session

1. Hot path E2E latency P50/P95 là bao nhiêu giây? (T06)
2. ClickHouse insert rate: bao nhiêu rows/sec khi pipeline sustained? (T02)
3. Concurrent queries: N=10 P95 có dưới 200ms không? (T07)
4. Kafka failure recovery: Spark mất bao lâu để resume? Mất bao nhiêu messages? (T08)
5. Schema Registry: cả 3 schemas registered, ClickHouse AvroConfluent hoạt động? (T10)
6. SparkSubmitOperator: DAG chain chạy end-to-end thành công? (T11)
7. FINAL overhead: query với FINAL chậm hơn bao nhiêu lần? (T05)

---

## Sau session — ghi vào memory

- Hot path E2E latency P50/P95
- ClickHouse insert rows/sec
- Concurrent query P95 at N=10
- Kafka recovery time + message loss
- Schema Registry working: Y/N + port used
- Bất kỳ bug mới phát hiện trong quá trình chạy

---

## Port reference
| Service | External | Internal |
|---------|----------|----------|
| Kafka | 9092 | 29092 |
| Kafka UI | 8090 | - |
| Schema Registry | 8082 | 8081 |
| ClickHouse HTTP | 8123 | - |
| ClickHouse native | 9900 | 9000 |
| Spark master UI | 8081 | - |
| Spark jobs UI | 4040/4041/4042 | - |
| Airflow | 8080 | - |
| MinIO API | 9000 | - |
| MinIO Console | 9001 | - |
| Grafana | 3000 | - |
| Prometheus | 9090 | - |
