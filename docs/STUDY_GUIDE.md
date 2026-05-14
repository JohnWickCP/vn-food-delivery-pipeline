# Study Guide — Vietnam Food Delivery Pipeline

> 3 phần:
> 1. Screenshot Checklist — khi nào chụp, đặt tên gì
> 2. Interview Questions — câu hỏi, không có đáp án (tự trả lời để luyện)
> 3. Vocabulary — thuật ngữ kỹ thuật cần thuộc

---

## Part 1: Screenshot Checklist

> Mục tiêu: Tạo bộ ảnh proof rằng pipeline chạy thật, metrics đo thật.
> Đặt tất cả ảnh vào `docs/screenshots/`.
> Tên ảnh theo format: `[số thứ tự]_[mô tả].png`

### Phase 1 — Infrastructure

| Khi nào chụp | Tên file | Nội dung cần thấy trong ảnh |
|-------------|----------|----------------------------|
| Sau `make up`, tất cả containers green | `01_all_containers_healthy.png` | `docker ps` output: STATUS = healthy hoặc running, tất cả services |
| Mở Kafka-UI lần đầu | `02_kafka_ui_topics.png` | http://localhost:8090 — 3 topics: raw.orders, raw.payments, raw.rider_events |
| MinIO Console sau bucket init | `03_minio_buckets.png` | http://localhost:9001 — bucket `food-delivery-lake` tồn tại |
| ClickHouse init SQL chạy xong | `04_clickhouse_tables.png` | Query: `SHOW TABLES FROM food_delivery` — thấy raw_orders, raw_payments, raw_rider_events |

### Phase 2 — Generator

| Khi nào chụp | Tên file | Nội dung cần thấy trong ảnh |
|-------------|----------|----------------------------|
| Generator đang chạy, throughput log xuất hiện | `05_generator_throughput_log.png` | Terminal log: "Throughput: XXXX msg/10s" — peak phải đạt 800+ msg/10s (=4800+/min) |
| Kafka-UI sau 2 phút generator chạy | `06_kafka_messages_flowing.png` | Topic raw.orders — Messages count tăng, có thể xem message JSON sample |
| Kafka-UI consumer groups | `07_kafka_consumer_groups.png` | Tab Consumer Groups — thấy consumer groups của Spark và ClickHouse |

### Phase 3 — Spark Streaming

| Khi nào chụp | Tên file | Nội dung cần thấy trong ảnh |
|-------------|----------|----------------------------|
| Spark Master UI sau khi workers kết nối | `08_spark_workers_connected.png` | http://localhost:8081 — 2 workers "ALIVE", total cores + memory |
| Spark Streaming query đang chạy | `09_spark_streaming_running.png` | Spark UI → Streaming tab — 3 queries: orders, payments, riders — Status: ACTIVE |
| **QUAN TRỌNG**: Spark processing latency | `10_spark_streaming_latency.png` | Spark UI → Streaming → Statistics: **Avg Processing Time < 500ms** — đây là CV metric |
| MinIO có Parquet files sau 5 phút | `11_minio_parquet_partitions.png` | MinIO Console → food-delivery-lake → raw/orders/ — thấy folder structure year=/month=/day=/hour= |

### Phase 4 — ClickHouse Kafka Engine

| Khi nào chụp | Tên file | Nội dung cần thấy trong ảnh |
|-------------|----------|----------------------------|
| Real-time data trong ClickHouse | `12_clickhouse_realtime_data.png` | `clickhouse-client` hoặc HTTP query: `SELECT count(), max(event_timestamp) FROM food_delivery.raw_orders_rt` — count tăng theo thời gian |
| **QUAN TRỌNG**: Query performance | `13_clickhouse_query_performance.png` | Terminal: query `SELECT city, count(), sum(total_vnd) FROM raw_orders_rt WHERE ... GROUP BY city` với `\timing` — **thấy < 100ms trên 5M rows** |
| Airflow DAGs list | `14_airflow_dags.png` | http://localhost:8080 — thấy 2 DAGs: dbt_run, monitor_kafka_lag |

### Phase 5 — dbt

| Khi nào chụp | Tên file | Nội dung cần thấy trong ảnh |
|-------------|----------|----------------------------|
| **QUAN TRỌNG**: dbt test 100% pass | `15_dbt_tests_pass.png` | Terminal: `dbt test` output — cuối cùng phải thấy "Completed successfully" + tổng số tests (≥30) và 0 failures |
| dbt docs lineage graph | `16_dbt_lineage_graph.png` | `dbt docs serve` → browser → DAG graph: thấy raw → stg → int → mart flow |
| Airflow DAG run success | `17_airflow_dbt_success.png` | Airflow UI → dbt_run DAG → Grid view: nhiều ô xanh liên tiếp |

### Phase 6 — Grafana

| Khi nào chụp | Tên file | Nội dung cần thấy trong ảnh |
|-------------|----------|----------------------------|
| Grafana Kafka dashboard | `18_grafana_kafka_dashboard.png` | http://localhost:3000 — Kafka Overview: messages/sec, consumer lag graph (lag ổn định thấp) |
| Grafana Spark dashboard | `19_grafana_spark_dashboard.png` | Spark metrics: processing time, batch size |
| **QUAN TRỌNG**: Business metrics dashboard | `20_grafana_business_metrics.png` | Orders/min chart (thấy peak hours 11-13h, 18-20h), revenue by city, real-time numbers |
| **FINAL**: Full stack running screenshot | `21_full_stack_final.png` | Chụp màn hình Grafana business dashboard đẹp nhất — đây là ảnh để README và CV |

### Đo Metrics cho CV (chụp terminal)

```bash
# Chạy sau khi pipeline chạy được ít nhất 24 giờ để có đủ data
make metrics   # hoặc bash scripts/measure_metrics.sh
```

| Metric | Tên file screenshot | Target |
|--------|--------------------|---------| 
| Kafka throughput (kafka-consumer-groups output) | `metrics_01_kafka_throughput.png` | 5000+ msg/min |
| ClickHouse row count + query time | `metrics_02_clickhouse_perf.png` | <100ms, 5M+ rows |
| MinIO storage size | `metrics_03_minio_storage.png` | ~20GB/30 days |
| dbt test results | `metrics_04_dbt_tests.png` | 100%, 30+ tests |
| Airflow success rate | `metrics_05_airflow_success.png` | ≥99% |

---

## Part 2: Interview Questions

> Không có đáp án ở đây. Tự trả lời bằng miệng, ghi âm, nghe lại.
> Mục tiêu: trả lời mỗi câu trong 60-90 giây, rõ ràng, không ấp úng.

### Architecture & Design Decisions

1. Tại sao dự án này có 2 paths từ Kafka — một vào ClickHouse, một vào MinIO qua Spark? Chúng phục vụ mục đích gì khác nhau?
2. Đây là Lambda Architecture hay Kappa Architecture? Phân biệt 2 kiểu này như thế nào?
3. Tại sao không ghi thẳng từ Spark Streaming vào ClickHouse mà phải qua MinIO?
4. Nếu ClickHouse down 2 giờ, data có bị mất không? Pipeline xử lý thế nào?
5. Nếu Spark Streaming down 6 giờ, data có bị mất không? Làm sao recover?
6. Tại sao Airflow không trigger/submit Spark Streaming job?
7. Airflow trong dự án này làm gì? Tại sao vẫn cần Airflow nếu Spark chạy liên tục?
8. Schema thay đổi (thêm field vào Order) — pipeline này cần làm gì để adapt?

### Kafka

9. Consumer group là gì? Tại sao Spark và ClickHouse Kafka Engine phải dùng consumer group khác nhau?
10. Partition có liên quan gì đến throughput? Tại sao dùng 3 partitions?
11. Kafka retention 24h có đủ không? Điều gì xảy ra nếu consumer lag >24h?
12. At-least-once vs exactly-once semantics — dự án này đang dùng cái nào ở mỗi bước?
13. Offset là gì? Spark track offset bằng cách nào?
14. Tại sao cần `KAFKA_ADVERTISED_LISTENERS` có 2 entries (INTERNAL và EXTERNAL)?

### PySpark Structured Streaming

15. Watermark là gì? Tại sao chọn 2 phút? Nếu đặt quá thấp thì sao, quá cao thì sao?
16. `dropDuplicates(["order_id", "event_timestamp"])` — tại sao phải include `event_timestamp`?
17. Checkpoint location dùng để làm gì khi Spark container restart?
18. `trigger(processingTime="500 milliseconds")` nghĩa là gì? Khác gì `trigger(once=True)`?
19. `failOnDataLoss=false` — trade-off là gì? Khi nào nên set `true`?
20. Micro-batch vs continuous processing — Spark Structured Streaming dùng cái nào?

### ClickHouse

21. MergeTree `ORDER BY` vs `PARTITION BY` — 2 cái này khác nhau thế nào? Ảnh hưởng gì đến performance?
22. Tại sao ORDER BY của raw_orders là `(city, placed_at, order_id)` mà không phải `(order_id)` hay `(placed_at)`?
23. `LowCardinality(String)` — ưu điểm là gì so với `String` thông thường? Khi nào dùng?
24. `ReplacingMergeTree` dedup hoạt động thế nào? `FINAL` keyword là gì?
25. ClickHouse Kafka Engine + Materialized View — giải thích flow data đi qua 3 objects như thế nào?
26. Tại sao ClickHouse native port phải map ra ngoài là 9900 thay vì 9000?
27. Tại sao dbt dùng HTTP port 8123 thay vì native port 9900?

### dbt

28. 3 layers staging / intermediate / mart — rule của mỗi layer là gì? Tại sao phải tách?
29. `source freshness` trong dbt là gì? Tại sao quan trọng?
30. dbt test types: `not_null`, `unique`, `accepted_values`, `relationships` — mỗi cái kiểm tra điều gì?
31. dbt lineage graph có tác dụng gì thực tế (ngoài việc trông đẹp)?
32. `dbt run` có idempotent không? Chạy 2 lần liên tiếp có ra kết quả khác không?
33. Incremental model trong dbt là gì? Tại sao dùng cho fact tables?

### Data Engineering General

34. Idempotency nghĩa là gì? Cho ví dụ về một bước trong pipeline này có hoặc không có idempotency.
35. Late data là gì? Watermark giải quyết late data như thế nào?
36. End-to-end latency trong dự án này là bao nhiêu? Đo bằng cách nào?
37. Columnar storage (ClickHouse) vs row storage (PostgreSQL) — khác nhau thế nào về I/O khi query analytical?
38. Parquet format có ưu điểm gì so với CSV hay JSON khi lưu trên object storage?
39. Làm sao đảm bảo data quality end-to-end trong pipeline này?
40. Nếu phải scale lên 10x throughput (50,000 orders/min), bottleneck đầu tiên sẽ ở đâu?

### Behavioral / Project-specific

41. Phần nào của dự án khó nhất khi implement? Tại sao?
42. Nếu làm lại từ đầu, bạn sẽ thay đổi gì?
43. Làm sao bạn biết pipeline đang healthy? Monitor gì?
44. Khác biệt giữa dự án này và production system thực tế ở công ty là gì?

---

### "Why did you choose X?" — Technology Justification

> Đây là nhóm câu hỏi interviewer hay hỏi nhất để kiểm tra xem bạn có thực sự hiểu stack hay chỉ copy tutorial.
> Trả lời theo format: lý do chọn X, X giải quyết vấn đề gì, và X kém hơn Y ở điểm nào (honest trade-off).

**Kafka**

45. Tại sao dùng Kafka thay vì RabbitMQ? RabbitMQ không có gì mà Kafka có?
46. Tại sao không dùng Redis Pub/Sub hoặc AWS SQS thay vì Kafka?
47. Kafka Log Retention 24h — tại sao không 7 ngày hoặc vĩnh viễn? Trade-off là gì?
48. Tại sao `acks=all` trong producer config thay vì `acks=1` (nhanh hơn)?
49. `linger.ms=5` và `batch.size=65536` — 2 config này ảnh hưởng gì đến throughput và latency?

**Parquet & MinIO**

50. Tại sao chọn Parquet thay vì CSV hoặc JSON để lưu trên MinIO?
51. Parquet vs ORC vs Avro — khi nào dùng cái nào? Dự án này chọn Parquet vì lý do gì cụ thể?
52. Tại sao lưu Parquet partitioned by `year/month/day` thay vì một file flat?
53. Tại sao dùng MinIO thay vì AWS S3 thật? Điều gì sẽ thay đổi khi migrate lên S3 thật?
54. MinIO trong dự án này có giá trị gì sau khi đã có ClickHouse Kafka Engine? Tại sao không xóa đi?

**ClickHouse**

55. Tại sao ClickHouse thay vì PostgreSQL cho analytical layer? PostgreSQL không đủ sao?
56. Tại sao ClickHouse thay vì DuckDB? DuckDB cũng là columnar và nhanh.
57. Tại sao không dùng BigQuery hay Redshift? Chúng không tốt hơn ClickHouse sao?
58. Tại sao `ReplacingMergeTree` thay vì plain `MergeTree` cho raw tables? Trade-off là gì?
59. Tại sao `ORDER BY (city, placed_at, order_id)` trong raw_orders? City là dimension thấp cardinality — điều đó có lợi gì?
60. Tại sao ClickHouse Kafka Engine thay vì viết custom consumer service (Python) để insert vào ClickHouse?

**PySpark**

61. Tại sao PySpark thay vì Apache Flink? Flink được coi là real-time hơn Spark.
62. Tại sao PySpark thay vì Kafka Streams hay ksqlDB?
63. Tại sao không dùng Python + Pandas để xử lý data từ Kafka? Khi nào Pandas là đủ, khi nào cần Spark?
64. Tại sao Structured Streaming thay vì Spark DStream (old API)? DStream có vấn đề gì?
65. Client mode vs Cluster mode trong Spark — dự án này dùng gì và tại sao?

**Airflow**

66. Tại sao dùng Airflow thay vì cronjob Linux thuần túy? `crontab -e` không đủ sao?
67. Tại sao Airflow thay vì Prefect hay Dagster? Chúng "modern" hơn Airflow.
68. LocalExecutor vs CeleryExecutor — dự án này dùng LocalExecutor, có vấn đề gì ở production?
69. Tại sao dbt_run DAG chạy lúc HH:05 thay vì HH:00 đúng giờ?

**dbt**

70. Tại sao dùng dbt thay vì viết SQL scripts thủ công và chạy bằng Python/Airflow?
71. dbt-clickhouse vs dbt-core với connection thủ công — khác nhau điểm nào?
72. Tại sao staging models là `materialized='view'` trong khi mart models là `materialized='table'`?
73. Tại sao không dùng `incremental` materialization cho `fct_orders`? Trade-off?
74. Tại sao cần `dbt test` riêng biệt sau `dbt run`? Không thể kết hợp?

**Python/Generator**

75. Tại sao dùng `asyncio` cho generator thay vì `threading` hoặc `multiprocessing`?
76. Tại sao Pydantic v2 thay vì Python `dataclasses` hoặc `TypedDict`?
77. Tại sao dùng `confluent-kafka` (C extension) thay vì `kafka-python` (pure Python)?
78. `confluent-kafka` là synchronous — tại sao không block event loop khi produce?
79. Tại sao `random.sample()` thay vì `random.choices()` cho order items?

**Docker & Infrastructure**

80. Tại sao Docker Compose thay vì Kubernetes cho project này?
81. Tại sao tách `docker-compose.monitoring.yml` riêng thay vì gộp vào file chính?
82. Tại sao Prometheus 2.45.6 cụ thể? Tại sao không dùng latest?
83. Tại sao `healthcheck` quan trọng trong Docker Compose? Không có thì sao?
84. `depends_on: condition: service_completed_successfully` vs `service_healthy` — khác gì nhau?

**General System Design**

85. Tại sao dự án cần cả Kafka lẫn Spark? Không thể dùng Kafka Streams thôi là đủ?
86. Tại sao dùng UUID làm primary key thay vì auto-increment integer?
87. Tại sao `DateTime64(3, 'UTC')` cho `event_timestamp` nhưng `DateTime64(3, 'Asia/Ho_Chi_Minh')` cho `placed_at`?
88. Tại sao Kafka partition count là 3? 1 partition có gì sai, 10 partitions có gì sai?
89. Consumer group `__airflow_monitor__` trong monitor_kafka_lag.py — tại sao đặt tên với dấu `__`?
90. Tại sao `kafka_skip_broken_messages = 10` trong Kafka Engine settings? Không skip thì sao?

---

## Part 3: Vocabulary

### Kafka

| Term | Nghĩa |
|------|-------|
| **Broker** | Server Kafka — nhận, lưu, và deliver messages. Một cluster có thể có nhiều brokers. |
| **Topic** | Kênh message — producers gửi vào, consumers đọc ra. Như một log file được phân tán. |
| **Partition** | Một topic được chia thành nhiều partitions. Mỗi partition là ordered log. Tăng partitions = tăng parallelism. |
| **Offset** | Vị trí của một message trong partition. Kafka không xóa message đã đọc — consumer tự track offset của mình. |
| **Consumer Group** | Một nhóm consumers cùng đọc một topic. Mỗi partition chỉ được đọc bởi 1 consumer trong group tại một thời điểm. |
| **Retention** | Thời gian Kafka giữ messages trước khi xóa (vd: 24h). Sau retention, message mất vĩnh viễn. |
| **Replication Factor** | Số bản sao của mỗi partition. RF=1 = không có redundancy. Production cần ≥3. |
| **ISR (In-Sync Replicas)** | Danh sách replicas đang sync đúng với leader. ISR < RF = một replica đang lag. |
| **Producer Acknowledgment (acks)** | `acks=0`: fire-and-forget. `acks=1`: leader confirm. `acks=all`: tất cả ISR confirm. |
| **At-least-once** | Message được xử lý ≥1 lần. Có thể duplicate nhưng không mất. |
| **Exactly-once** | Message được xử lý đúng 1 lần. Cần transactional producer + idempotent consumer. Phức tạp hơn nhiều. |
| **Consumer Lag** | Số messages trong topic mà consumer chưa đọc = `latest_offset - current_offset`. Lag cao = consumer chậm. |
| **Compaction** | Kafka chỉ giữ message mới nhất cho mỗi key. Dùng cho state storage, không phải event stream. |

### Spark Structured Streaming

| Term | Nghĩa |
|------|-------|
| **Structured Streaming** | Spark API xử lý data stream như một infinite DataFrame. Khác với DStream (RDD-based cũ). |
| **Micro-batch** | Processing mode mặc định: accumulate data trong N ms rồi process một batch. Khác với true streaming (per-record). |
| **Trigger** | Quyết định khi nào run một micro-batch. `processingTime="500ms"` = run mỗi 500ms. |
| **Watermark** | Ngưỡng thời gian để handle late data. `withWatermark("ts", "2 minutes")` = chờ tối đa 2 phút cho event trễ. |
| **Late Data** | Event đến sau thời điểm Spark expects (dựa trên event_timestamp). Network delay, retry, hoặc system clock drift. |
| **Checkpoint** | Spark ghi lại: (1) offsets đã đọc, (2) aggregation state. Dùng để resume đúng chỗ khi restart. |
| **State Store** | Bộ nhớ Spark dùng để track aggregation state (vd: dedup state, window state). Phải bound bằng watermark. |
| **dropDuplicates** | Dedup trong streaming. Phải include watermark column để Spark biết khi nào xóa state cũ. |
| **Output Mode** | `append`: chỉ ghi rows mới. `update`: ghi rows thay đổi. `complete`: ghi lại toàn bộ result mỗi batch. |
| **S3A** | Hadoop connector để đọc/ghi S3-compatible storage (MinIO). Cần jars: `hadoop-aws` + `aws-java-sdk-bundle`. |
| **failOnDataLoss** | Nếu `true`: Spark crash khi Kafka xóa offset mà Spark chưa đọc. `false`: skip và continue. |

### ClickHouse

| Term | Nghĩa |
|------|-------|
| **MergeTree** | Engine cơ bản nhất của ClickHouse. Append-only, sort data theo ORDER BY key, hỗ trợ partition. |
| **ReplacingMergeTree** | MergeTree + dedup: giữ row mới nhất cho mỗi ORDER BY key. Dedup chỉ xảy ra khi merge (background). |
| **ORDER BY (sort key)** | Primary key trong ClickHouse. Quyết định cách data được sắp xếp và index. Ảnh hưởng lớn đến query speed. |
| **PARTITION BY** | Chia data vào các folders theo giá trị (vd: tháng). Prune partitions khi query để skip data không cần. |
| **LowCardinality** | Wrapper type: nếu column có ít distinct values (<10,000), ClickHouse dùng dictionary encoding. Tiết kiệm storage 2-5x. |
| **Sparse Index** | ClickHouse không index từng row mà index mỗi 8192 rows (index_granularity). Rất hiệu quả cho range scans. |
| **Materialized View** | View tự động cập nhật khi có data mới INSERT vào source table. Dùng để pipeline data từ Kafka Engine sang MergeTree. |
| **Kafka Engine** | ClickHouse table engine đọc từ Kafka topic. Không lưu data, chỉ là consumer. Phải dùng kết hợp với Materialized View + MergeTree. |
| **FINAL** | Query modifier: force ClickHouse dedup trước khi trả kết quả. Chậm hơn nhưng kết quả chính xác với ReplacingMergeTree. |
| **Vectorized Execution** | ClickHouse xử lý data theo cột (column chunks) thay vì từng row. Tận dụng SIMD CPU instructions. Lý do ClickHouse nhanh hơn PostgreSQL cho analytical queries. |
| **index_granularity** | Số rows giữa 2 index entries trong sparse index. Default 8192. Giảm = index dày hơn = nhanh hơn point lookup nhưng tốn RAM hơn. |

### dbt

| Term | Nghĩa |
|------|-------|
| **Model** | Một file SQL trong dbt = một table hoặc view trong database. dbt compile + run SQL file đó. |
| **Staging (stg_)** | Layer đầu tiên: chỉ rename columns, cast types, filter NULL rows cơ bản. Không có business logic. |
| **Intermediate (int_)** | Layer giữa: join tables, calculate derived fields, apply business rules. Không expose ra ngoài. |
| **Mart (fct_, dim_, rpt_)** | Layer cuối: analytics-ready tables. fct = fact, dim = dimension, rpt = report/aggregate. |
| **Source** | Declaration trong YAML: "đây là raw table trong database, đây là freshness expectation". |
| **Freshness Check** | dbt kiểm tra xem source table có được update gần đây không. Alert nếu data cũ hơn threshold. |
| **Lineage Graph** | DAG (Directed Acyclic Graph) hiển thị dependency giữa các models. Thấy ngay model nào ảnh hưởng model nào. |
| **Materialization** | Cách dbt persist model: `view` (không lưu data), `table` (recreate mỗi run), `incremental` (chỉ xử lý rows mới). |
| **Incremental Model** | Model chỉ process new/changed data. Dùng `is_incremental()` macro + `unique_key` để merge. |
| **Generic Test** | Built-in tests: `not_null`, `unique`, `accepted_values`, `relationships`. Định nghĩa trong YAML. |
| **Singular Test** | Custom SQL test: viết query, nếu trả về rows = test fail. |
| **Ref** | `{{ ref('model_name') }}` — cách dbt reference model khác. dbt build dependency graph từ các ref() này. |

### Data Engineering Concepts

| Term | Nghĩa |
|------|-------|
| **Lambda Architecture** | Kiến trúc có 2 paths: batch layer (chính xác, chậm) và speed layer (gần real-time, có thể approximate). |
| **Kappa Architecture** | Đơn giản hóa Lambda: chỉ có 1 stream processing layer cho cả real-time lẫn historical. |
| **Idempotency** | Thao tác có thể thực hiện nhiều lần mà kết quả vẫn như thực hiện 1 lần. Ví dụ: `INSERT OR REPLACE`. |
| **Exactly-once Semantics** | Mỗi event được xử lý đúng 1 lần, không mất, không duplicate. Khó đạt được end-to-end. |
| **At-least-once Semantics** | Mỗi event được xử lý ít nhất 1 lần. Có thể duplicate. Phổ biến hơn, cần idempotent consumer để handle. |
| **Columnar Storage** | Lưu data theo cột, không theo hàng. Analytical query chỉ đọc cột cần thiết → I/O giảm đáng kể. |
| **Parquet** | Columnar file format với compression. Splittable (Spark có thể đọc song song). Hỗ trợ predicate pushdown. |
| **Predicate Pushdown** | Filter data ngay tại storage layer trước khi load vào memory. ClickHouse, Parquet, và nhiều engines hỗ trợ. |
| **Data Lineage** | Tracking data đi từ đâu đến đâu qua những transformation nào. dbt lineage graph là ví dụ. |
| **Schema Evolution** | Khả năng thay đổi schema (thêm/xóa/đổi tên column) mà không break downstream consumers. |
| **Backfilling** | Reprocess historical data sau khi có bug fix hoặc schema change. Cần raw data preserved (lý do giữ MinIO). |
| **Consumer Lag** | Xem Kafka → lag của consumer group. Metric quan trọng: lag tăng = consumer không bắt kịp producer. |
| **Hot Path** | Xử lý data real-time (Kafka → ClickHouse). Ưu tiên tốc độ, có thể approximate. |
| **Cold Path** | Xử lý data historical (Spark → MinIO). Ưu tiên chính xác và completeness. |
| **Watermark** | Trong streaming: threshold thời gian để handle late-arriving data. Ngoài watermark = data bị drop. |
| **SCD (Slowly Changing Dimension)** | Dimension data thay đổi theo thời gian (vd: restaurant đổi tên, rider đổi city). Cần chiến lược lưu history. |
| **OLTP vs OLAP** | OLTP: transactional, nhiều reads/writes nhỏ, row-based. OLAP: analytical, ít queries nhưng scan nhiều rows, columnar. |
| **Compaction** | ClickHouse: background merge của MergeTree parts. Kafka: giữ message mới nhất theo key. Hai nghĩa khác nhau. |
| **Dead Letter Queue (DLQ)** | Queue riêng để chứa messages không xử lý được (malformed, gây lỗi). Dự án này chưa implement — limitation. |
