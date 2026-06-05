# Autonomous Benchmark Session — vn-food-delivery-pipeline
<!-- Last updated: 2026-06-05 | Branch: feat/benchmarks -->

## Session Rules
- Docker start/stop freely — `docker compose up -d`, `docker compose down`, `docker compose down -v` đều OK
- Fresh stack (volumes wiped) required before T06, T10, T11: run `docker compose down -v && docker compose up -d` at those tasks
- No questions during execution — make judgment calls, document decisions in Result field
- Every task ends with a git commit on `feat/benchmarks`
- Real numbers only — never estimate, always measure

## Prerequisites Check
```powershell
docker compose ps          # all services Up
git branch                 # should show feat/benchmarks
```

## Task Status Legend
- `[ ]` pending  |  `[~]` in_progress  |  `[x]` done  |  `[!]` blocked

---

## TIER 1 — Pure Measurement (no code change, ~1.5h)

### T01 — ClickHouse Compression Ratio
- **Status:** `[ ]`
- **Why:** Already have raw numbers (raw_orders 2.4×) but need exact figures from system table — more defensible in interview
- **Approach:**
  ```sql
  SELECT table,
      formatReadableSize(sum(data_compressed_bytes))   AS compressed,
      formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed,
      round(sum(data_uncompressed_bytes) / sum(data_compressed_bytes), 1) AS ratio
  FROM system.columns
  WHERE database IN ('food_delivery', 'food_delivery_dbt_marts')
  GROUP BY table ORDER BY ratio DESC;
  ```
  Run via: `docker exec -it clickhouse clickhouse-client --query "..."`
- **Success:** Table with ratio per table, ratio >= 1.5× for raw_orders
- **Commit:** `perf(clickhouse): document MergeTree LZ4 compression ratios per table`
- **Result:** _fill during execution_

---

### T02 — ClickHouse Kafka Engine Ingestion Rate
- **Status:** `[ ]`
- **Why:** rows/sec metric not currently in metrics_results.md — strong CV claim
- **Approach:**
  1. Ensure pipeline running (generator + ClickHouse)
  2. Wait 2 minutes after stack is stable
  3. Query:
     ```sql
     SELECT
         toStartOfMinute(event_time) AS minute,
         sum(written_rows)           AS rows_inserted,
         round(sum(written_rows) / 60, 0) AS rows_per_sec
     FROM system.query_log
     WHERE query_kind = 'Insert'
       AND tables[1] LIKE '%raw_%'
       AND event_time >= now() - INTERVAL 10 MINUTE
     GROUP BY minute ORDER BY minute;
     ```
- **Success:** At least 5 rows in result, rows_per_sec > 10
- **Commit:** `perf(clickhouse): measure Kafka Engine insert rate rows/sec`
- **Result:** _fill during execution_

---

### T03 — dbt Full Pipeline Timing
- **Status:** `[ ]`
- **Why:** metrics_results.md only has test time (~4s), missing run time — needed for complete story
- **Approach:**
  ```powershell
  docker exec airflow-webserver bash -c "
    cd /opt/airflow && \
    time dbt deps --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt 2>&1 | tail -3 && \
    time dbt run --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt 2>&1 | tail -5 && \
    time dbt test --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt 2>&1 | tail -5
  "
  ```
- **Success:** 3 separate timing numbers (deps, run, test)
- **Commit:** `docs(dbt): add full pipeline run timing to metrics`
- **Result:** _fill during execution_

---

### T04 — Spark Micro-Batch Stats
- **Status:** `[ ]`
- **Why:** Currently only have "7-9s" — need rows/batch and inputRate for complete cold path story
- **Approach:**
  Query Spark REST API for each streaming job:
  ```powershell
  Invoke-RestMethod "http://localhost:4040/api/v1/applications" | ConvertTo-Json
  # Then for each app:
  Invoke-RestMethod "http://localhost:4040/api/v1/applications/<appId>/streaming/statistics"
  ```
  Check ports 4040, 4041, 4042 (orders, payments, rider_events respectively)
- **Success:** inputRate, processedRowsPerSecond, batchDuration for all 3 jobs
- **Commit:** `docs(spark): document micro-batch throughput and batch duration`
- **Result:** _fill during execution_

---

### T05 — Dedup Efficiency Documentation
- **Status:** `[ ]`
- **Why:** 17,830 raw → 6,936 fct_orders is a strong data quality metric, not currently formatted for CV
- **Approach:**
  Run SQL to get current ratio with FINAL:
  ```sql
  SELECT
      count()                     AS raw_rows,
      uniqExact(order_id)         AS unique_orders,
      round(count() / uniqExact(order_id), 2) AS dup_ratio
  FROM food_delivery.raw_orders FINAL;
  ```
  Also measure FINAL vs non-FINAL query time (bonus metric)
- **Success:** Clear dedup ratio + FINAL overhead number
- **Commit:** `docs(metrics): add deduplication efficiency and FINAL overhead stats`
- **Result:** _fill during execution_

---

## TIER 2 — Code Changes + Measurement (~5h)

### T06 — Hot Path E2E Latency (producer_ts)
- **Status:** `[ ]`
- **Requires:** `docker compose down -v && docker compose up -d` (fresh stack, schema change)
- **Why:** Most important missing CV metric. "Vài giây" not defensible — need P50/P95 in seconds
- **Approach:**

  **Step 1** — Add `producer_ts` to Order schema (`generator/schemas/order.py`):
  ```python
  import time
  producer_ts: float = Field(default_factory=time.time)
  ```

  **Step 2** — ALTER TABLE in ClickHouse init (`clickhouse/init/02_create_raw_tables.sql`):
  ```sql
  producer_ts Float64 DEFAULT 0,  -- add after event_timestamp column
  ```

  **Step 3** — Update Kafka Engine MV (`clickhouse/init/04_create_kafka_engine.sql`):
  Add `producer_ts` to both the queue table column list and the MV SELECT

  **Step 4** — After fresh stack up, run generator 10 minutes, then:
  ```sql
  SELECT
      round(quantile(0.5)(toUnixTimestamp(now()) - producer_ts), 2)  AS p50_latency_sec,
      round(quantile(0.95)(toUnixTimestamp(now()) - producer_ts), 2) AS p95_latency_sec,
      round(min(toUnixTimestamp(now()) - producer_ts), 2)            AS min_sec,
      round(max(toUnixTimestamp(now()) - producer_ts), 2)            AS max_sec,
      count()                                                         AS sample_count
  FROM food_delivery.raw_orders
  WHERE producer_ts > 0
  ORDER BY _ingested_at DESC
  LIMIT 1000;
  ```
  Run this query 5 times, take average P50/P95

- **Success:** P50 < 10s, P95 < 30s, sample_count > 500
- **Commits:**
  1. `feat(generator): add producer_ts field for E2E latency tracking`
  2. `feat(clickhouse): add producer_ts column + update Kafka Engine MV`
  3. `docs(metrics): record hot path P50/P95 E2E latency`
- **Result:** _fill during execution_

---

### T07 — ClickHouse Concurrent Query Benchmark
- **Status:** `[ ]`
- **Why:** ClickHouse designed for analytical concurrency — need data to back this up
- **Approach:**
  Create `clickhouse/bench_concurrent.py`:
  ```python
  import concurrent.futures, time
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
      return time.perf_counter() - t0

  for n in [1, 5, 10]:
      with concurrent.futures.ThreadPoolExecutor(max_workers=n) as ex:
          times = list(ex.map(run_query, range(n)))
      p50 = sorted(times)[len(times)//2]
      p95 = sorted(times)[int(len(times)*0.95)]
      print(f"N={n:2d} | P50={p50*1000:.0f}ms | P95={p95*1000:.0f}ms")
  ```
  Run 3 times, pick median result
- **Success:** N=10 P95 < 500ms
- **Commit:** `perf(clickhouse): concurrent query benchmark N=1,5,10`
- **Result:** _fill during execution_

---

### T08 — Kafka Broker Failure Recovery Test
- **Status:** `[ ]`
- **Why:** Classic interview question, currently documented as "gap" in testing
- **Approach:**
  1. Confirm pipeline running, note current lag counters
  2. Kill broker: `docker compose stop kafka`
  3. Record exactly what happens in Spark logs + ClickHouse (30-second window)
  4. Restart: `docker compose start kafka`
  5. Measure time until Spark reconnects (grep logs for "reconnect" or "resumed")
  6. Check messages in-flight: compare offset before kill vs after recovery
  7. Document: messages lost (expected: yes, RF=1), recovery time, Spark behavior
- **Success:** Document recovery time in seconds, message loss count, exact error sequence
- **Commit:** `test(reliability): Kafka broker failure recovery — document RF=1 behavior`
- **Result:** _fill during execution_
- **Expected finding:** RF=1 → some in-flight messages lost on broker kill; Spark resumes from checkpoint; ClickHouse Kafka Engine reconnects automatically

---

### T09 — Cold Path Exact Latency
- **Status:** `[ ]`
- **Why:** Currently "7-9s S3A overhead" but that's just Spark processing, not full pipeline. Need: event_time → Parquet readable in MinIO
- **Approach:**
  Add timing instrumentation to `spark/jobs/stream_orders.py`:
  ```python
  def log_batch_timing(batch_df, epoch_id):
      if batch_df.count() == 0:
          return
      t_start = time.time()
      batch_df.write.parquet(...)  # existing write logic
      t_end = time.time()
      logger.info("BATCH_TIMING | epoch=%d rows=%d write_sec=%.2f",
                  epoch_id, batch_df.count(), t_end - t_start)
  ```
  Switch from `.writeStream` to `foreachBatch` to enable timing
  Run 5 minutes, collect BATCH_TIMING logs, compute P50/P95 write time
- **Success:** 20+ timing samples, clear P50/P95 write latency
- **Commit:** `perf(spark): instrument foreachBatch timing for cold path latency`
- **Result:** _fill during execution_

---

## TIER 3 — Architecture Features (~6h)

### T10 — Schema Registry + Avro Serialization
- **Status:** `[ ]`
- **Requires:** `docker compose down -v && docker compose up -d` after config changes
- **Why:** Most common Kafka interview question. "We use JSON" is a weak answer. Full Avro stack = strong answer
- **Port:** Use 8082 (external) → 8081 (internal) to avoid conflict with Spark worker UI (8081)
- **Approach:**

  **Step 1** — Add to `docker-compose.yml`:
  ```yaml
  schema-registry:
    image: confluentinc/cp-schema-registry:7.5.0
    depends_on: [kafka]
    ports: ["8082:8081"]
    environment:
      SCHEMA_REGISTRY_HOST_NAME: schema-registry
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: "kafka:29092"
      SCHEMA_REGISTRY_LISTENERS: "http://0.0.0.0:8081"
  ```

  **Step 2** — Create Avro schema files (`generator/schemas/avro/`):
  - `order.avsc` — mirror of `Order` Pydantic model
  - `payment.avsc` — mirror of `Payment` model
  - `rider_event.avsc` — mirror of `RiderEvent` model

  **Step 3** — Update `generator/requirements.txt`:
  ```
  confluent-kafka[avro]>=2.3.0
  fastavro>=1.9.0
  ```

  **Step 4** — Update `generator/config.py` + `base_producer.py`:
  - Add `SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")`
  - Switch from plain `Producer` to `AvroProducer` (or `SerializingProducer` with AvroSerializer)
  - Schema auto-registration on first message

  **Step 5** — Update `clickhouse/init/04_create_kafka_engine.sql`:
  ```sql
  -- Change format from JSONEachRow to AvroConfluent
  kafka_format = 'AvroConfluent'
  -- Add setting (in SETTINGS block):
  format_avro_schema_registry_url = 'http://schema-registry:8081'
  ```

  **Step 6** — Update `spark/jobs/stream_*.py`:
  ```python
  from pyspark.sql.functions import from_avro
  # Replace from_json with from_avro using schema registry URL
  ```

  **Step 7** — Verify end-to-end:
  - Check schema registered: `curl http://localhost:8082/subjects`
  - Check ClickHouse ingesting: `SELECT count() FROM raw_orders`
  - Check Spark writing: MinIO objects increasing

- **Success:** All 3 schemas registered in Registry, ClickHouse ingesting Avro, Spark reading Avro from Kafka
- **Commits:**
  1. `feat(docker-compose): add Confluent Schema Registry service on port 8082`
  2. `feat(generator): migrate producers to Avro serialization with Schema Registry`
  3. `feat(clickhouse): update Kafka Engine to AvroConfluent format`
  4. `feat(spark): update streaming jobs to deserialize Avro via Schema Registry`
- **Result:** _fill during execution_

---

### T11 — SparkSubmitOperator in Airflow
- **Status:** `[ ]`
- **Why:** Current DAG assumes Spark job completes before 2 AM — no actual dependency tracking. Known limitation in code comment
- **Approach:**

  **Step 1** — Add to `airflow/requirements.txt`:
  ```
  apache-airflow-providers-apache-spark>=4.1.0
  ```

  **Step 2** — Add Spark connection to Airflow (`docker-compose.yml` env vars for Airflow):
  ```yaml
  AIRFLOW_CONN_SPARK_DEFAULT: "spark://spark:7077"
  ```

  **Step 3** — Rewrite `airflow/dags/batch_daily_summary.py`:
  Replace the "run-at-1AM-assume-done" approach with:
  ```python
  from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

  run_spark = SparkSubmitOperator(
      task_id="run_spark_batch",
      application="/opt/spark/jobs/batch_daily_summary.py",
      conn_id="spark_default",
      application_args=["--date", "{{ ds }}"],
      conf={
          "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
          "spark.hadoop.fs.s3a.access.key": "minioadmin",
          "spark.hadoop.fs.s3a.secret.key": "minioadmin",
          "spark.hadoop.fs.s3a.path.style.access": "true",
      },
      dag=dag,
  )
  run_spark >> check_spark_output >> load_to_clickhouse
  ```

  **Step 4** — Update schedule from `0 2 * * *` to `0 1 * * *` (run at 1 AM, chain tasks)

  **Step 5** — Rebuild Airflow image: `docker compose build airflow-webserver`

- **Success:** DAG shows SparkSubmitOperator task, runs to completion, ClickHouse gets loaded as downstream dependency
- **Commits:**
  1. `feat(airflow): add SparkSubmitOperator for batch job with real dependency tracking`
  2. `feat(airflow): update batch DAG schedule and task chain`
- **Result:** _fill during execution_

---

### T12 — Incremental dbt Model (fct_orders)
- **Status:** `[ ]`
- **Why:** Interview Q: "do you use incremental?" — currently all full-replace. Need one incremental model with clear rationale
- **Approach:**
  Update `dbt/models/marts/fct_orders.sql`:
  ```sql
  {{ config(
      materialized='incremental',
      engine='ReplacingMergeTree()',
      order_by='(city, placed_date, order_id)',
      unique_key='order_id',
      incremental_strategy='delete+insert',
      on_schema_change='append_new_columns'
  ) }}

  SELECT ...
  {% if is_incremental() %}
  WHERE placed_at >= (SELECT max(placed_at) FROM {{ this }}) - INTERVAL 10 MINUTE
  {% endif %}
  ```
  Note: 10-minute lookback handles late-arriving events (matches Spark watermark)

  Measure: full refresh time vs incremental run time
- **Success:** `dbt run --full-refresh` vs `dbt run` shows time difference; model materializes correctly
- **Commit:** `feat(dbt): incremental materialization for fct_orders with 10min lookback`
- **Result:** _fill during execution_

---

### T13 — Data Freshness SLA Panel (Grafana)
- **Status:** `[ ]`
- **Why:** Ops maturity signal — knowing data is stale is as important as knowing lag count
- **Approach:**
  Add panel to `monitoring/grafana/dashboards/food_delivery_infra.json`:
  - Query (ClickHouse datasource):
    ```sql
    SELECT toUnixTimestamp(now()) - max(toUnixTimestamp(placed_at))
    AS seconds_since_last_order
    FROM food_delivery.raw_orders
    ```
  - Panel type: Stat
  - Thresholds: green < 60s, yellow < 300s, red >= 300s
  - Alert rule: fire if > 300s (5 minutes without new order)
- **Success:** Panel visible in Grafana at localhost:3000, shows current freshness in seconds
- **Commit:** `feat(monitoring): add data freshness SLA panel with 5-min alert threshold`
- **Result:** _fill during execution_

---

### T14 — Airflow Failure Alerting
- **Status:** `[ ]`
- **Why:** Currently raise ValueError → DAG FAILED silently. Upgrade to actual alerting
- **Approach:**
  Update all 3 DAGs (`dbt_run.py`, `monitor_kafka_lag.py`, `batch_daily_summary.py`):
  ```python
  default_args = {
      ...
      "email_on_failure": True,
      "email_on_retry": False,
      "email": ["caophon9ats2018@gmail.com"],
  }
  ```
  Also configure Airflow SMTP in `docker-compose.yml`:
  ```yaml
  AIRFLOW__EMAIL__EMAIL_BACKEND: airflow.utils.email.send_email_smtp
  AIRFLOW__SMTP__SMTP_HOST: smtp.gmail.com
  AIRFLOW__SMTP__SMTP_PORT: 587
  AIRFLOW__SMTP__SMTP_USER: ${SMTP_USER}
  AIRFLOW__SMTP__SMTP_PASSWORD: ${SMTP_PASSWORD}
  AIRFLOW__SMTP__SMTP_MAIL_FROM: ${SMTP_USER}
  ```
  Add SMTP_USER + SMTP_PASSWORD to `.env` (not committed)
- **Success:** Trigger a test failure in monitor_kafka_lag, email received
- **Commit:** `feat(airflow): configure email alerting on DAG failure for all 3 DAGs`
- **Result:** _fill during execution_

---

## TIER 4 — Documentation (~30min)

### T15 — Update metrics_results.md
- **Status:** `[ ]`
- **Approach:** Consolidate all results from T01–T14 into `docs/metrics_results.md`
  - Add new sections for each measured metric
  - Update Summary for CV table at bottom
- **Commit:** `docs(metrics): update with benchmark session results`

---

### T16 — Final CV Bullets
- **Status:** `[ ]`
- **Approach:** Write complete CV section in English with all measured numbers
  All claims must map to a specific task result above
  Output to `docs/cv_bullets.md`
- **Commit:** `docs(cv): add final CV bullets with measured metrics`

---

## Execution Order

```
T01 → T02 → T03 → T04 → T05          # Tier 1, no code changes, ~1.5h
  → docker compose down -v
T06 (fresh stack) → T07 → T08 → T09  # Tier 2, code changes + measurement, ~5h
  → docker compose down -v
T10 (fresh stack) → T11 → T12        # Tier 3a, architecture, ~4h
T13 → T14                             # Tier 3b, observability, ~1.5h
T15 → T16                             # Tier 4, documentation, ~30min
```

## Known Constraints
- RF=1 on Kafka → expect message loss in T08 failure test (this is the finding, not a failure)
- Schema Registry port: 8082 external (avoids conflict with Spark worker UI at 8081)
- ClickHouse native port: 9900 (external), 9000 (internal) — use 9900 for clickhouse-client from host
- dbt profiles.yml path: `/opt/airflow/dbt/profiles.yml` inside Airflow container
- Spark submit in Airflow (T11): requires `spark://spark:7077` connection, test connection before wiring DAG
