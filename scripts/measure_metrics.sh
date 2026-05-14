#!/usr/bin/env bash
# measure_metrics.sh — CV target metrics for vn-food-delivery-pipeline
# Run from repo root: make metrics
# Requires: pipeline stack running (make up), curl, docker, python3

set -uo pipefail

CH="http://${CLICKHOUSE_HOST:-localhost}:${CLICKHOUSE_PORT:-8123}"

ch()      { curl -sf --data "$1" "${CH}/"; }
ch_null() { curl -sf --data "$1 FORMAT Null" "${CH}/"; }
ms_now()  { date +%s%3N 2>/dev/null || python3 -c 'import time; print(int(time.time()*1000))' 2>/dev/null || python -c 'import time; print(int(time.time()*1000))'; }

sep() { echo ""; echo "── $1 ────────────────────────────────────────────────────"; }

echo ""
echo "======================================================"
echo "  Food Delivery Pipeline — CV Metrics Report"
echo "  $(date '+%Y-%m-%d %H:%M:%S')"
echo "======================================================"

# ── 1. Data Volume ──────────────────────────────────────────────────────────
sep "1. DATA VOLUME  [target: 7M+ events/day, ~20GB/30d]"
ch "
SELECT
    table,
    formatReadableQuantity(sum(rows))                        AS row_count,
    formatReadableSize(sum(data_compressed_bytes))           AS compressed,
    formatReadableSize(sum(data_uncompressed_bytes))         AS uncompressed,
    round(sum(data_uncompressed_bytes) / sum(data_compressed_bytes), 1) AS compression_ratio
FROM system.parts
WHERE database = 'food_delivery' AND active
GROUP BY table
ORDER BY sum(rows) DESC
FORMAT PrettyNoEscapes
"

# ── 2. Real-time Ingestion Rate ─────────────────────────────────────────────
sep "2. KAFKA ENGINE INGESTION RATE  [target: 5000+ orders/min peak]"
echo "  Sampling ClickHouse insert rate (10s window)..."
count1=$(ch "SELECT count() FROM food_delivery.raw_orders FINAL FORMAT TSV")
sleep 10
count2=$(ch "SELECT count() FROM food_delivery.raw_orders FINAL FORMAT TSV")
delta=$((count2 - count1))
echo "  raw_orders: +${delta} rows in 10s  (~$((delta * 6))/min)"

count1p=$(ch "SELECT count() FROM food_delivery.raw_payments FINAL FORMAT TSV")
count1r=$(ch "SELECT count() FROM food_delivery.raw_rider_events FINAL FORMAT TSV")
sleep 5
count2p=$(ch "SELECT count() FROM food_delivery.raw_payments FINAL FORMAT TSV")
count2r=$(ch "SELECT count() FROM food_delivery.raw_rider_events FINAL FORMAT TSV")
echo "  raw_payments: +$((count2p - count1p)) rows in 5s"
echo "  raw_rider_events: +$((count2r - count1r)) rows in 5s"

# ── 3. ClickHouse Query Latency ─────────────────────────────────────────────
sep "3. CLICKHOUSE QUERY LATENCY  [target: <100ms on 5M rows]"
for label_sql in \
    "Q1 hourly_agg 7d|SELECT city,toStartOfHour(placed_at),count(),sum(total_vnd) FROM food_delivery.raw_orders WHERE placed_at>=now()-INTERVAL 7 DAY GROUP BY 1,2" \
    "Q2 payment_method 30d|SELECT city,payment_method,count(),sum(total_vnd) FROM food_delivery.raw_orders WHERE placed_at>=now()-INTERVAL 30 DAY GROUP BY 1,2" \
    "Q3 district 30d|SELECT city,district,count() FROM food_delivery.raw_orders WHERE placed_at>=now()-INTERVAL 30 DAY GROUP BY 1,2" \
    "Q4 simple_count|SELECT count() FROM food_delivery.raw_orders"
do
    label="${label_sql%%|*}"
    sql="${label_sql##*|}"
    t=$(ms_now)
    ch_null "$sql"
    elapsed=$(( $(ms_now) - t ))
    status="✓"; [ "$elapsed" -gt 100 ] && status="⚠ (>100ms)"
    printf "  %-28s %4d ms  %s\n" "${label}:" "${elapsed}" "${status}"
done

# ── 4. dbt Layer Row Counts ─────────────────────────────────────────────────
sep "4. DBT MART TABLES"
ch "
SELECT database, table, formatReadableQuantity(sum(rows)) AS row_count
FROM system.parts
WHERE database LIKE 'food_delivery_dbt%' AND active
GROUP BY database, table
ORDER BY database, table
FORMAT PrettyNoEscapes
" || echo "  (dbt models not yet materialized — run dbt first)"

# ── 5. dbt Tests ────────────────────────────────────────────────────────────
sep "5. DBT TEST RESULTS  [target: 100% pass, 30+ tests]"
MSYS_NO_PATHCONV=1 docker exec airflow-scheduler dbt test \
    --project-dir /opt/airflow/dbt \
    --profiles-dir /opt/airflow/dbt \
    --no-write-json 2>&1 \
    | grep -E "Done\.|Finished running" \
    | tail -3 || echo "  (run 'make up' first to start airflow-scheduler)"

# ── 6. MinIO Storage ─────────────────────────────────────────────────────────
sep "6. MINIO STORAGE (cold path)"
docker exec minio sh -c \
    "du -sh /data/food-delivery-lake 2>/dev/null || echo '  bucket empty or not yet populated'"

echo ""
echo "======================================================"
echo "  Grafana:    http://localhost:3000"
echo "  Kafka UI:   http://localhost:8090"
echo "  Airflow:    http://localhost:8080"
echo "  MinIO:      http://localhost:9001"
echo "======================================================"
echo ""
