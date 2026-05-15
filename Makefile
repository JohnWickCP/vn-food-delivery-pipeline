COMPOSE      = docker compose -f docker-compose.yml
COMPOSE_MON  = docker compose -f docker-compose.monitoring.yml

.PHONY: up down restart logs ps \
        up-mon down-mon \
        kafka-topics kafka-lag \
        metrics test \
        disk-usage clean-data purge

# ── Core stack ────────────────────────────────────────────────────────────────

up:
	$(COMPOSE) up -d

down:
	$(COMPOSE) down
	$(COMPOSE_MON) down

restart:
	$(COMPOSE) restart

logs:
	$(COMPOSE) logs -f --tail=100

ps:
	$(COMPOSE) ps

# ── Monitoring stack ──────────────────────────────────────────────────────────

up-mon:
	$(COMPOSE_MON) up -d

down-mon:
	$(COMPOSE_MON) down

# ── Kafka helpers ─────────────────────────────────────────────────────────────

kafka-topics:
	docker exec kafka kafka-topics --list --bootstrap-server kafka:29092

kafka-lag:
	docker exec kafka kafka-consumer-groups \
		--bootstrap-server kafka:29092 \
		--describe --all-groups

# ── Metrics (run after pipeline is live) ──────────────────────────────────────

metrics:
	@bash scripts/measure_metrics.sh

# ── Tests ─────────────────────────────────────────────────────────────────────

test:
	@echo "=== dbt run ===" && \
	MSYS_NO_PATHCONV=1 docker exec airflow-scheduler \
	  bash -c "dbt deps --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt --quiet && \
	           dbt run  --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt" && \
	echo "=== dbt test ===" && \
	MSYS_NO_PATHCONV=1 docker exec airflow-scheduler \
	  dbt test --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt

# ── Disk management ───────────────────────────────────────────────────────────

disk-usage:
	@echo "=== Docker disk usage ===" && \
	docker system df && \
	echo "" && \
	echo "=== MinIO cold storage ===" && \
	docker exec minio mc du local/food-delivery-lake/ && \
	echo "" && \
	echo "=== ClickHouse data ===" && \
	docker exec clickhouse clickhouse-client \
	  --query "SELECT formatReadableSize(sum(bytes_on_disk)) FROM system.parts"

# Xóa MinIO data (Parquet + checkpoints) — giải phóng dung lượng nhanh nhất.
# ClickHouse, Kafka, Airflow không bị ảnh hưởng — pipeline tiếp tục sau khi start lại.
clean-data:
	@echo "Stopping Spark streaming jobs..."
	$(COMPOSE) stop spark-streaming-orders spark-streaming-payments spark-streaming-rider-events
	@echo "Removing MinIO data volume..."
	$(COMPOSE) rm -f spark-streaming-orders spark-streaming-payments spark-streaming-rider-events
	docker volume rm -f vn-food-delivery-pipeline_minio-data
	@echo "Restarting MinIO + minio-init..."
	$(COMPOSE) up -d minio minio-init
	@echo "Done. Run 'make up' to restart Spark streaming jobs."

# Reset hoàn toàn: xóa tất cả volumes (mất toàn bộ data).
purge:
	$(COMPOSE) down -v
	$(COMPOSE_MON) down -v
	@echo "All volumes removed. Run 'make up' for a fresh start."
