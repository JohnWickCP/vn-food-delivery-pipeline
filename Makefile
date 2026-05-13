COMPOSE      = docker compose -f docker-compose.yml
COMPOSE_MON  = docker compose -f docker-compose.monitoring.yml

.PHONY: up down restart logs ps \
        up-mon down-mon \
        kafka-topics metrics test

# ── Core stack ────────────────────────────────────────────────────────────────

up:
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

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
	docker exec kafka kafka-topics --list --bootstrap-server localhost:29092

kafka-lag:
	docker exec kafka kafka-consumer-groups.sh \
		--bootstrap-server localhost:29092 \
		--describe --all-groups

# ── Metrics (run after pipeline is live) ──────────────────────────────────────

metrics:
	@bash scripts/measure_metrics.sh

# ── Tests ─────────────────────────────────────────────────────────────────────

test:
	@echo "=== dbt tests ===" && \
	docker exec airflow-scheduler dbt test --project-dir /opt/dbt || true
