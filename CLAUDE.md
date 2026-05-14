# CLAUDE.md — Vietnam Food Delivery Pipeline

## Project Context

Real-time food delivery analytics pipeline (GrabFood/ShopeeFood style).
- **Goal:** Build a portfolio project with measurable CV metrics
- **Master plan:** See `PROJECT1_MASTER_PLAN_v2.md` for full architecture, tech stack, commit plan
- **Phase tracking:** 48 commits across 6 phases (infra → generator → spark → clickhouse/load → dbt → observability)
- **Plan is a guide, not a contract** — expect commits to shift, be added, or renamed as implementation reveals real constraints

Stack: Kafka 3.6 · PySpark 3.5 · MinIO (S3-compatible) · ClickHouse 24.x · Airflow 2.8 · dbt-core 1.7 (dbt-clickhouse) · Grafana 10 · Prometheus · Docker Compose · Python 3.11

## Collaboration Style

- **Phản bác thoải mái.** Nếu thấy approach chưa hợp lý, nói thẳng và giải thích tại sao — không cần lịch sự quá.
- **Chỉ ra lỗi sai chủ động.** Nếu tôi làm sai kỹ thuật, thiếu edge case, hay có cách tốt hơn → nói ra ngay, đừng chờ được hỏi.
- **Không giải thích những gì tôi đã biết.** Assume tôi hiểu Python, SQL cơ bản. Chỉ giải thích khi tôi hỏi hoặc khi concept thực sự phức tạp.
- **Ngắn gọn.** Không cần summary cuối response. Code tự nói lên. Nếu tôi cần giải thích thêm tôi sẽ hỏi.
- **Tiếng Việt** cho conversation, tiếng Anh cho code/comments/commit messages.
- **Ghi nhớ lỗi và fix.** Mỗi khi phát hiện bug, misconfiguration, hoặc gotcha — dù tự tìm hay được báo — phải lưu ngay vào memory (`feedback_docker_compose_gotchas.md` hoặc file phù hợp). Ghi: lỗi gì, tại sao xảy ra, fix như nào, áp dụng ở đâu.

## Shortcuts (dùng trong chat để tránh viết lại)

| Keyword | Nghĩa |
|---------|-------|
| `/new-file [path]` | Tạo file theo đúng folder structure trong master plan |
| `/debug [error]` | Format: file + error + code đang dùng + đã thử gì |
| `/review [file]` | List bugs, edge cases, performance issues — không giải thích, chỉ list + fix |
| `/commit [phase]` | Gợi ý commit message theo conventional commits cho phase hiện tại |
| `/metrics` | Nhắc cách đo 5 metrics chính (throughput, latency, dbt tests, uptime, volume) |
| `/interview [topic]` | Chuẩn bị câu trả lời interview cho topic (kafka/spark/dbt/airflow) |

## Technical Constraints (luôn nhớ khi code)

- Port assignments: Kafka 9092/29092, Zookeeper 2181, Kafka-UI 8090, MinIO 9000/9001, Spark 8081/7077, Airflow 8080, ClickHouse 8123/9900, Grafana 3000, Prometheus 9090
- Spark: luôn có watermark + checkpointLocation + trigger 500ms; `dropDuplicates` phải include watermark column (`event_timestamp`) để bound state; write sang MinIO dùng `format("parquet")` hoặc `format("delta")`
- Kafka: INTERNAL://kafka:29092 + EXTERNAL://localhost:9092, `failOnDataLoss=false`, checkpoint để track offset
- ClickHouse: MergeTree, `LowCardinality` cho enum fields, port native 9900 (external) để tránh conflict MinIO 9000; dbt dùng HTTP port 8123
- dbt: staging = rename+cast only, intermediate = joins/logic, marts = analytics-ready; `dbt-clickhouse` package
- Git: conventional commits (`feat:`, `fix:`, `docs:`, `test:`, `perf:`, `refactor:`), 1 commit = 1 việc cụ thể
- **Không commit:** `.env`, `PROJECT1_MASTER_PLAN_v2.md`, `venv/`, `__pycache__/`

## Current Phase

Xem `PROJECT1_MASTER_PLAN_v2.md` → section "Commit Plan" để biết đang ở commit nào.
Khi bắt đầu session mới, nói "đang ở commit X" để tôi biết context.
