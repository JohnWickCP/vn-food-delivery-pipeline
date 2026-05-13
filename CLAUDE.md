# CLAUDE.md — Vietnam Food Delivery Pipeline

## Project Context

Real-time food delivery analytics pipeline (GrabFood/ShopeeFood style).
- **Goal:** Build a portfolio project with measurable CV metrics
- **Master plan:** See `PROJECT1_MASTER_PLAN.md` for full architecture, tech stack, commit plan
- **Phase tracking:** 42 commits across 6 phases (infra → generator → spark → airflow → dbt → observability)

Stack: Kafka 3.6 · PySpark 3.5 · Airflow 2.8 · dbt-core 1.7 · PostgreSQL 15 · Grafana 10 · Docker Compose · Python 3.11

## Collaboration Style

- **Phản bác thoải mái.** Nếu thấy approach chưa hợp lý, nói thẳng và giải thích tại sao — không cần lịch sự quá.
- **Chỉ ra lỗi sai chủ động.** Nếu tôi làm sai kỹ thuật, thiếu edge case, hay có cách tốt hơn → nói ra ngay, đừng chờ được hỏi.
- **Không giải thích những gì tôi đã biết.** Assume tôi hiểu Python, SQL cơ bản. Chỉ giải thích khi tôi hỏi hoặc khi concept thực sự phức tạp.
- **Ngắn gọn.** Không cần summary cuối response. Code tự nói lên. Nếu tôi cần giải thích thêm tôi sẽ hỏi.
- **Tiếng Việt** cho conversation, tiếng Anh cho code/comments/commit messages.

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

- Port assignments: Kafka 9092/29092, Zookeeper 2181, PG 5432, Airflow 8080, Spark 8081/7077, Grafana 3000, Prometheus 9090
- Spark: luôn có watermark + checkpointLocation + trigger 500ms + foreachBatch cho JDBC
- Kafka: INTERNAL + EXTERNAL listeners, manual offset commit
- dbt: staging = rename+cast only, intermediate = joins/logic, marts = analytics-ready
- Git: conventional commits (`feat:`, `fix:`, `docs:`, `test:`, `perf:`, `refactor:`), 1 commit = 1 việc cụ thể
- **Không commit:** `.env`, `PROJECT1_MASTER_PLAN.md`, `venv/`, `__pycache__/`

## Current Phase

Xem `PROJECT1_MASTER_PLAN.md` → section "Commit Plan" để biết đang ở commit nào.
Khi bắt đầu session mới, nói "đang ở commit X" để tôi biết context.
