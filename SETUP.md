# Setup Guide — vn-food-delivery-pipeline

> Bất kỳ máy nào có Docker đều chạy được. Không cần cài Python, Java, hay bất cứ thứ gì khác.

---

## Prerequisites

| Tool | Minimum Version | Check |
|------|----------------|-------|
| Docker Engine | 24.x | `docker --version` |
| Docker Compose | v2 (plugin) | `docker compose version` |
| Git | any | `git --version` |
| make | any | `make --version` |
| RAM | **8 GB free** | — |
| Disk | **20 GB free** (MinIO grows ~600 MB/h when running) | — |

> **Windows users:** Docker Desktop + WSL2 backend is required. Git Bash hoặc PowerShell đều dùng được.  
> **macOS users:** Docker Desktop hoặc OrbStack. Apple Silicon (M1/M2/M3) đã được test.  
> **Linux users:** Docker Engine + Docker Compose plugin trực tiếp.

### Kiểm tra nhanh

```bash
docker compose version          # phải ra "Docker Compose version v2.x"
docker run --rm hello-world     # phải thấy "Hello from Docker!"
```

---

## 1. Clone & Configure

```bash
git clone https://github.com/JohnWickCP/vn-food-delivery-pipeline.git
cd vn-food-delivery-pipeline
cp .env.example .env
```

File `.env` đã có defaults đầy đủ cho local dev — **không cần sửa gì** để chạy demo:

```dotenv
# Kafka
KAFKA_BOOTSTRAP_SERVERS=kafka:29092

# MinIO
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin

# Airflow
AIRFLOW_DB_USER=airflow
AIRFLOW_DB_PASSWORD=airflow
AIRFLOW_FERNET_KEY=           # để trống — Airflow dùng default key
AIRFLOW_SECRET_KEY=supersecret

# ClickHouse
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=           # để trống — ClickHouse default user có no password

# Generator settings
ORDERS_PER_MIN_PEAK=5000
ORDERS_PER_MIN_BASE=1200
NUM_RIDERS=200
RIDER_GPS_INTERVAL_SEC=30
```

---

## 2. Kiểm tra Port Conflicts (bắt buộc trước khi start)

Project dùng 12 ports. Nếu bất kỳ port nào đang bị chiếm, `docker compose up` sẽ fail với lỗi `Bind: address already in use`.

### Danh sách ports và ứng dụng hay xung đột

| Port | Service trong project | Ứng dụng hay chiếm |
|------|-----------------------|---------------------|
| 2181 | Zookeeper | Local Zookeeper (non-Docker) |
| 9092 | Kafka (external) | Local Kafka (non-Docker) |
| 9000 | MinIO S3 API | SonarQube, local MinIO, Hadoop NameNode |
| 9001 | MinIO Console | Local MinIO |
| 8080 | Airflow UI | **Spring Boot**, **Tomcat**, XAMPP, Jenkins, local dev server |
| 8081 | Spark Master UI | **Tomcat**, local dev server |
| 8085 | Schema Registry API | Ít gặp |
| 8090 | Kafka UI | Ít gặp |
| 8123 | ClickHouse HTTP | Local ClickHouse (non-Docker) |
| 9900 | ClickHouse Native TCP | Ít gặp |
| 3000 | Grafana | **React/Next.js** `npm run dev`, Node.js apps |
| 9090 | Prometheus | Local Prometheus (non-Docker) |

### Kiểm tra nhanh

**Windows (PowerShell):**
```powershell
netstat -ano | findstr " 2181 9092 9000 9001 8080 8081 8085 8090 8123 9900 3000 9090" | findstr "LISTENING"
```

**macOS / Linux:**
```bash
lsof -i :2181,9092,9000,9001,8080,8081,8085,8090,8123,9900,3000,9090 | grep LISTEN
```

Nếu output trống → tất cả ports free, tiếp tục Bước 3.  
Nếu có dòng xuất hiện → port đó đang bị chiếm, xem hướng dẫn bên dưới.

### Xử lý khi port bị chiếm

**Tắt ứng dụng trước khi chạy:**

| Nếu bạn đang chạy | Cách tắt |
|--------------------|----------|
| Spring Boot app | Dừng trong IDE hoặc `Ctrl+C` trong terminal |
| `npm run dev` / `npm start` | `Ctrl+C` trong terminal |
| XAMPP | XAMPP Control Panel → Stop Apache |
| Tomcat | XAMPP Control Panel → Stop Tomcat, hoặc tắt service |
| SonarQube | `<sonarqube_dir>/bin/sonar.sh stop` |
| Local Kafka/Zookeeper | Dừng service hoặc `kill` process |
| Local ClickHouse | `sudo service clickhouse-server stop` |

**Nếu không biết process nào đang chiếm (Windows PowerShell):**
```powershell
# Tìm PID đang dùng port 8080 (thay 8080 bằng port cần kiểm tra)
netstat -ano | findstr ":8080 " | findstr "LISTENING"
# Kết quả ví dụ: TCP  0.0.0.0:8080  LISTENING  12345  ← 12345 là PID

# Xem tên process
Get-Process -Id 12345

# Tắt process (nếu an toàn)
Stop-Process -Id 12345
```

**macOS / Linux:**
```bash
# Tìm và tắt process chiếm port 8080
lsof -ti :8080 | xargs kill -9
```

> Không nên đổi port trong `docker-compose.yml` vì các services phụ thuộc vào nhau qua port nội bộ — tắt ứng dụng xung đột là cách an toàn nhất.

---

## 3. Start Core Stack

```bash
make up
```

Lần đầu chạy Docker sẽ pull ~3–4GB images và build custom images cho Spark, Airflow, Generator. **Mất 5–10 phút tùy tốc độ internet.**

Lần sau (images đã cache): ~30 giây.

### Theo dõi startup

```bash
make logs           # tail tất cả container logs
# hoặc theo dõi 1 container cụ thể:
docker logs -f clickhouse
docker logs -f airflow-scheduler
```

### Xác nhận tất cả services healthy

```bash
make ps
```

Expected output (sau ~2 phút):

```
NAME                           STATUS
zookeeper                      Up (healthy)
kafka                          Up (healthy)
kafka-init                     Exited (0)       ← one-shot, exit 0 = success
schema-registry                Up (healthy)
minio                          Up (healthy)
minio-init                     Exited (0)       ← one-shot
spark-master                   Up (healthy)
spark-worker-1                 Up
spark-worker-2                 Up
spark-streaming-orders         Up
spark-streaming-payments       Up
spark-streaming-rider-events   Up
airflow-postgres               Up (healthy)
airflow-init                   Exited (0)       ← one-shot
airflow-webserver              Up (healthy)
airflow-scheduler              Up
clickhouse                     Up (healthy)
generator                      Up
kafka-ui                       Up
```

> Nếu một service ở `Restarting` hoặc `Exit non-zero` → xem [Troubleshooting](#7-troubleshooting).

---

## 4. Start Monitoring Stack

```bash
make up-mon
```

Starts: Prometheus, Grafana, node-exporter, kafka-exporter, clickhouse-exporter.

```bash
docker compose -f docker-compose.monitoring.yml ps
```

Expected: prometheus, grafana, và 3 exporters đều `Up`.

---

## 5. Verify Pipeline is Working

### Service URLs

| Service | URL | Login |
|---------|-----|-------|
| **Kafka UI** | http://localhost:8090 | — |
| **Airflow** | http://localhost:8080 | admin / admin |
| **Spark Master** | http://localhost:8081 | — |
| **Grafana** | http://localhost:3000 | admin / admin |
| **Prometheus** | http://localhost:9090 | — |
| **MinIO Console** | http://localhost:9001 | minioadmin / minioadmin |

### Bước 1 — Kafka có data chưa?

Mở http://localhost:8090 → Topics → `raw.orders` → Messages.  
Phải thấy messages JSON với `order_id`, `city`, `total_vnd`, ...

### Bước 2 — ClickHouse nhận data chưa?

```bash
docker exec clickhouse clickhouse-client \
  --query "SELECT count(), max(placed_at) FROM food_delivery.raw_orders"
```

Số lượng phải tăng mỗi vài giây.

### Bước 3 — MinIO có Parquet files chưa?

Mở http://localhost:9001 → Buckets → `food-delivery-lake` → `raw/orders/`.  
Sau 2–3 phút phải thấy folder structure `year=.../month=.../day=.../`.

### Bước 4 — Airflow DAGs

Mở http://localhost:8080 → DAGs.  
Thấy 3 DAGs: `dbt_run`, `monitor_kafka_lag`, `batch_daily_summary`.  
Cả 3 mặc định paused. Để chạy thủ công: toggle ON rồi click "Trigger DAG".

### Bước 5 — dbt tests (sau khi có data)

```bash
make test
```

Expected: `PASS=55 WARN=0 ERROR=0`

### Bước 6 — CV metrics report

```bash
make metrics
```

In ra: data volume, ingestion rate, ClickHouse query latency, dbt test results, MinIO storage.

---

## 6. Run dbt Manually

```bash
# Run all models
docker exec airflow-scheduler dbt run \
  --project-dir /opt/airflow/dbt \
  --profiles-dir /opt/airflow/dbt

# Run tests
docker exec airflow-scheduler dbt test \
  --project-dir /opt/airflow/dbt \
  --profiles-dir /opt/airflow/dbt

# Run specific model
docker exec airflow-scheduler dbt run \
  --project-dir /opt/airflow/dbt \
  --profiles-dir /opt/airflow/dbt \
  --select fct_orders
```

> **Note:** dbt models query `food_delivery` database (raw tables) và write vào `food_delivery_dbt_staging`, `food_delivery_dbt_intermediate`, `food_delivery_dbt_marts`.

---

## 7. Troubleshooting

### ClickHouse không có data sau khi start

`docker compose up` sau khi đã từng chạy trước → ClickHouse tìm thấy volume cũ và **skip init scripts**.  
Biểu hiện: `dbt run` fail với `UNKNOWN_DATABASE food_delivery`.

Fix — chạy init scripts thủ công:

```bash
for f in clickhouse/init/*.sql; do
  echo "=== $f ===" && \
  MSYS_NO_PATHCONV=1 docker exec -i clickhouse clickhouse-client --multiquery < "$f"
done
```

Verify: `docker exec clickhouse clickhouse-client --query "SHOW DATABASES"` phải thấy `food_delivery`.

### `kafka-init` không tạo topics (race condition)

`kafka-init` chạy trước khi Kafka broker fully ready → topics không được tạo → generator log `produced=0 errors=0`, Spark crash với `UnknownTopicOrPartitionException`.

Fix — tạo topics thủ công:

```bash
docker exec kafka kafka-topics --create --if-not-exists --topic raw.orders \
  --partitions 3 --replication-factor 1 --bootstrap-server kafka:29092
docker exec kafka kafka-topics --create --if-not-exists --topic raw.payments \
  --partitions 3 --replication-factor 1 --bootstrap-server kafka:29092
docker exec kafka kafka-topics --create --if-not-exists --topic raw.rider_events \
  --partitions 3 --replication-factor 1 --bootstrap-server kafka:29092

# Verify
docker exec kafka kafka-topics --list --bootstrap-server kafka:29092

# Restart Spark streaming jobs sau khi topics đã sẵn sàng
docker compose restart spark-streaming-orders spark-streaming-payments spark-streaming-rider-events
```

### Spark Streaming không start / restart loop

```bash
docker logs spark-streaming-orders
```

Nguyên nhân phổ biến: MinIO bucket chưa tồn tại khi Spark write checkpoint. Fix:
```bash
docker compose restart spark-streaming-orders spark-streaming-payments spark-streaming-rider-events
```

### Airflow webserver không healthy sau 2 phút

```bash
docker logs airflow-webserver | tail -20
```

Nếu thấy `alembic` error → database migration chưa xong:
```bash
docker compose restart airflow-webserver
```

### dbt `Found 1 package(s) but 0 installed`

Packages bị mất khi container recreate. Fix:
```bash
docker exec airflow-scheduler dbt deps \
  --project-dir /opt/airflow/dbt \
  --profiles-dir /opt/airflow/dbt
```

### Port conflict (address already in use)

Xem [Bước 2 — Kiểm tra Port Conflicts](#2-kiểm-tra-port-conflicts-bắt-buộc-trước-khi-start) để biết danh sách đầy đủ ports, ứng dụng hay xung đột và cách xử lý.

Lệnh kiểm tra nhanh nếu `make up` vừa fail:
```powershell
# Windows — tìm port bị chiếm (ví dụ 8080)
netstat -ano | findstr ":8080 " | findstr "LISTENING"
Get-Process -Id <PID_từ_kết_quả_trên>
```
```bash
# macOS/Linux
lsof -ti :8080 | xargs kill -9
```

### Prometheus targets không up

```bash
make up-mon        # đảm bảo monitoring stack đã start
# Sau đó mở http://localhost:9090/targets
```

Nếu Spark targets (4040/4041/4042) không up: Spark streaming jobs cần vài phút để start driver.

### Windows: Git Bash path mangling

Nếu dùng Git Bash và gặp lỗi `C:/Program Files/Git/opt/...`:
```bash
MSYS_NO_PATHCONV=1 docker exec clickhouse clickhouse-client ...
```

---

## 8. Stop & Disk Management

### Khi nào cần dừng?

Pipeline tạo ra ~600 MB dữ liệu mỗi giờ (Parquet files trong MinIO). Trên máy dev **không nên để chạy qua đêm** nếu ổ C dưới 30 GB free.

Kiểm tra nhanh:
```bash
make disk-usage
```

### Dừng bình thường (giữ nguyên data)

```bash
make down        # dừng tất cả (core + monitoring)
```

Data vẫn còn trong Docker volumes — `make up` lại là tiếp tục ngay.

### Xóa cold storage MinIO (giải phóng dung lượng nhanh nhất)

ClickHouse, Kafka, Airflow không bị ảnh hưởng. Chỉ mất Parquet files và Spark checkpoints.

```bash
make clean-data
```

Sau đó `make up` để restart Spark streaming jobs. Spark sẽ bắt đầu lại từ đầu Kafka (offset `earliest`).

### Reset hoàn toàn (mất toàn bộ data)

```bash
make purge       # xóa hết volumes
make up          # fresh start
```

### Windows WSL2: ổ C vẫn đầy sau khi xóa data?

Docker Desktop dùng một file `.vhdx` trong WSL2 — file này **không tự shrink** sau khi xóa data bên trong. Phải compact thủ công.

**Bước 1 — Tìm đúng đường dẫn file vhdx** (PowerShell bình thường):

```powershell
Get-ChildItem "C:\Users\$env:USERNAME\AppData\Local\Docker" -Filter "*.vhdx" -Recurse -ErrorAction SilentlyContinue | Select-Object FullName, @{N='SizeGB';E={[math]::Round($_.Length/1GB,2)}}
```

Kết quả sẽ ra đường dẫn đầy đủ, ví dụ:
```
C:\Users\CaoPhon\AppData\Local\Docker\wsl\disk\docker_data.vhdx   17.5
```

**Bước 2 — Quit Docker Desktop hoàn toàn** (chuột phải icon system tray → Quit Docker Desktop).

**Bước 3 — PowerShell Administrator:**

```powershell
wsl --shutdown
```

**Bước 4 — Vẫn trong PowerShell Admin, gõ `diskpart` rồi Enter:**

```powershell
diskpart
```

Chờ thấy prompt `DISKPART>` rồi paste từng dòng (thay path bằng kết quả từ Bước 1):

```
select vdisk file="C:\Users\CaoPhon\AppData\Local\Docker\wsl\disk\docker_data.vhdx"
detach vdisk
attach vdisk readonly
compact vdisk
detach vdisk
exit
```

> `compact vdisk` mất 2–5 phút, không có progress bar — chờ đến khi thấy `DiskPart successfully compacted the virtual disk file.`

> **Lưu ý:** `detach vdisk` đầu tiên là bắt buộc nếu file đang được Docker giữ — nếu không, `attach vdisk readonly` sẽ fail với lỗi "already attached".

Kết quả thực tế: 17–61 GB → 1–3.5 GB sau compact.

---

## 9. Resource Requirements

| Component | RAM | CPU |
|-----------|-----|-----|
| Kafka + Zookeeper | ~800 MB | — |
| Spark (master + 2 workers + 3 drivers) | ~4 GB | 6–8 cores recommended |
| ClickHouse | ~1 GB | — |
| Airflow (webserver + scheduler + postgres) | ~1 GB | — |
| Generator | ~200 MB | — |
| MinIO | ~200 MB | — |
| Monitoring (Prometheus + Grafana + exporters) | ~400 MB | — |
| **Total** | **~8 GB** | **8+ cores recommended** |

> Nếu máy có ít RAM: giảm `SPARK_WORKER_MEMORY=2G` và `SPARK_WORKER_CORES=2` trong `.env`.

---

## Quick Reference

```bash
make up           # start core stack
make up-mon       # start monitoring
make down         # stop all (core + monitoring)
make logs         # tail all logs
make ps           # container status
make test         # dbt deps + run + test (55/55)
make metrics      # print CV metrics report
make kafka-lag    # check Kafka consumer lag
make kafka-topics # list Kafka topics
make disk-usage   # Docker + MinIO + ClickHouse disk stats
make clean-data   # xóa MinIO cold storage (giữ ClickHouse/Kafka)
make purge        # reset hoàn toàn — xóa tất cả volumes
```
