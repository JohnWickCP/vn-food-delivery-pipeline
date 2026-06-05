# Docker Disk Cleanup — Windows WSL2

Dùng khi Docker Desktop chiếm quá nhiều dung lượng ổ cứng.
Tested: 61 GB → 3.5 GB sau full cleanup + VHDX compact.

---

## Bước 1 — Prune trong Docker

Chạy từng lệnh, theo thứ tự:

```powershell
# Xóa dangling images (<none>)
docker image prune -f

# Xóa images không còn dùng (từ project cũ, không ảnh hưởng pipeline đang chạy)
docker rmi apache/nifi:1.23.2 debezium/connect:2.5 grafana/grafana:latest `
    cdc-spark:3.5.0 mongo:7.0 mysql:8.0 prom/prometheus:latest `
    grafana/grafana:10.0.0 2>$null

# Xóa volumes orphan — KHÔNG xóa volume của vn-food-delivery-pipeline
docker volume prune -f
```

> **Lưu ý:** `docker volume prune` chỉ xóa volumes không được gắn với container nào.
> Volumes của pipeline (`vn-food-delivery-pipeline_*`) sẽ không bị ảnh hưởng nếu containers đang chạy hoặc đã được tạo.

### Kiểm tra trước khi prune
```powershell
# Xem images nào đang chiếm nhiều nhất
docker images --format "{{.Size}}\t{{.Repository}}:{{.Tag}}" | sort -Descending | head -20

# Xem volumes nào tồn tại
docker volume ls
```

---

## Bước 2 — Compact VHDX (bắt buộc, không bỏ qua)

**Tại sao cần bước này?** Sau khi Docker xóa dữ liệu bên trong container/volume, file `.vhdx` (virtual disk) trên Windows vẫn giữ nguyên kích thước. VHDX không tự shrink. Phải compact thủ công.

### Chuẩn bị
1. **Stop Docker Desktop** hoàn toàn (Right-click icon → Quit Docker Desktop)
2. Kiểm tra Docker đã stop: `docker ps` phải báo lỗi "Cannot connect to the Docker daemon"

### Compact VHDX bằng diskpart (PowerShell as Administrator)

```
# Mở PowerShell với quyền Administrator, sau đó:
diskpart
```

Trong cửa sổ diskpart, gõ từng dòng:

```
select vdisk file="C:\Users\CaoPhon\AppData\Local\Docker\wsl\disk\docker_data.vhdx"
attach vdisk readonly
compact vdisk
detach vdisk
exit
```

> Quá trình compact mất 2–10 phút tùy dung lượng. Không tắt giữa chừng.

### Kiểm tra kết quả
```powershell
# Xem kích thước VHDX sau compact
Get-Item "C:\Users\CaoPhon\AppData\Local\Docker\wsl\disk\docker_data.vhdx" | Select-Object Name, @{N='SizeGB';E={[math]::Round($_.Length/1GB,1)}}
```

---

## Kết quả kỳ vọng

| Giai đoạn | Kích thước VHDX |
|-----------|-----------------|
| Trước khi prune | ~61 GB |
| Sau prune, trước compact | ~61 GB (không đổi!) |
| Sau compact | ~3.5 GB |

---

## Khi nào cần làm lại?

- Pipeline đã chạy tích lũy nhiều ngày (MinIO sinh ~14 GB/ngày Parquet)
- Sau `make purge` (xóa toàn bộ volumes) — VHDX vẫn không tự shrink
- Ổ C: sắp đầy

### Dọn dẹp nhanh (không cần compact)
```powershell
# Chỉ xóa MinIO data + Spark checkpoints, giữ lại Kafka/ClickHouse/Airflow
make clean-data

# Xóa toàn bộ volumes (reset sạch)
make purge
```

### Dọn dẹp pipeline đang chạy
```powershell
# Stop generator trước để không mất data đang ghi
make down

# Xóa chỉ Parquet files (giữ ClickHouse + Kafka offsets)
make clean-data

# Restart
make up
```

---

## Troubleshooting

**diskpart báo "Access is denied":**
→ Chưa chạy PowerShell as Administrator. Chuột phải → "Run as administrator".

**diskpart báo "The system cannot find the file specified":**
→ Kiểm tra đường dẫn VHDX. Có thể ở:
```powershell
Get-ChildItem "C:\Users\$env:USERNAME\AppData\Local\Docker\" -Recurse -Filter "*.vhdx" 2>$null
```

**Docker không start lại sau compact:**
→ Detach vdisk trước khi compact đã được thực hiện chưa? Thử restart máy.

**Compact xong nhưng VHDX vẫn lớn:**
→ Compact chỉ giảm được phần "free space" bên trong VHDX. Nếu trong Docker vẫn còn nhiều images/volumes, cần prune trước rồi compact lại.
