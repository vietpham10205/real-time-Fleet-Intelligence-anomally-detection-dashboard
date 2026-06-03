# NYC Taxi — Real-time Fraud Detection & Fleet Intelligence Pipeline

**Đồ án môn học: Big Data & Cơ sở dữ liệu phân tán**

Hệ thống xử lý luồng dữ liệu xe Taxi NYC theo thời gian thực (Real-time Stream Processing), ứng dụng Ensemble Machine Learning (Isolation Forest + MinCovDet) để phát hiện các chuyến đi bất thường (thu phí sai luật, tốc độ phi lý, pattern gian lận...) và đánh giá độ chính xác của hệ thống thông qua bộ Ground Truth do AI tạo ra.

---

## Kiến trúc Hệ thống

Hệ thống xử lý song song 2 luồng dữ liệu (Yellow Taxi & Green Taxi), đi qua các tầng sau:

1. **Nguồn dữ liệu**: File `Parquet` tháng 03/2026 (Yellow ~67MB, Green ~1MB) chứa hàng triệu chuyến đi.
2. **Đọc & Tiền xử lý**: Script Python đọc Parquet, chuẩn hóa tên cột, loại bỏ giá trị null.
3. **Producer (Kafka)**: 2 Kafka Producer song song (`yellow_producer.py`, `green_producer.py`) đẩy dữ liệu JSON vào topic `taxi_stream` (200 msg/s mỗi producer).
4. **Kafka Topic**: Dữ liệu tập trung vào topic `taxi_stream` — 1 partition, replication factor 1.
5. **Kafka Cluster**: Confluent Kafka 7.1.1 + Zookeeper 7.1.1 chạy trên Docker. Được tinh chỉnh throughput (socket buffers, I/O threads) cho luồng Yellow thể tích lớn.
6. **Consume**: Spark Structured Streaming đọc liên tục từ Kafka (5,000 offsets/trigger).
7. **Apache Spark**: PySpark 4.x, driver memory 4GB, package `spark-sql-kafka-0-10_2.13:4.1.1`.
8. **Transform & ML (Feature Engineering + Ensemble)**:
   - Spark: tính `trip_duration_sec`, `avg_speed_kmh`, `fare_per_mile`, lọc các chuyến đi vật lý không hợp lệ.
   - Pandas (in-memory): thêm cyclical time encoding (`hour_sin`/`hour_cos`), log transforms (`log_distance`, `log_fare`), interaction terms (`speed_x_duration`, `fare_distance_ratio`), context flags (`is_rush_hour`, `is_night`).
   - **Ensemble Model**: Isolation Forest (200 estimators, sample weighting theo business rules) + MinCovDet — chỉ flag anomaly khi **cả 2 model đồng ý** (strict voting).
   - **Continuous Learning**: Model tự retrain mỗi 25 batch trên rolling buffer 10,000 bản ghi mới nhất. Contamination rate được tính adaptive (80% prior + 20% observed).
9. **Anomaly Labeling**: Mỗi chuyến bất thường được gán nhãn mô tả lý do (speed, fare, duration, passengers) kèm severity (`[HIGH]`/`[MEDIUM]`).
10. **Ghi (Sink) — PostgreSQL**: Toàn bộ kết quả ghi vào 3 bảng: `normal_trips`, `anomalous_trips`, `pipeline_metrics`.
11. **Web Dashboard**: Streamlit dashboard (`dashboard.py`) tự refresh mỗi 3 giây, hiển thị system metrics, live data stream, scatter charts, và anomaly log.
12. **Benchmark Evaluator**: Streamlit app riêng (`benchmark_evaluator.py`) so sánh kết quả Streaming với Ground Truth AI (Precision, Recall, F1-Score, Confusion Matrix).

---

## Thành phần Dự án

```
data-streams/
├── spark_taxi_processor.py     # Lõi xử lý: Spark Streaming + Ensemble ML
├── yellow_producer.py          # Kafka Producer — Yellow Taxi (tpep)
├── green_producer.py           # Kafka Producer — Green Taxi (lpep)
├── dashboard.py                # Web Dashboard real-time (port 8501)
├── benchmark_evaluator.py      # Đánh giá độ chính xác vs Ground Truth (port 8502)
├── ai_batch_labeler.py         # Script offline tạo Ground Truth labels
├── evaluate_model.py           # Công cụ đánh giá model độc lập
├── reset_db.py                 # Reset PostgreSQL schema
├── docker-compose.yml          # Kafka + Zookeeper + PostgreSQL
├── start_project.bat           # Khởi động toàn hệ thống (1 click)
├── stop_project.bat            # Dừng và dọn dẹp toàn hệ thống
├── isolation_forest_model.pkl  # Model IsolationForest đã train (persisted)
├── robust_scaler.pkl           # RobustScaler đã fit (persisted)
├── mcd_model.pkl               # MinCovDet model đã fit (persisted)
├── AI_LABELING_METHODOLOGY.md  # Tài liệu kỹ thuật về Ground Truth labeling
├── datasets/taxi/
│   ├── yellow_tripdata_2026-03.parquet         # Dữ liệu thô Yellow
│   ├── green_tripdata_2026-03.parquet          # Dữ liệu thô Green
│   ├── yellow_tripdata_labeled.parquet         # Ground Truth Yellow (có is_anomaly)
│   └── green_tripdata_labeled.parquet          # Ground Truth Green (có is_anomaly)
└── hadoop/bin/                 # winutils.exe cho Spark trên Windows
```

---

## Yêu cầu Hệ thống

- **Hệ điều hành**: Windows 10/11
- **Phần mềm bắt buộc**:
  - [Docker Desktop](https://www.docker.com/products/docker-desktop/) (phải đang chạy)
  - [Python 3.9+](https://www.python.org/downloads/)
  - [Java 8 hoặc 11](https://www.oracle.com/java/technologies/javase-downloads.html) (bắt buộc để chạy Apache Spark)
- **RAM khuyến nghị**: tối thiểu 8GB (Spark driver dùng 4GB)
- **Dung lượng**: khoảng 500MB (datasets Yellow ~67MB, Yellow labeled ~224MB)

---

## Cài đặt & Khởi động

### Bước 0 — Bắt buộc: Mở Docker Desktop trước

Đảm bảo Docker Desktop đang chạy (status hiện **Engine Running**) trước khi tiếp tục.

### Bước 1 — Clone dự án

```bash
git clone <link-repo>
cd "lab đồ án/data-streams"
```

### Bước 2 — Khởi động toàn hệ thống (1 click)

```cmd
start_project.bat
```

Script thực hiện tuần tự:
1. Cài đặt tất cả thư viện Python (`pandas`, `pyarrow`, `kafka-python`, `pyspark`, `psycopg2-binary`, `streamlit`, `plotly`, `scikit-learn`, `numpy`).
2. Khởi động Docker: Zookeeper → Kafka → PostgreSQL (chờ health check pass).
3. Tạo Kafka topic `taxi_stream`.
4. Reset PostgreSQL schema (`reset_db.py`).
5. Mở 5 cửa sổ Terminal song song:
   - **SPARK PROCESSOR** — `spark_taxi_processor.py`
   - **WEB DASHBOARD** — `dashboard.py` trên port 8501
   - **BENCHMARK EVALUATOR** — `benchmark_evaluator.py` trên port 8502
   - **YELLOW PRODUCER** — `yellow_producer.py`
   - **GREEN PRODUCER** — `green_producer.py`

### Bước 3 — Truy cập giao diện

| Ứng dụng | URL |
|---|---|
| Main Dashboard (real-time) | http://localhost:8501 |
| Benchmark Evaluator (vs Ground Truth) | http://localhost:8502 |

---

## Dừng Hệ thống

```cmd
stop_project.bat
```

Script dừng tất cả tiến trình Python và chạy `docker compose down`.

---

## Điểm nhấn Kỹ thuật

### Ensemble Machine Learning (Dual-Model)

Hệ thống kết hợp 2 thuật toán bổ trợ nhau:

- **Isolation Forest** (200 estimators): phát hiện point outlier khi 1 feature lệch chuẩn đột ngột. Được tăng cường bằng sample weighting — các chuyến đi vi phạm business rules được tính trọng số ×3 trong quá trình huấn luyện.
- **MinCovDet (Robust Covariance)**: phát hiện multivariate outlier — tổ hợp nhiều feature cùng lúc lạ dù từng feature riêng lẻ vẫn bình thường. Dùng Mahalanobis distance với ngưỡng ở percentile 97.5.
- **Strict Voting**: một chuyến đi chỉ bị đánh dấu anomaly khi **cả IF và MCD cùng đồng ý** — giảm đáng kể false positive.

### Feature Engineering (14 Features)

| Feature | Loại | Mục đích |
|---|---|---|
| `trip_distance`, `fare_amount`, `passenger_count` | Raw | Thông tin cơ bản |
| `trip_duration_sec`, `avg_speed_kmh`, `fare_per_mile` | Derived (Spark) | Phát hiện tốc độ/giá bất thường |
| `hour_sin`, `hour_cos` | Cyclical | Mã hóa thời gian liên tục (23h sát 0h) |
| `log_distance`, `log_fare` | Log transform | Giảm skew phân phối đuôi dài |
| `speed_x_duration` | Interaction | Proxy quãng đường thực (phát hiện odometer gian lận) |
| `fare_distance_ratio` | Ratio | Nhạy hơn `fare_per_mile` với chuyến ngắn |
| `is_rush_hour`, `is_night` | Context flags | Phân biệt pattern theo thời điểm |

### Continuous Learning

Model không static — tự động retrain mỗi 25 batch trên rolling buffer 10,000 bản ghi gần nhất. Contamination rate được điều chỉnh adaptive: `0.8 × prior (3%) + 0.2 × observed_rate`, clamp trong `[1%, 4%]`.

### Ground Truth & Benchmark

File `ai_batch_labeler.py` chạy offline trên toàn bộ dữ liệu lịch sử (~4 triệu chuyến đi), tạo bộ nhãn chuẩn (`is_anomaly`, `ai_anomaly_score`, `ai_reason`) dùng Isolation Forest 300-estimator với max_samples=256,000. Dashboard `benchmark_evaluator.py` so sánh kết quả Streaming với Ground Truth này, xuất Precision/Recall/F1-Score và Confusion Matrix theo thời gian thực.

### PostgreSQL Schema

| Bảng | Nội dung |
|---|---|
| `normal_trips` | Tối đa 50 chuyến bình thường mỗi batch |
| `anomalous_trips` | Toàn bộ chuyến bất thường kèm `reason` |
| `pipeline_metrics` | Throughput, latency, anomaly rate, cost ước tính mỗi batch |

---

## Docker Services

| Service | Image | Port |
|---|---|---|
| Zookeeper | confluentinc/cp-zookeeper:7.1.1 | 2181 |
| Kafka | confluentinc/cp-kafka:7.1.1 | 9092 (internal), 9093 (external) |
| PostgreSQL | postgres:13 | 5432 |

Credentials PostgreSQL mặc định: user `admin`, password `password`, database `taxidb`.
