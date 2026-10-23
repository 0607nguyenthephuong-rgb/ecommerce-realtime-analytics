# Ecommerce realtime analytics

Pipeline mô phỏng giao dịch thương mại điện tử và phân tích dữ liệu theo luồng:

```text
CSV ──(phát lại theo event_time)──> Oracle ──(outbox đã commit)──> Kafka
                                  │                              ├─ analytics → Bronze → Silver → Gold
                                  └─ đơn hàng, thanh toán         └─ payments (bị từ chối)
```

Chỉ có **một Kafka broker** với hai topic đầu ra. CSV đi **thẳng vào Oracle**;
Kafka không đứng trước Oracle. Đây là mô phỏng thanh toán: không gọi cổng
thanh toán thật và không chuyển tiền.

## Điều kiện chạy

- WSL/Linux, Docker Compose, Java và Spark 3.5.8 có `spark-submit` trong `PATH`.
- Python trong conda env `ecommerce`; cài thư viện bằng `pip install -r requirements.txt`.
- Lần chạy Spark đầu tiên cần mạng để tải Kafka connector và Delta Lake.

Trong mỗi terminal:

```bash
conda activate ecommerce
export PYSPARK_PYTHON="$CONDA_PREFIX/bin/python"
export PYSPARK_DRIVER_PYTHON="$CONDA_PREFIX/bin/python"
```

## Chạy thử

Terminal 1 — khởi động Oracle, Kafka, outbox relay và các job Spark:

```bash
bash scripts/run_pipeline.sh
```

Terminal 2 — phát một CSV mẫu vào Oracle:

```bash
ECOMMERCE_DATA_DIR=examples bash scripts/run_producer.sh --max-events 3 --speed 1000000
```

Để dùng các file CSV trong `data/`, bỏ `ECOMMERCE_DATA_DIR=examples`. Thư mục
`data/` không được đưa lên GitHub vì bộ CSV gốc rất lớn. Có thể dùng
`--max-events 1000` để giới hạn lượt thử; bỏ tùy chọn này để đọc hết. Mỗi file
CSV cần được sắp xếp tăng dần theo `event_time`. Producer trộn nhiều file theo
thời gian sự kiện và giữ nhịp phát lại bằng `--speed` (mặc định `600`).

## Vai trò của từng script

| Script | Công việc |
| --- | --- |
| [`init_oracle_payments.sh`](scripts/init_oracle_payments.sh) | Khởi động Oracle, đợi database sẵn sàng, tạo user và các bảng giao dịch; `run_pipeline.sh` tự gọi script này. |
| [`run_pipeline.sh`](scripts/run_pipeline.sh) | Khởi động Kafka, relay Oracle outbox → Kafka, rồi chạy Bronze → Silver → Gold. Nhấn Ctrl+C để dừng các job. |
| [`run_producer.sh`](scripts/run_producer.sh) | Đọc CSV theo `event_time`, tạo `event_id` ổn định và ghi từng sự kiện trực tiếp vào Oracle. |
| [`reset_kafka.sh`](scripts/reset_kafka.sh) | Xóa và tạo lại **hai topic Kafka** sau khi đã lưu trữ toàn bộ lakehouse/checkpoint. Không xóa dữ liệu Oracle. |

`run_pipeline.sh` ghi log vào `logs/bronze.log`, `logs/silver.log`,
`logs/gold.log`, `logs/oracle_outbox.log`. Đổi nơi lưu bằng `ECOMMERCE_LOG_DIR`.

## Xử lý đơn hàng và tính toán

Oracle lưu dữ liệu tại `PAYMENT_CATALOG`, `PAYMENT_INGESTED_EVENTS`,
`PAYMENT_ORDERS`, `PAYMENT_TRANSACTIONS` và `PAYMENT_OUTBOX`. Sự kiện đầu tiên
của mỗi sản phẩm tạo giá trong catalog. Khi gặp sự kiện `purchase`, Oracle
tính `unit_price × quantity` từ **giá catalog**, rồi ghi đơn hàng, kết quả
thanh toán và outbox trong cùng transaction. Vì vậy giá `price` ở yêu cầu mua
có thể khác giá thực tế đã lưu trong Oracle.

CSV gốc không có `quantity` hoặc `payment_outcome`: mặc định số lượng là `1`
và thanh toán thành công. Có thể thêm hai cột này để mô phỏng số lượng khác
hoặc `payment_outcome=declined`. Đơn thành công và hành vi xem/thêm giỏ đi vào
topic analytics; đơn bị từ chối đi vào topic payments và không tính doanh thu
Gold.

Relay chỉ đọc outbox đã commit. Sau khi Kafka xác nhận đã nhận bản tin, relay
mới đánh dấu dòng outbox là đã phát. Nếu relay dừng giữa hai bước đó, Kafka có
thể nhận lại bản tin; Silver khử trùng lặp bằng `event_id`. Producer tạo
`event_id` từ `tên file:line number`, nên phát lại cùng CSV cũng không tạo đơn
hàng thứ hai trong Oracle.

Mặc định hai topic là `ecommerce.events.replay` (analytics) và
`ecommerce.payments` (đơn bị từ chối). Có thể đổi bằng `ECOMMERCE_TOPIC` và
`ECOMMERCE_PAYMENTS_TOPIC`. Kết nối Oracle dùng `ORACLE_DSN`,
`ORACLE_ADMIN_PASSWORD`, `ORACLE_APP_PASSWORD`; giá trị mặc định khớp
`docker-compose.yml` cho môi trường phát triển.

## Lakehouse và khởi động lại

| Lớp | Đường dẫn |
| --- | --- |
| Bronze | `lakehouse/bronze/ecommerce_events` (Parquet) |
| Silver | `lakehouse/silver/ecommerce_events_delta` (Delta) |
| Quarantine | `lakehouse/quarantine/ecommerce_events_delta` (Delta) |
| Gold | `lakehouse/gold/daily_product_metrics` (Delta) |

Silver kiểm tra bản tin và đưa dữ liệu lỗi vào Quarantine. Gold tổng hợp theo
ngày và sản phẩm: lượt xem, giỏ hàng, lượt mua, giá trị mua và số người dùng.
Checkpoint nằm trong `lakehouse/checkpoints/`; luôn giữ checkpoint cùng với
dữ liệu tương ứng khi khởi động lại.

`reset_kafka.sh` từ chối reset nếu lakehouse còn dữ liệu hoặc checkpoint.
Muốn làm lại từ đầu, dừng pipeline và lưu trữ toàn bộ `lakehouse/` trước.
**Reset Kafka không reset Oracle:** các dòng CSV đã xử lý vẫn được Oracle nhận
diện là trùng và sẽ không được phát lại.

## Kiểm tra tích hợp

```bash
python -m tests.test_oracle_payments
spark-submit --master 'local[2]' --packages io.delta:delta-spark_2.12:3.3.2 tests/test_silver_pipeline.py
spark-submit --master 'local[2]' --packages io.delta:delta-spark_2.12:3.3.2 tests/test_gold_pipeline.py
```
