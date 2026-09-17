# Kế hoạch thực thi: Hệ thống Data Pipeline & BI Phân tích Sạc Xe điện (Batch Architecture)

**Trạng thái:** Sẵn sàng thực thi  
**Đối tượng đọc:** Data Engineer, Backend Engineer, BI Engineer, Solution Architect  
**Quy mô xác nhận:** 100.000 xe điện, tần suất telemetry 5s/lần (~20.000 RPS trung bình, ~60.000 RPS đỉnh), tương đương **~1,73 tỷ bản ghi raw telemetry/ngày** (~40–55 GB nén/ngày).  
**Định hướng thiết kế:** **100% tập trung vào luồng BATCH (Batch Lakehouse/Warehouse Pipeline)** phục vụ bài toán Business Intelligence & phân tích vị trí mở rộng trạm sạc. **BỎ QUA TOÀN BỘ LUỒNG REAL-TIME** (tách riêng phần cảnh báo an toàn khẩn cấp cho hệ thống viễn thông chuyên biệt của xe, không gộp vào hạ tầng dữ liệu BI).

---

## 0. Tóm tắt điều hành

Dự án xây dựng pipeline dữ liệu end-to-end thu thập dữ liệu từ **100.000 xe điện**, dữ liệu chốt ngày từ **hệ thống trạm sạc**, và dữ liệu phân phối từ **chuỗi đại lý/cửa hàng**, lưu trữ và chuyển đổi theo mô hình **Medallion (Bronze → Silver → Gold Star Schema)** nhằm phục vụ Dashboard BI nội bộ ra quyết định quy hoạch mở rộng trạm sạc:
- Xác định điểm nóng tập trung nhiều xe sắp hết pin (`SOC ≤ 20%`) theo tọa độ và khung giờ.
- Đánh giá công suất, mức độ quá tải (`utilization rate`) của các trạm sạc theo hình thức sở hữu (`relationship_type`).
- Đối soát chéo giữa mức sạc thực tế ghi nhận trên xe và điện năng xuất ra từ trụ sạc.
- Đánh giá tương quan giữa lượng xe mới bán ra theo khu vực đại lý với hạ tầng trạm sạc hiện hữu.

### Các nguyên tắc kiến trúc bắt buộc (Tập trung Batch):
1. **Loại bỏ hoàn toàn luồng Real-time:** Không xây dựng cụm streaming alert thời gian thực (SLA tính bằng giây). Cắt giảm toàn bộ chi phí tài nguyên và vận hành của streaming engine (như Flink/Spark Streaming 24/7). Trường `alert_flags` chỉ lưu trữ lịch sử để phục vụ phân tích hồi cứu (Batch Diagnostics).
2. **Hạ tầng Ingest dùng công nghệ chuẩn làm Buffer:** MQTT (EMQX) tiếp nhận kết nối xe; Apache Kafka đóng vai trò Ingestion Buffer chống sốc tải (hấp thụ đỉnh 60.000 RPS) và dump trực tiếp dạng batch xuống Data Lake (S3/MinIO).
3. **Chuẩn hóa Ngày nghiệp vụ (Business Date: 02h00 → 02h00):** Khung giờ chốt ngày phân tích được ấn định từ **02h00 sáng hôm nay đến 02h00 sáng hôm sau**, hoàn toàn đồng bộ với thời điểm trạm sạc chốt và gửi dữ liệu ngày lúc 02h00 sáng.
4. **Data Model Star Schema chuẩn hóa:** Ánh xạ 1-1 với bản thảo ERD tại [erd_telemetry_xe_dien_sac.html](file:///home/admin1/Work/repo/Vibe/VF-DP/erd_telemetry_xe_dien_sac.html), tối ưu cho truy vấn phân tích đa chiều trên BI Dashboard.

---

## 1. Đặc tả dữ liệu nguồn (Data Specifications)

### 1.1 Wire Frame từ xe — 24 byte nhị phân
Xe truyền gói tin nhị phân chuẩn 24 byte qua giao thức MQTT:

| Offset | Trường dữ liệu | Kiểu dữ liệu | Kích thước | Ghi chú kỹ thuật |
| :--- | :--- | :--- | :--- | :--- |
| **0–2** | `device_id` | `uint24` | 3B | ID số nội bộ, map 1-1 với `device_id_display` (VD: `VF7_0000001`) qua `dim_vehicle` |
| **3–4** | `timestamp_offset`| `uint16` | 2B | Offset giây so với base epoch (chuẩn hóa thành UTC timestamp ở Silver) |
| **5** | `speed` | `uint8` | 1B | Vận tốc tức thời (km/h) |
| **6** | `soc` | `uint8` | 1B | State of Charge (%) từ 0 đến 100 |
| **7** | `battery_temp` | `int8` | 1B | Nhiệt độ pin (°C) |
| **8–9** | `alert_flags` | `uint16` | 2B | Giá trị 0–10 (mức độ cảnh báo xe). **Trong luồng Batch:** Lưu trữ phục vụ audit/thống kê lỗi hỏng theo lô, **không** kích hoạt cảnh báo tức thời |
| **10–12**| `lat_encoded` | `int24` | 3B | Vĩ độ mã hóa: `lat = lat_encoded / 100000.0` |
| **13–15**| `lon_encoded` | `int24` | 3B | Kinh độ mã hóa: `lon = lon_encoded / 100000.0` |
| **16–17**| `charging_station_id`| `uint16` | 2B | `0` = không sạc; `> 0` = ID trạm xe đang cắm sạc |
| **18–19**| `sequence_id` | `uint16` | 2B | Bộ đếm tuần tự theo từng xe (per-device) dùng để phát hiện gói tin mất/khử trùng (dedup) |
| **20–21**| `crc16_checksum` | `uint16` | 2B | Checksum tính trên 20 byte đầu (0–19); validate ngay khi ingest |
| **22–23**| `reserved` | `uint16` | 2B | Dự phòng mở rộng, giá trị mặc định = 0 |

**Quy tắc xử lý bắt buộc:**
- **Kiểm tra CRC16:** Bản ghi lỗi CRC16 bị loại bỏ hoặc đưa vào Dead Letter Queue (DLQ) để đo lường tỷ lệ lỗi đường truyền (`corrupt_frame_count`).
- **Hỗ trợ tương thích ngược (Backward Compatibility):** Hỗ trợ gói tin 16 byte (firmware cũ) và 24 byte (firmware mới). Bộ giải mã (Decoder) tự động nhận diện dựa trên độ dài payload (`payload_length = 16` hoặc `24`).
- **Khóa bảo mật vật lý (`private_id`):** Chỉ lưu trong `dim_vehicle` tại Data Warehouse, tuyệt đối không xuất hiện trên gói tin truyền dẫn.

### 1.2 In-Memory Record tại Ingestion Buffer — 32 byte
Khi tiếp nhận gói tin từ MQTT/Kafka, mỗi record được đóng gói sơ bộ:
- `server_timestamp`: `uint64` (8 byte) — Thời điểm server nhận bản tin (dùng để đo lường độ trễ mạng).
- `raw_payload`: `uint8[24]` (24 byte) — Dữ liệu nhị phân nguyên bản.
- *Lưu ý:* `client_ip` không đưa vào payload phân tích mà chỉ lưu ở connection log của Gateway nếu cần kiểm toán hạ tầng.

### 1.3 Dữ liệu trạm sạc (Batch hàng ngày, nhận lúc 02h00 sáng)
- **Chu kỳ:** 1 lần/ngày lúc 02h00 sáng, tương ứng với thời điểm chốt ngày nghiệp vụ.
- **Nội dung:** `station_id`, `station_status`, `total_charging`, `total_charging_time`, `total_charging_energy`, kèm danh sách `device_id` tham gia sạc.
- **Vai trò:** Dùng để đối soát chéo với các phiên sạc được nhận diện từ dữ liệu telemetry của xe.

### 1.4 Dữ liệu đại lý / cửa hàng phân phối (Batch hàng tháng)
- **Chu kỳ:** Ngày 01 hàng tháng.
- **Nội dung:** `distributor_id`, `province`, `city`, `location`, danh sách xe đã bàn giao kèm `device_id` và phân khúc xe (`segment`).

### 1.5 Cơ chế xử lý dữ liệu Batch đặc thù
- **Late-Arriving Data (Dữ liệu gửi bù):** Khi xe đi vào vùng mất sóng (hầm để xe, cao tốc vùng sâu), telemetry được lưu trên bộ nhớ đệm của xe và gửi bù hàng loạt khi có kết nối lại. Pipeline Batch sử dụng `timestamp` gốc của gói tin để định tuyến dữ liệu vào đúng partition `business_date` (áp dụng cửa sổ trễ cho phép 3–7 ngày khi chạy đối soát/backfill).
- **Deduplication (Khử trùng lặp):** Khử trùng lặp bản tin telemetry trong cùng một batch dựa trên khóa tổng hợp `(device_id, sequence_id, timestamp)`.
- **Unknown Vehicle Handling:** Trường hợp xe mới bán ra đã gửi telemetry nhưng danh mục đại lý chưa kịp đồng bộ vào `dim_vehicle`: Dữ liệu telemetry vẫn được nạp vào Silver với cờ `is_unknown_vehicle = true`, lưu tạm vào unmapped partition để backfill tự động khi `dim_vehicle` cập nhật.

---

## 2. Data Model — ERD (Gold Layer, Star Schema)

Mô hình dữ liệu được thiết kế theo chuẩn Star Schema, tham chiếu trực tiếp từ file [erd_telemetry_xe_dien_sac.html](file:///home/admin1/Work/repo/Vibe/VF-DP/erd_telemetry_xe_dien_sac.html).

### 2.1 Bảng Chiều (Dimension Tables)
1. **`DIM_VEHICLE`** (Chiều xe):
   - `device_id_numeric` (PK, int): Mã ID số nội bộ của thiết bị.
   - `device_id_display` (string): Mã hiển thị công khai (VD: `VF7_0000001`).
   - `private_id` (string): Khóa vật lý xuất xưởng dùng cho bảo hành/thu hồi.
   - `model` (string): Dòng xe (VF5, VF7, VF8, VF9,...).
   - `segment` (string): Phân khúc (A, B, C, D, E).
   - `battery_capacity_kwh` (float): Dung lượng pin khả dụng (kWh).
   - `distributor_id` (FK, int): Đại lý bán xe.
   - `sale_date` (date): Ngày bàn giao xe.
2. **`DIM_DISTRIBUTOR`** (Chiều đại lý/cửa hàng):
   - `distributor_id` (PK, int): Mã đại lý.
   - `province` (string): Tỉnh/Thành phố.
   - `city` (string): Quận/Huyện/Thị xã.
   - `location` (string): Tọa độ / địa chỉ chi tiết (hỗ trợ SCD Type 2 nếu di dời vị trí).
3. **`DIM_STATION`** (Chiều trạm sạc):
   - `station_id` (PK, int): Mã trạm sạc.
   - `lat` (float), `lon` (float): Tọa độ địa lý của trạm sạc.
   - `car_chargers` (int): Số lượng cổng sạc ô tô.
   - `moto_chargers` (int): Số lượng cổng sạc xe máy.
4. **`BRIDGE_STATION_DISTRIBUTOR`** (Bảng cầu quan hệ trạm sạc – đại lý):
   - `station_id` (FK, int), `distributor_id` (FK, int).
   - `relationship_type` (string): Hình thức liên kết (`owned_by_distributor`, `partner`, `public`).
5. **`DIM_DATE`** (Chiều ngày nghiệp vụ):
   - `business_date` (PK, date): Ngày nghiệp vụ chốt theo chu kỳ 02h00 sáng đến 02h00 sáng hôm sau.
   - `calendar_date` (date), `day_of_week` (int), `month` (int), `quarter` (int), `year` (int), `is_weekend` (boolean).

### 2.2 Bảng Sự kiện (Fact Tables)
1. **`FACT_VEHICLE_TELEMETRY`** *(Tầng Silver — Partition theo `business_date` & `hour`)*:
   - `device_id` (FK, int), `timestamp` (datetime), `speed` (int), `soc` (int), `battery_temp` (int), `alert_flags` (int), `charging_station_id` (FK, int), `lat_encoded` (float), `lon_encoded` (float).
   - Đóng vai trò tầng dữ liệu chi tiết đã làm sạch, phục vụ tính toán aggregate lên các bảng Fact tầng Gold.
2. **`FACT_VEHICLE_DAILY_STATE`** *(Tầng Gold)*:
   - Khóa: `(device_id, business_date)`.
   - Các chỉ số tổng hợp: `minutes_moving` (tổng số phút di chuyển trong ngày), `min_soc` (mức pin thấp nhất trong ngày), `max_soc` (mức pin cao nhất trong ngày), `charging_sessions_count` (số lần cắm sạc trong ngày).
3. **`FACT_CHARGING_SESSION`** *(Tầng Gold)*:
   - Khóa: `(device_id, station_id, business_date)` (hoặc session id).
   - Các chỉ số: `total_charging_time` (phút), `total_charging_energy` (kWh).
   - Đối soát chéo giữa mức pin tăng từ telemetry xe và điện năng từ trạm.
4. **`FACT_STATION_DAILY`** *(Tầng Gold)*:
   - Khóa: `(station_id, business_date)`.
   - Các chỉ số: `total_charging` (tổng lượt sạc trong ngày), `total_charging_time` (tổng thời gian phục vụ), `total_charging_energy` (tổng kWh cấp ra).
5. **`FACT_DISTRIBUTOR_MONTHLY`** *(Tầng Gold)*:
   - Khóa: `(distributor_id, month, segment)`.
   - Các chỉ số: `vehicles_sold` (số lượng xe đã bán theo từng phân khúc trong tháng).

---

## 3. Kiến trúc tổng thể Luồng Batch (Batch Lakehouse Architecture)

```
Xe điện (100K xe, 5s/lần, payload 24B)
        │
        ▼
  MQTT Broker (EMQX)
        │
        ▼
  Kafka Topic: telemetry-raw (Ingestion Buffer chống sốc tải 20K–60K RPS)
        │
        ▼  [Kafka Connect S3 Sink / Ingestion Consumer (Buffer 128MB / 10 phút)]
┌─────────────────────────────────────────────────────────────────────────────┐
│                           BRONZE LAYER (Data Lake)                          │
│  - raw_telemetry (Parquet, partition: business_date=YYYY-MM-DD/hour=HH)     │
│  - raw_station_batch (nhận lúc 02h00 sáng hàng ngày)                         │
│  - raw_distributor_batch (nhận ngày 01 hàng tháng)                          │
└──────────────────────────────────────┬──────────────────────────────────────┘
                                       │
                                       ▼  [Batch Job: Hourly / Scheduled (Spark/DuckDB/dbt)]
┌─────────────────────────────────────────────────────────────────────────────┐
│                           SILVER LAYER (Cleaned & Enriched)                 │
│  - Validate CRC16, Decode 24B/16B, Convert Lat/Lon float                     │
│  - Deduplicate theo (device_id, sequence_id, timestamp)                     │
│  - Feature Engineering & State Detection: is_moving, is_charging, is_idle   │
│  - Handle Late-arriving Data & Unknown Vehicles                             │
│  - Bảng: fact_vehicle_telemetry                                             │
└──────────────────────────────────────┬──────────────────────────────────────┘
                                       │
                                       ▼  [Daily Batch Job: Chạy lúc 02h30 sáng sau khi chốt ngày]
┌─────────────────────────────────────────────────────────────────────────────┐
│                         GOLD LAYER (Star Schema Warehouse)                  │
│  - fact_vehicle_daily_state (tính minutes_moving, min/max soc)              │
│  - fact_charging_session (đối soát chéo telemetry xe vs dữ liệu trạm)       │
│  - fact_station_daily (tổng hợp tải, thời gian, công suất trạm)             │
│  - fact_distributor_monthly (cập nhật doanh số theo tháng)                  │
│  - Bảng chiều chuẩn hóa: dim_vehicle, dim_distributor, dim_station, dim_date│
└──────────────────────────────────────┬──────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                              BI & SERVING LAYER                             │
│  - BI Engine: Metabase / Apache Superset                                    │
│  - Dashboard 1: Bản đồ nhiệt xe cạn pin (SOC ≤ 20%) theo khu vực/khung giờ  │
│  - Dashboard 2: Tỷ lệ quá tải trạm sạc vs Lượng xe mới bán theo showroom    │
│  - Dashboard 3: Đề xuất vị trí mở rộng trạm sạc dựa trên mật độ và nhu cầu  │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Tại sao loại bỏ hoàn toàn luồng Real-time?
1. **Đúng với bản chất bài toán:** Quy hoạch và mở rộng trạm sạc là bài toán chiến lược mang tầm trung và dài hạn (theo dõi theo ngày, tuần, tháng). Chạy batch 10–60 phút hoặc daily batch hoàn toàn đáp ứng xuất sắc nhu cầu ra quyết định mà không cần streaming sub-second.
2. **Tiết kiệm tài nguyên và chi phí vận hành:** Xử lý luồng streaming cho 100.000 xe (20.000–60.000 RPS) đòi hỏi duy trì cluster streaming 24/7 với chi phí CPU/RAM rất cao. Chuyển sang batch cho phép dồn tài nguyên xử lý theo lịch định kỳ (scheduled compute), dễ dàng co giãn (elastic scaling).
3. **Phân tách trách nhiệm hệ thống:** Trách nhiệm cảnh báo tức thời cho an toàn xe thuộc về đội ngũ Telematics & Firmware với cụm server socket độc lập kết nối đến trung tâm cứu hộ, không đè nặng SLA khắt khe lên hạ tầng phân tích dữ liệu BI.

---

## 4. Chi tiết quy trình Batch ETL & Xử lý kỹ thuật

### 4.1 Tầng Bronze: Ingestion & Tránh "Small Files Problem"
- **Dung lượng ước tính:** 100.000 xe × (86.400s / 5s) = **1,728 tỷ records/ngày**.
- Mỗi record 24B nhị phân + 8B server timestamp = 32B. Dung lượng thô ~55 GB/ngày.
- **Chiến lược xử lý Small Files:**
  - Không ghi trực tiếp từng bản tin nhỏ vào Object Storage.
  - Sử dụng Kafka Connect (S3 Sink Connector) hoặc Ingestion Worker gom dữ liệu vào bộ nhớ đệm: flush file khi đạt kích thước **128 MB** hoặc sau mỗi **10–15 phút**.
  - Định dạng lưu trữ: **Parquet** (nén **Snappy/ZSTD**), phân vùng:
    `s3://lakehouse/bronze/telemetry/business_date=YYYY-MM-DD/hour=HH/`

### 4.2 Tầng Silver: Làm sạch, Giải mã & Nhận diện trạng thái
Job Batch tầng Silver (chạy định kỳ 15–30 phút hoặc 1 giờ một lần):
1. **Kiểm tra CRC16:** Bản tin không hợp lệ bị loại bỏ, ghi nhận số lượng vào metric `corrupt_frame_count`.
2. **Giải mã nhị phân:**
   - Decode tọa độ: $\text{lat} = \text{lat\_encoded} / 100000.0$, $\text{lon} = \text{lon\_encoded} / 100000.0$.
   - Tính toán `timestamp` (UTC) từ `timestamp_offset` + base epoch.
3. **Khử trùng lặp (Deduplication):** Loại bỏ bản tin trùng lặp bằng window function partition theo `(device_id, sequence_id)` sắp xếp theo `timestamp`.
4. **Bóc tách trạng thái xe (State Detection Engine):**
   - **Đang di chuyển (`is_moving`):** `speed ≥ 5 km/h` (ngưỡng lọc nhiễu GPS khi đỗ).
   - **Đang sạc (`is_charging`):** `charging_station_id > 0`.
   - **Đỗ / Chờ (`is_idle`):** `speed < 5 km/h` và `charging_station_id == 0`.
   - **Pin yếu (`is_low_battery`):** `soc ≤ 20%`.
5. **Ghi vào `fact_vehicle_telemetry`:** Bảng Parquet tối ưu truy vấn phục vụ tầng Gold.

### 4.3 Tầng Gold: Đối soát chéo & Tổng hợp Star Schema
Job Batch ngày (chạy vào lúc **02h30 sáng**, sau khi trạm sạc gửi dữ liệu ngày lúc 02h00 sáng):
1. **Tổng hợp `fact_vehicle_daily_state`:**
   - Gom nhóm theo `(device_id, business_date)`.
   - $\text{minutes\_moving} = \sum (\text{khoảng thời gian } \text{is\_moving} = 1)$.
   - $\text{min\_soc} = \min(soc)$, $\text{max\_soc} = \max(soc)$.
   - $\text{charging\_sessions\_count} = \text{số phiên sạc độc lập trong ngày}$.
2. **Tổng hợp và đối soát `fact_charging_session`:**
   - Bóc tách các phiên sạc từ telemetry của từng xe (thời điểm cắm và rút sạc tại `charging_station_id`).
   - Join trực tiếp với dữ liệu chốt từ trạm sạc theo `(device_id, station_id, business_date)`.
   - **Data Quality Check (Đối soát chéo):** So sánh năng lượng sạc đo từ trạm (`total_charging_energy`) với độ tăng pin của xe:
     $$\Delta E_{\text{ước tính}} = (\text{soc}_{\text{kết thúc}} - \text{soc}_{\text{bắt đầu}}) \times \text{battery\_capacity\_kwh}$$
     Nếu sai lệch vượt quá ngưỡng cho phép (VD: $> 15\%$), gắn cờ cảnh báo `reconciliation_mismatch`.
3. **Tổng hợp `fact_station_daily`:**
   - Gom nhóm theo `(station_id, business_date)`.
   - Tính tổng số lượt sạc, tổng thời lượng sạc, tổng điện năng cấp ra.
   - Tính toán hệ số sử dụng trạm: $\text{utilization\_rate} = \frac{\text{total\_charging\_time}}{\text{số cổng} \times 24 \times 60}$.
4. **Xử lý các ca biên (Edge Cases):**
   - **Phiên sạc xuyên mốc 02h00 sáng:** Thống nhất quy tắc tách thời gian (split) theo mốc 02h00 hoặc gán phiên sạc vào ngày bắt đầu (chốt dứt điểm ở Giai đoạn 0).
   - **Unknown Vehicle:** Bản ghi không tìm thấy trong `dim_vehicle` được gán vào `device_id_numeric = -1` (hoặc nhóm unmapped), sau đó re-run backfill khi master data đại lý được cập nhật.

---

## 5. Lộ trình thực thi theo giai đoạn (Tập trung Batch)

### GIAI ĐOẠN 0 — Thiết kế chi tiết & Chốt quy tắc nghiệp vụ (2–3 tuần)
*Chủ trì: Solution Architect + Data Lead + Chuyên gia trạm sạc/đại lý*
- [ ] Chốt cơ chế xe nhận biết `charging_station_id` khi cắm sạc (BLE broadcast từ trụ / QR / NFC).
- [ ] Chốt quy tắc xử lý phiên sạc vắt qua mốc 02h00 sáng (split hay assign to start date).
- [ ] Chốt ngưỡng nghiệp vụ: Ngưỡng `speed` xác định xe di chuyển (VD: $5 \text{ km/h}$), ngưỡng SOC cạn pin ($20\%$).
- [ ] Phân loại toàn bộ danh mục trạm sạc theo `relationship_type` (`partner`, `public`, `owned_by_distributor`).
- [ ] Chốt DDL chi tiết cho các bảng Bronze, Silver và Gold (Star Schema) dựa trên [erd_telemetry_xe_dien_sac.html](file:///home/admin1/Work/repo/Vibe/VF-DP/erd_telemetry_xe_dien_sac.html).
- **Tiêu chí hoàn thành:** Tài liệu thiết kế kỹ thuật (Technical Design Document) được ký duyệt, không còn điểm TBD.

### GIAI ĐOẠN 1 — Hạ tầng Ingest Buffer & Bronze Data Lake (3–4 tuần)
*Chủ trì: Data Engineer + DevOps/Platform Engineer*
- [ ] Triển khai cụm EMQX tiếp nhận kết nối MQTT từ xe điện.
- [ ] Triển khai Kafka đóng vai trò Ingestion Buffer chịu tải đỉnh 60.000 RPS.
- [ ] Cấu hình Kafka Connect S3 Sink (hoặc Ingest Worker) dump dữ liệu nhị phân xuống S3/MinIO dạng Parquet (ZSTD), partition theo `business_date` và `hour`.
- [ ] Thiết lập cơ chế kiểm tra CRC16 và định tuyến gói hỏng vào DLQ.
- [ ] Viết load test generator giả lập 100.000 xe gửi telemetry 5s/lần, kiểm chứng độ ổn định và đo lường kích thước file nén trên S3.
- **Tiêu chí hoàn thành:** Chịu tải giả lập liên tục 24h ở mức 60.000 RPS mà không mất mát dữ liệu, kích thước các file Parquet phân bổ tối ưu (100–150 MB/file).

### GIAI ĐOẠN 2 — Xây dựng Batch ETL Pipeline (Silver & Gold) (4–5 tuần)
*Chủ trì: Data Engineer / Analytics Engineer*
- [ ] Xây dựng job Silver: Giải mã payload 24B/16B, decode tọa độ lat/lon, khử trùng lặp `(device_id, sequence_id, timestamp)`.
- [ ] Xây dựng State Detection Engine: Gắn nhãn trạng thái di chuyển, sạc, chờ, cạn pin.
- [ ] Xây dựng job Gold (chạy 02h30 sáng hàng ngày):
  - Tính toán `fact_vehicle_daily_state`.
  - Đối soát chéo telemetry vs dữ liệu trạm sạc, tạo `fact_charging_session`.
  - Tính toán hiệu suất trạm `fact_station_daily`.
- [ ] Xây dựng job nạp dữ liệu hàng tháng đại lý vào `fact_distributor_monthly`.
- [ ] Thiết lập quy trình Airflow/Dagster điều phối các DAGs với sensor đợi file lúc 02h00 sáng.
- [ ] Cài đặt cơ chế xử lý Late-arriving data và Backfill tự động cho Unknown Vehicles.
- **Tiêu chí hoàn thành:** Dữ liệu nạp đầy đủ vào Data Warehouse Star Schema, kiểm toán đối soát dữ liệu chéo pass $\ge 98\%$ số phiên sạc.

### GIAI ĐOẠN 3 — Xây dựng BI Dashboard & Phân tích quy hoạch (3–4 tuần)
*Chủ trì: BI Engineer / Data Analyst*
- [ ] Triển khai công cụ BI (Metabase / Apache Superset) kết nối trực tiếp vào Gold Warehouse.
- [ ] **Dashboard 1 — Bản đồ nhiệt cạn pin:** Heatmap hiển thị mật độ xe có `min_soc ≤ 20%` theo vị trí địa lý và khung giờ trong ngày.
- [ ] **Dashboard 2 — Hiệu suất và quá tải trạm sạc:** Tỷ lệ lấp đầy (`utilization_rate`), doanh thu điện sạc, thời gian cao điểm của từng trạm sạc theo hình thức sở hữu.
- [ ] **Dashboard 3 — Tương quan doanh số & Quy hoạch:** Đối chiếu số xe mới bán ra theo từng quận/huyện của showroom với công suất trạm sạc hiện tại $\rightarrow$ Đề xuất danh sách khu vực ưu tiên bổ sung trụ sạc mới.
- [ ] Phân quyền truy cập: Ban lãnh đạo/BI xem toàn quốc; Đại lý xem trong địa bàn phụ trách.
- **Tiêu chí hoàn thành:** Stakeholder nghiệm thu dashboard, trả lời trực quan câu hỏi "Nên mở thêm trạm sạc công suất bao nhiêu tại quận/huyện nào?".

### GIAI ĐOẠN 4 — Vận hành, Quản trị Data Quality & Tối ưu hóa (Liên tục)
*Chủ trì: Toàn bộ team Data & Ops*
- [ ] Thiết lập Data Quality Checks tự động hàng ngày (Great Expectations / Soda Core).
- [ ] Thiết lập chính sách lưu trữ (Data Lifecycle / Retention Policy): Bronze lưu trữ 90 ngày (hoặc chuyển Glacier), Silver lưu 1–2 năm, Gold lưu vĩnh viễn.
- [ ] Tối ưu hóa truy vấn trên Gold Warehouse (phân vùng theo `business_date`, sort keys/indexing trên `station_id`, `device_id`).
- [ ] Giám sát tài nguyên: Kafka lag, Airflow execution duration, S3 storage growth.

---

## 6. Tech Stack chuẩn hóa cho Luồng Batch

| Thành phần | Công nghệ lựa chọn | Rationale / Ghi chú |
| :--- | :--- | :--- |
| **Giao thức thiết bị** | MQTT | Truyền payload nhị phân 24B siêu nhẹ qua mạng di động 4G/LTE |
| **Broker kết nối** | EMQX | Chịu tải đồng thời > 100.000 kết nối MQTT ổn định |
| **Ingestion Buffer** | Apache Kafka | Làm bộ đệm chống shock tải cho 20K–60K RPS; retention 1–3 ngày |
| **Bronze Storage (Data Lake)** | S3 / MinIO | Lưu trữ file Parquet nén Snappy/ZSTD, chi phí thấp, mở rộng không giới hạn |
| **Batch Compute Engine** | Apache Spark / DuckDB / Trino | Xử lý hàng tỷ bản ghi mỗi ngày với tốc độ cao và khả năng scale ngang |
| **Transform & Modeling** | dbt (dbt-core) | Quản lý logic SQL, version control, lineage và automated testing |
| **Orchestration** | Apache Airflow / Dagster | Quản lý dependency các luồng dữ liệu (5 phút, 1 giờ, ngày 02h sáng, tháng) |
| **Gold Warehouse** | ClickHouse / PostgreSQL | Tối ưu hóa cho truy vấn phân tích OLAP đa chiều |
| **BI & Visualization** | Metabase / Apache Superset | Nền tảng phân tích trực quan hóa dữ liệu, mã nguồn mở, dễ tích hợp |

---

## 7. Ma trận rủi ro & Phương án giảm thiểu (Batch Focus)

| # | Rủi ro kỹ thuật | Mức độ | Phương án giảm thiểu |
| :- | :--- | :--- | :--- |
| **1** | **Khối lượng dữ liệu cực lớn (~1,73 tỷ record/ngày)** gây nghẽn I/O và chi phí tính toán cao | Cao | Chuyển đổi nhị phân sang định dạng cột Parquet có nén cao (ZSTD). Phân vùng chặt chẽ theo `business_date` và `hour`. Sử dụng kỹ thuật partition pruning trong truy vấn. |
| **2** | **Vấn đề Small Files trên S3** khi flush liên tục từ Kafka | Cao | Cấu hình kích thước buffer tối thiểu 128 MB hoặc time window 10–15 phút tại Kafka Connect trước khi commit file lên S3. Chạy compaction job định kỳ nếu cần. |
| **3** | **Lệch số liệu đối soát giữa xe và trạm sạc** | Trung bình | Xây dựng quy tắc đối soát chéo ở tầng Gold: So sánh $\Delta \text{SOC} \times \text{Dung lượng pin}$ với `total_charging_energy` từ trạm; chấp nhận sai số tổn hao sạc thực tế (10–15%). |
| **4** | **Late-arriving data (xe mất sóng gửi bù)** làm lệch aggregate của ngày đã chốt | Trung bình | Thiết kế cơ chế Idempotent Batch Job và Upsert theo `(device_id, business_date)` với cửa sổ trượt (rolling window 3–7 ngày gần nhất). |
| **5** | **Xe mới bán chưa có trong `dim_vehicle` (Unknown vehicle)** | Trung bình | Tách bản ghi vào staging/unmapped partition, gắn cờ `is_unknown_vehicle = true`, không để batch job fail; tự động backfill sau khi Master Data cập nhật. |
| **6** | **Phiên sạc vắt qua mốc 02h00 sáng** | Thấp | Thống nhất quy tắc tách thời gian (split session) trước và sau 02h00 hoặc quy ước gán phiên vào ngày kết thúc sạc. |

---

## 8. Danh sách quyết định cần chốt trước khi viết code (Giai đoạn 0)

1. **Phương thức xe nhận diện trạm sạc:** BLE beacon tại trụ, quét QR code trên ứng dụng, hay trao đổi tín hiệu PLC (Power Line Communication) qua chuẩn sạc.
2. **Quy tắc phiên sạc vắt ngày:** Chốt giữa 2 phương án: (A) Tách đôi phiên sạc theo mốc 02h00 sáng hay (B) Gán toàn bộ phiên sạc vào ngày bắt đầu/ngày kết thúc.
3. **Chính sách Retention cụ thể:** Số ngày lưu trữ Bronze (đề xuất: 60–90 ngày), Silver (1–2 năm), Gold (vĩnh viễn).
4. **Lựa chọn Warehouse tầng Gold:** ClickHouse (tối ưu nhất nếu cần tốc độ query trực tiếp hàng tỷ dòng) hay PostgreSQL (đơn giản, dễ vận hành nếu chỉ query dữ liệu đã aggregate ở Gold).
5. **Kế hoạch rollout firmware xe 24 byte:** Lịch trình cập nhật OTA cho 100.000 xe theo từng đợt để đội ngũ Data kiểm soát tỷ lệ frame 16B vs 24B.