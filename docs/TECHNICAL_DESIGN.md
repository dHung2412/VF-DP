# Technical Design Document (Giai đoạn 0) — chốt trước khi viết code ETL

## 1. Quyết định GĐ0 (README §8) — ĐÃ CHỐT
| # | Câu hỏi | Chốt |
|---|---------|------|
| 1 | Xe nhận diện `charging_station_id` bằng gì | **BLE beacon từ trụ (primary) + QR app fallback**; PLC để dành |
| 2 | Phiên sạc vắt mốc 02h00 | **Phương án A: split** theo tỷ lệ thời gian trước/sau mốc (code `gold/aggregates._split_at_cutoff`) |
| 3 | Retention | Bronze 90d (-> Glacier), Silver 730d, Gold vĩnh viễn |
| 4 | Warehouse Gold | Dev: DuckDB; Prod: **ClickHouse** cho telemetry + Gold aggregate (tương thích Postgres qua DDL chuẩn) |
| 5 | Rollout firmware 24B | OTA theo đợt; decoder nhận diện `len==16/24`; metric `payload_len` giám sát tỷ lệ |

## 2. Ngưỡng nghiệp vụ
`speed>=5` moving; `soc<=20` low-battery; tolerance đối soát 15%; late window 7 ngày.
Chi tiết: `config/business_rules.yaml` (single source of truth, job đọc runtime).

## 3. Khung16B firmware cũ
16 byte đầu layout ident 24B, thiếu `charging_station_id/sequence_id/crc/reserved`
-> decode với `charging=0, sequence=None, crc_ok=True`. Ghi `payload_len` để tracking.

## 4. Idempotency & Backfill
- Silver dedup `(device_id, sequence_id, timestamp)`; Gold `DELETE+INSERT` theo `business_date`.
- Late data routing bằng `timestamp` gốc (không dùng `server_ts`).
- `jobs/backfill.py --end-date X --window 7` re-run rolling window.
- Unknown vehicle: `is_unknown_vehicle=true`, không fail batch; backfill khi `dim_vehicle` đủ.

## 5. Dung lượng & Small files
1,728 tỷ rec/ngày ~55GB thô. Buffer flush 128MB/10-15p -> file 100-150MB Parquet ZSTD,
partition `business_date/hour`. Xem `src/vfdp/ingest/buffer.py`.

## 6. Vận hành (GĐ4)
- DQ: `src/vfdp/dq/checks.py` (silver null/dup/range; recon mismatch ratio, mục tiêu pass >=98%).
- Giám sát: Kafka lag, Airflow duration, S3 growth (prod); local: đếm row/DQ log sau mỗi job.
- Retention áp bằng lifecycle policy S3 + `DELETE ... WHERE business_date < cutoff` ở warehouse.
