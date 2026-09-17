-- ============================================================================
-- VF-DP DDL — Star Schema (tương thích DuckDB / PostgreSQL)
-- Tham chiếu ERD: erd_telemetry_xe_dien_sac.html + README §2
-- Business date: 02h00 -> 02h00 hôm sau (xem config/business_rules.yaml)
-- ============================================================================

-- ---------------- DIM_DATE ----------------
CREATE TABLE IF NOT EXISTS dim_date (
  business_date DATE PRIMARY KEY,
  calendar_date DATE NOT NULL,
  day_of_week INTEGER NOT NULL,   -- ISO 1=Mon..7=Sun
  month INTEGER NOT NULL,
  quarter INTEGER NOT NULL,
  year INTEGER NOT NULL,
  is_weekend BOOLEAN NOT NULL
);

-- ---------------- DIM_DISTRIBUTOR ----------------
CREATE TABLE IF NOT EXISTS dim_distributor (
  distributor_id INTEGER PRIMARY KEY,
  province VARCHAR NOT NULL,
  city VARCHAR NOT NULL,
  location VARCHAR
);

-- ---------------- DIM_VEHICLE ----------------
CREATE TABLE IF NOT EXISTS dim_vehicle (
  device_id_numeric INTEGER PRIMARY KEY,
  device_id_display VARCHAR NOT NULL UNIQUE,
  private_id VARCHAR NOT NULL,          -- khóa vật lý, chỉ ở warehouse, không lên xe
  model VARCHAR NOT NULL,               -- VF5/VF7/VF8/VF9...
  segment VARCHAR NOT NULL,             -- A/B/C/D/E
  battery_capacity_kwh DOUBLE NOT NULL,
  distributor_id INTEGER REFERENCES dim_distributor(distributor_id),
  sale_date DATE
);

-- ---------------- DIM_STATION ----------------
CREATE TABLE IF NOT EXISTS dim_station (
  station_id INTEGER PRIMARY KEY,
  lat DOUBLE NOT NULL,
  lon DOUBLE NOT NULL,
  car_chargers INTEGER NOT NULL DEFAULT 0,
  moto_chargers INTEGER NOT NULL DEFAULT 0
);

-- ---------------- BRIDGE_STATION_DISTRIBUTOR ----------------
CREATE TABLE IF NOT EXISTS bridge_station_distributor (
  station_id INTEGER NOT NULL REFERENCES dim_station(station_id),
  distributor_id INTEGER NOT NULL REFERENCES dim_distributor(distributor_id),
  relationship_type VARCHAR NOT NULL,   -- owned_by_distributor | partner | public
  PRIMARY KEY (station_id, distributor_id)
);

-- ---------------- SILVER: FACT_VEHICLE_TELEMETRY ----------------
-- Partition vật lý trên Data Lake: business_date=YYYY-MM-DD/hour=HH/
-- Bảng warehouse tương ứng (DuckDB) giữ thêm cột partition để pruning.
CREATE TABLE IF NOT EXISTS fact_vehicle_telemetry (
  device_id INTEGER NOT NULL,
  timestamp TIMESTAMP NOT NULL,         -- UTC, đã chuẩn hóa từ timestamp_offset + base epoch
  business_date DATE NOT NULL,
  hour INTEGER NOT NULL,                -- 0..23 (giờ VN, phục vụ partition pruning)
  speed INTEGER NOT NULL,
  soc INTEGER NOT NULL,
  battery_temp INTEGER NOT NULL,
  alert_flags INTEGER NOT NULL,
  charging_station_id INTEGER NOT NULL DEFAULT 0,
  lat DOUBLE NOT NULL,
  lon DOUBLE NOT NULL,
  sequence_id INTEGER,
  is_moving BOOLEAN NOT NULL,
  is_charging BOOLEAN NOT NULL,
  is_idle BOOLEAN NOT NULL,
  is_low_battery BOOLEAN NOT NULL,
  is_unknown_vehicle BOOLEAN NOT NULL DEFAULT FALSE,
  payload_len INTEGER NOT NULL          -- 16 | 24 (giám sát rollout firmware)
  -- Không đặt PK ở Silver vì ~1.7 tỷ dòng/ngày; dedup bằng job theo
  -- (device_id, sequence_id, timestamp); index tạo ở engine prod (ClickHouse ORDER BY).
);

-- ---------------- GOLD: FACT_VEHICLE_DAILY_STATE ----------------
CREATE TABLE IF NOT EXISTS fact_vehicle_daily_state (
  device_id INTEGER NOT NULL,
  business_date DATE NOT NULL,
  minutes_moving INTEGER NOT NULL,
  min_soc INTEGER NOT NULL,
  max_soc INTEGER NOT NULL,
  charging_sessions_count INTEGER NOT NULL,
  PRIMARY KEY (device_id, business_date)
);

-- ---------------- GOLD: FACT_CHARGING_SESSION ----------------
CREATE TABLE IF NOT EXISTS fact_charging_session (
  device_id INTEGER NOT NULL,
  station_id INTEGER NOT NULL,
  business_date DATE NOT NULL,
  session_start TIMESTAMP NOT NULL,
  session_end TIMESTAMP NOT NULL,
  total_charging_time INTEGER NOT NULL,     -- phút
  soc_start INTEGER,
  soc_end INTEGER,
  total_charging_energy DOUBLE,             -- kWh (từ trạm; NULL nếu chỉ có telemetry)
  estimated_energy_kwh DOUBLE,              -- dSOC * capacity (từ xe)
  reconciliation_mismatch BOOLEAN NOT NULL DEFAULT FALSE,
  PRIMARY KEY (device_id, station_id, business_date, session_start)
);

-- ---------------- GOLD: FACT_STATION_DAILY ----------------
CREATE TABLE IF NOT EXISTS fact_station_daily (
  station_id INTEGER NOT NULL,
  business_date DATE NOT NULL,
  total_charging INTEGER NOT NULL,          -- tổng lượt sạc
  total_charging_time INTEGER NOT NULL,     -- tổng phút phục vụ
  total_charging_energy DOUBLE NOT NULL,    -- tổng kWh
  utilization_rate DOUBLE,                  -- total_charging_time / (ports*24*60)
  PRIMARY KEY (station_id, business_date)
);

-- ---------------- GOLD: FACT_DISTRIBUTOR_MONTHLY ----------------
CREATE TABLE IF NOT EXISTS fact_distributor_monthly (
  distributor_id INTEGER NOT NULL,
  month VARCHAR NOT NULL,                   -- 'YYYY-MM'
  segment VARCHAR NOT NULL,
  vehicles_sold INTEGER NOT NULL,
  PRIMARY KEY (distributor_id, month, segment)
);
