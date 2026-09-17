"""Decoder gói tin telemetry xe điện.

Wire frame 24B (firmware mới) — big-endian:
  0-2   device_id        uint24
  3-4   timestamp_offset uint16 (offset giây so với base epoch của batch)
  5     speed            uint8
  6     soc              uint8 (0-100)
  7     battery_temp     int8
  8-9   alert_flags      uint16 (0-10, batch: chỉ lưu audit, không alert realtime)
  10-12 lat_encoded      int24  (lat = v/100000.0)
  13-15 lon_encoded      uint24 (lon = v/100000.0; NOTE: đặc tả ghi int24 nhưng
                         kinh độ VN ~102-110° -> ~10-11M vượt max int24
                         8.38M nên triển khai dùng uint24, max 16.7M)
  16-17 charging_station uint16 (0 = không sạc)
  18-19 sequence_id      uint16 (per-device, dedup)
  20-21 crc16            uint16 (CRC16-CCITT 0xFFFF trên 20 byte đầu)
  22-23 reserved         uint16 (=0)

Tương thích ngược 16B (firmware cũ): 16 byte đầu với layout tương tự
(device_id, ts_offset, speed, soc, batt_temp, alert, lat, lon); không có
charging_station/sequence/crc/reserved -> các trường đó = 0/None.
Nhận diện bằng len(payload): 16 | 24.
"""
from __future__ import annotations

import struct
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone


def crc16_ccitt(data: bytes, poly: int = 0x1021, init: int = 0xFFFF) -> int:
    crc = init
    for b in data:
        crc ^= b << 8
        for _ in range(8):
            crc = ((crc << 1) ^ poly) & 0xFFFF if crc & 0x8000 else (crc << 1) & 0xFFFF
    return crc


def _u24(be3: bytes) -> int:
    return int.from_bytes(be3, "big", signed=False)


def _i24(be3: bytes) -> int:
    return int.from_bytes(be3, "big", signed=True)


@dataclass
class TelemetryFrame:
    device_id: int
    timestamp: datetime          # UTC (base_epoch + offset)
    speed: int
    soc: int
    battery_temp: int
    alert_flags: int
    lat: float
    lon: float
    charging_station_id: int
    sequence_id: int | None
    payload_len: int
    crc_ok: bool


class DecodeError(ValueError):
    pass


def decode_frame(payload: bytes, base_epoch: datetime) -> TelemetryFrame:
    if len(payload) not in (16, 24):
        raise DecodeError(f"payload_length={len(payload)} không hỗ trợ (chỉ 16/24)")
    if base_epoch.tzinfo is None:
        base_epoch = base_epoch.replace(tzinfo=timezone.utc)

    device_id = _u24(payload[0:3])
    ts_offset = struct.unpack(">H", payload[3:5])[0]
    speed = payload[5]
    soc = payload[6]
    battery_temp = struct.unpack("b", payload[7:8])[0]
    alert_flags = struct.unpack(">H", payload[8:10])[0]
    lat = _i24(payload[10:13]) / 100000.0
    lon = _u24(payload[13:16]) / 100000.0
    timestamp = base_epoch + timedelta(seconds=ts_offset)

    if len(payload) == 16:
        return TelemetryFrame(device_id, timestamp, speed, soc, battery_temp,
                              alert_flags, lat, lon, 0, None, 16, True)

    charging_station_id = struct.unpack(">H", payload[16:18])[0]
    sequence_id = struct.unpack(">H", payload[18:20])[0]
    crc_recv = struct.unpack(">H", payload[20:22])[0]
    crc_calc = crc16_ccitt(bytes(payload[0:20]))
    return TelemetryFrame(device_id, timestamp, speed, soc, battery_temp,
                          alert_flags, lat, lon, charging_station_id,
                          sequence_id, 24, crc_recv == crc_calc)


def validate_frame(f: TelemetryFrame) -> list[str]:
    """Trả về danh sách lỗi (rỗng = hợp lệ). Bản tin lỗi CRC do caller routing sang DLQ."""
    errs: list[str] = []
    if f.payload_len == 24 and not f.crc_ok:
        errs.append("crc_mismatch")
    if not (0 <= f.soc <= 100):
        errs.append("soc_out_of_range")
    if not (-30 <= f.battery_temp <= 80):
        errs.append("battery_temp_out_of_range")
    if not (-90.0 <= f.lat <= 90.0):
        errs.append("lat_out_of_range")
    if not (-180.0 <= f.lon <= 180.0):
        errs.append("lon_out_of_range")
    if not (0 <= f.alert_flags <= 10):
        errs.append("alert_flags_out_of_range")
    return errs
