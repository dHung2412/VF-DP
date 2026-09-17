"""Encoder (chỉ dùng cho generator/test — firmware thật encode trên xe)."""
from __future__ import annotations

import struct
from datetime import datetime, timezone

from .decoder import crc16_ccitt


def _to_u24(v: int) -> bytes:
    return v.to_bytes(3, "big", signed=False)


def _to_i24(v: int) -> bytes:
    return v.to_bytes(3, "big", signed=True)


def encode_frame24(device_id: int, ts_offset_s: int, speed: int, soc: int,
                   battery_temp: int, alert_flags: int, lat: float, lon: float,
                   charging_station_id: int, sequence_id: int) -> bytes:
    head = (b"".join([
        _to_u24(device_id),
        struct.pack(">H", ts_offset_s),
        struct.pack("B", speed),
        struct.pack("B", soc),
        struct.pack("b", battery_temp),
        struct.pack(">H", alert_flags),
        _to_i24(int(round(lat * 100000))),
        _to_u24(int(round(lon * 100000))),
        struct.pack(">H", charging_station_id),
        struct.pack(">H", sequence_id),
    ]))
    assert len(head) == 20
    return head + struct.pack(">H", crc16_ccitt(head)) + struct.pack(">H", 0)


def encode_frame16(device_id: int, ts_offset_s: int, speed: int, soc: int,
                   battery_temp: int, alert_flags: int, lat: float, lon: float) -> bytes:
    return b"".join([
        _to_u24(device_id),
        struct.pack(">H", ts_offset_s),
        struct.pack("B", speed),
        struct.pack("B", soc),
        struct.pack("b", battery_temp),
        struct.pack(">H", alert_flags),
        _to_i24(int(round(lat * 100000))),
        _to_u24(int(round(lon * 100000))),
    ])
