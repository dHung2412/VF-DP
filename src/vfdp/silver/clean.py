"""Silver job: Bronze Parquet (raw) -> fact_vehicle_telemetry (cleaned).

Bước: decode 24B/16B theo base_epoch -> validate CRC/range -> dedup
(device_id, sequence_id, timestamp) -> state detection -> gắn business_date/hour.
Late-arriving: dùng timestamp gốc để routing partition (không dùng server_ts).
Unknown vehicle: join dim_vehicle (nếu có); thiếu -> is_unknown_vehicle=true.
"""
from __future__ import annotations

from collections import OrderedDict
from datetime import datetime, timezone
from pathlib import Path

from ..protocol.decoder import decode_frame, validate_frame
from ..utils.dates import business_date_of, hour_vn


def silver_from_bronze(bronze_dir: str | Path, base_epoch: datetime,
                       known_device_ids: set[int] | None = None,
                       speed_th: int = 5, soc_low: int = 20) -> list[dict]:
    import pyarrow.parquet as pq

    bronze_dir = Path(bronze_dir)
    files = sorted(bronze_dir.rglob("*.parquet"))
    if not files:
        return []
    if base_epoch.tzinfo is None:
        base_epoch = base_epoch.replace(tzinfo=timezone.utc)

    rows: list[dict] = []
    corrupt = 0
    for fp in files:
        tbl = pq.read_table(fp).to_pylist()
        for r in tbl:
            raw = bytes(r["raw_payload"])
            try:
                f = decode_frame(raw, base_epoch)
            except Exception:
                corrupt += 1
                continue
            errs = validate_frame(f)
            if "crc_mismatch" in errs:
                corrupt += 1
                continue  # -> DLQ (đếm corrupt_frame_count)
            if any(e in errs for e in ("soc_out_of_range", "lat_out_of_range", "lon_out_of_range")):
                continue
            is_moving = f.speed >= speed_th
            is_charging = f.charging_station_id > 0
            rows.append({
                "device_id": f.device_id,
                "timestamp": f.timestamp,
                "business_date": business_date_of(f.timestamp),
                "hour": hour_vn(f.timestamp),
                "speed": f.speed, "soc": f.soc,
                "battery_temp": f.battery_temp,
                "alert_flags": f.alert_flags,
                "charging_station_id": f.charging_station_id,
                "lat": f.lat, "lon": f.lon,
                "sequence_id": f.sequence_id if f.sequence_id is not None else -1,
                "is_moving": is_moving,
                "is_charging": is_charging,
                "is_idle": (not is_moving) and (not is_charging),
                "is_low_battery": f.soc <= soc_low,
                "is_unknown_vehicle": bool(known_device_ids is not None
                                           and f.device_id not in known_device_ids),
                "payload_len": f.payload_len,
            })

    # Dedup theo (device_id, sequence_id, timestamp): giữ bản đầu tiên
    seen: OrderedDict[tuple, dict] = OrderedDict()
    for r in sorted(rows, key=lambda x: (x["device_id"], x["sequence_id"], x["timestamp"])):
        ts = r["timestamp"]
        key = (r["device_id"], r["sequence_id"], ts.isoformat())
        if key not in seen:
            seen[key] = r
    out = list(seen.values())
    out.sort(key=lambda x: (x["device_id"], x["timestamp"]))
    return out


def write_silver_parquet(rows: list[dict], out_dir: str | Path) -> list[str]:
    import pyarrow as pa
    import pyarrow.parquet as pq

    out_dir = Path(out_dir)
    by_part: dict[tuple[str, str], list[dict]] = {}
    for r in rows:
        # chuẩn hóa datetime/date về ISO để parquet ổn định
        c = dict(r)
        if isinstance(c["timestamp"], datetime):
            c["timestamp"] = c["timestamp"].astimezone(timezone.utc).replace(tzinfo=None)
        c["business_date"] = str(c["business_date"])
        by_part.setdefault((c["business_date"], f'{c["hour"]:02d}'), []).append(c)
    files: list[str] = []
    for (bd, hh), part in sorted(by_part.items()):
        d = out_dir / f"business_date={bd}" / f"hour={hh}"
        d.mkdir(parents=True, exist_ok=True)
        fp = d / "silver.parquet"
        pq.write_table(pa.Table.from_pylist(part), fp, compression="zstd")
        files.append(str(fp))
    return files
