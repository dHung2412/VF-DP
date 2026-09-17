"""Load-test generator: giả lập N xe gửi telemetry 5s/lần ra Bronze.

Dùng local để kiểm chứng pipeline (không cần EMQX/Kafka thật).
Prod thay bằng publisher MQTT -> EMQX -> Kafka (xem docker-compose.yml).
"""
from __future__ import annotations

import argparse
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from vfdp.ingest.buffer import BronzeBuffer
from vfdp.protocol.encoder import encode_frame16, encode_frame24

BASE = datetime(2026, 9, 1, 0, 0, tzinfo=timezone.utc)


def gen(n_vehicles: int, minutes: int, out: str, old_fw_ratio: float = 0.1,
        seed: int = 42, errors: float = 0.001) -> list[str]:
    rnd = random.Random(seed)
    buf = BronzeBuffer(out, flush_max_records=50_000)
    seq = {i: rnd.randint(0, 60000) for i in range(1, n_vehicles + 1)}
    steps = minutes * 12  # mẫu mỗi 5s
    for s in range(steps):
        ts = BASE + timedelta(seconds=s * 5)
        off = int((ts - BASE).total_seconds()) % 65536
        for dev in range(1, n_vehicles + 1):
            seq[dev] = (seq[dev] + 1) % 65536
            charging = dev % 7 == 0 and (s % 60) < 20
            payload = (
                encode_frame16(dev, off, rnd.randint(0, 90), rnd.randint(5, 100),
                               rnd.randint(25, 45), rnd.choice([0, 0, 0, 1]),
                               10.7 + rnd.random() * 0.2, 106.6 + rnd.random() * 0.2)
                if rnd.random() < old_fw_ratio else
                encode_frame24(dev, off,
                               0 if charging else rnd.randint(0, 90),
                               rnd.randint(5, 100), rnd.randint(25, 45),
                               rnd.choice([0, 0, 0, 1]),
                               10.7 + rnd.random() * 0.2, 106.6 + rnd.random() * 0.2,
                               100 + dev % 5 if charging else 0, seq[dev])
            )
            if rnd.random() < errors:  # corrupt 1 byte để test DLQ/CRC
                payload = payload[:-3] + bytes([payload[-3] ^ 0xFF]) + payload[-2:]
            buf.add(ts, ts, payload, dev, seq[dev])
    files = buf.flush()
    print(f"generated vehicles={n_vehicles} minutes={minutes} files={len(files)}")
    return files


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--vehicles", type=int, default=200)
    ap.add_argument("--minutes", type=int, default=60)
    ap.add_argument("--out", default="data/bronze/telemetry")
    ap.add_argument("--old-fw-ratio", type=float, default=0.1)
    args = ap.parse_args()
    gen(args.vehicles, args.minutes, args.out, args.old_fw_ratio)
