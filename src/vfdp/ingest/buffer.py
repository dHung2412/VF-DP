"""Ingestion buffer: gom record (server_ts + raw 24B) rồi flush Parquet Bronze.

Prod: Kafka Connect S3 Sink flush khi >=128MB hoặc 10-15 phút, partition
  s3://lakehouse/bronze/telemetry/business_date=YYYY-MM-DD/hour=HH/
Local/dev: class BronzeBuffer gom batch trong RAM rồi flush parquet tương tự.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from ..utils.dates import business_date_of, hour_vn


class BronzeBuffer:
    """Gom (server_timestamp, raw_payload, base_epoch_offset...). Đơn giản:
    caller đưa timestamp UTC đã decode + payload; buffer tự partition."""

    def __init__(self, root: str | Path, flush_max_records: int = 100_000):
        self.root = Path(root)
        self.flush_max_records = flush_max_records
        self._buckets: dict[tuple[str, str], list[dict]] = defaultdict(list)
        self.flushed_files: list[str] = []

    def add(self, timestamp_utc: datetime, server_timestamp_utc: datetime,
            raw_payload: bytes, device_id: int, sequence_id: int | None) -> None:
        if timestamp_utc.tzinfo is None:
            timestamp_utc = timestamp_utc.replace(tzinfo=timezone.utc)
        bd = str(business_date_of(timestamp_utc))
        hh = f"{hour_vn(timestamp_utc):02d}"
        self._buckets[(bd, hh)].append({
            "timestamp_utc": timestamp_utc,
            "server_timestamp_utc": server_timestamp_utc,
            "raw_payload": bytes(raw_payload),
            "device_id": device_id,
            "sequence_id": sequence_id if sequence_id is not None else -1,
            "payload_len": len(raw_payload),
        })
        if sum(len(v) for v in self._buckets.values()) >= self.flush_max_records:
            self.flush()

    def flush(self) -> list[str]:
        import pyarrow as pa
        import pyarrow.parquet as pq

        out: list[str] = []
        for (bd, hh), rows in list(self._buckets.items()):
            if not rows:
                continue
            table = pa.Table.from_pylist(rows)
            d = self.root / f"business_date={bd}" / f"hour={hh}"
            d.mkdir(parents=True, exist_ok=True)
            fp = d / f"part-{len(self.flushed_files):05d}.parquet"
            pq.write_table(table, fp, compression="zstd")
            out.append(str(fp))
            self.flushed_files.append(str(fp))
            self._buckets[(bd, hh)] = []
        return out
