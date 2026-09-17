"""Data Quality checks chạy sau mỗi batch (Great Expectations/Soda thay bằng hàm nhẹ)."""
from __future__ import annotations


def check_silver(rows: list[dict]) -> dict:
    issues: list[str] = []
    if not rows:
        return {"pass": False, "issues": ["silver_empty"]}
    bad_soc = sum(1 for r in rows if not (0 <= r["soc"] <= 100))
    if bad_soc:
        issues.append(f"soc_out_of_range:{bad_soc}")
    null_ts = sum(1 for r in rows if r.get("timestamp") is None)
    if null_ts:
        issues.append(f"null_timestamp:{null_ts}")
    # dedup key duy nhất
    keys = [(r["device_id"], r["sequence_id"], str(r["timestamp"])) for r in rows]
    if len(set(keys)) != len(keys):
        issues.append("duplicate_keys")
    unk = sum(1 for r in rows if r.get("is_unknown_vehicle"))
    return {"pass": not issues, "issues": issues,
            "unknown_vehicle_count": unk, "row_count": len(rows)}


def check_reconciliation(sessions: list[dict], max_mismatch_ratio: float = 0.02) -> dict:
    if not sessions:
        return {"pass": True, "issues": [], "mismatch_ratio": 0.0}
    mm = sum(1 for s in sessions if s["reconciliation_mismatch"])
    ratio = mm / len(sessions)
    ok = ratio <= max_mismatch_ratio  # mục tiêu pass >=98% phiên
    return {"pass": ok, "issues": [] if ok else [f"mismatch_ratio:{ratio:.3f}"],
            "mismatch_ratio": round(ratio, 4), "session_count": len(sessions)}
