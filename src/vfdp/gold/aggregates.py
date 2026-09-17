"""Gold aggregates: Silver -> 4 fact Gold + reconciliation.

- fact_vehicle_daily_state: group (device_id, business_date)
  minutes_moving = 5s mẫu -> mỗi is_moving ~5/60 phút (clamp thực tế bằng
  diff timestamp khi có thể; ở đây dùng đếm mẫu * 5/60, làm tròn).
- fact_charging_session: sessionize telemetry is_charging theo
  (device_id, charging_station_id), ngắt khi đổi trạm / gap > 15 phút.
  cross_midnight_rule='split_at_cutoff': tách session vắt qua mốc 02h00 VN.
- reconciliation: dE = (soc_end-soc_start)*capacity; mismatch nếu
  |E_trạm - dE|/max(E_trạm,eps) > tol. E_trạm lấy từ station batch (nếu có).
- fact_station_daily + utilization_rate = total_time/(ports*24*60).
"""
from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timedelta, timezone

VN = timezone(timedelta(hours=7))
GAP_S = 15 * 60


def daily_state(silver_rows: list[dict]) -> list[dict]:
    groups: dict[tuple[int, object], list[dict]] = defaultdict(list)
    for r in silver_rows:
        groups[(r["device_id"], str(r["business_date"]))].append(r)
    out = []
    for (dev, bd), rs in sorted(groups.items()):
        rs.sort(key=lambda x: x["timestamp"])
        so = [x["soc"] for x in rs]
        # đếm session sạc độc lập (số block is_charging liên tục)
        n_sess, in_s = 0, False
        for x in rs:
            if x["is_charging"] and not in_s:
                n_sess += 1
                in_s = True
            elif not x["is_charging"]:
                in_s = False
        out.append({
            "device_id": dev, "business_date": bd,
            "minutes_moving": round(sum(1 for x in rs if x["is_moving"]) * 5 / 60, 2),
            "min_soc": min(so), "max_soc": max(so),
            "charging_sessions_count": n_sess,
        })
    return out


def _split_at_cutoff(start: datetime, end: datetime) -> list[tuple[date, float]]:
    """Chia session theo mốc 02h00 VN, trả [(business_date, frac_time)...]."""
    def bd(dt: datetime) -> date:
        vn = dt.astimezone(VN)
        return (vn - timedelta(days=1)).date() if vn.hour < 2 else vn.date()

    def cutoff_after(dt: datetime) -> datetime:
        vn = dt.astimezone(VN)
        day = vn.date() if vn.hour >= 2 else (vn - timedelta(days=1)).date()
        nxt = datetime(day.year, day.month, day.day, 2, 0, tzinfo=VN) + timedelta(days=1)
        return nxt.astimezone(timezone.utc)

    total = (end - start).total_seconds()
    if total <= 0:
        return [(bd(start), 1.0)]
    segs, cur = [], start
    while bd(cur) != bd(end):
        nxt = cutoff_after(cur)
        segs.append((bd(cur), (nxt - cur).total_seconds() / total))
        cur = nxt
    segs.append((bd(end), (end - cur).total_seconds() / total))
    return segs


def charging_sessions(silver_rows: list[dict],
                      station_energy: dict[tuple[int, int, str], float] | None = None,
                      capacity: dict[int, float] | None = None,
                      tol: float = 0.15, eps: float = 0.1) -> list[dict]:
    station_energy = station_energy or {}
    capacity = capacity or {}
    by_dev: dict[int, list[dict]] = defaultdict(list)
    for r in silver_rows:
        if r["is_charging"]:
            by_dev[r["device_id"]].append(r)
    sessions: list[dict] = []
    for dev, rs in by_dev.items():
        rs.sort(key=lambda x: x["timestamp"])
        cur: list[dict] = []
        for x in rs:
            if (cur and (x["charging_station_id"] != cur[-1]["charging_station_id"]
                         or (x["timestamp"] - cur[-1]["timestamp"]).total_seconds() > GAP_S)):
                sessions.extend(_finalize(dev, cur, station_energy, capacity, tol, eps))
                cur = [x]
            else:
                cur.append(x)
        if cur:
            sessions.extend(_finalize(dev, cur, station_energy, capacity, tol, eps))
    sessions.sort(key=lambda s: (s["device_id"], s["session_start"]))
    return sessions


def _finalize(dev, block, station_energy, capacity, tol, eps) -> list[dict]:
    st_id = block[0]["charging_station_id"]
    start, end = block[0]["timestamp"], block[-1]["timestamp"]
    mins = max(int(round((end - start).total_seconds() / 60)), len(block) * 5 // 60)
    mins = max(mins, 1)
    soc_s, soc_e = block[0]["soc"], block[-1]["soc"]
    est = max((soc_e - soc_s), 0) * capacity.get(dev, 50.0) / 100.0
    out = []
    for bd, frac in _split_at_cutoff(start, end):
        e_tram = station_energy.get((dev, st_id, str(bd)))
        energy = round(e_tram * frac, 3) if e_tram is not None else None
        base = energy if energy is not None else est * frac
        mismatch = bool(energy is not None and abs(energy - est * frac) / max(energy, eps) > tol)
        out.append({
            "device_id": dev, "station_id": st_id, "business_date": str(bd),
            "session_start": start.isoformat(), "session_end": end.isoformat(),
            "total_charging_time": max(int(round(mins * frac)), 1),
            "soc_start": soc_s, "soc_end": soc_e,
            "total_charging_energy": energy,
            "estimated_energy_kwh": round(est * frac, 3),
            "reconciliation_mismatch": mismatch,
        })
    return out


def station_daily(sessions: list[dict], station_ports: dict[int, int] | None = None,
                  station_batch: dict[tuple[int, str], dict] | None = None) -> list[dict]:
    """Ưu tiên số liệu chốt từ trạm (station_batch) nếu có; fallback cộng từ sessions."""
    station_ports = station_ports or {}
    agg: dict[tuple[int, str], dict] = defaultdict(lambda: {"n": 0, "t": 0, "e": 0.0})
    for s in sessions:
        k = (s["station_id"], s["business_date"])
        agg[k]["n"] += 1
        agg[k]["t"] += s["total_charging_time"]
        e = s["total_charging_energy"] if s["total_charging_energy"] is not None else (s.get("estimated_energy_kwh") or 0.0)
        agg[k]["e"] += e
    if station_batch:
        for (st, bd), v in station_batch.items():
            agg[(st, str(bd))] = {"n": v["total_charging"], "t": v["total_charging_time"],
                                  "e": v["total_charging_energy"]}
    out = []
    for (st, bd), v in sorted(agg.items()):
        ports = station_ports.get(st, 0)
        util = round(v["t"] / (ports * 24 * 60), 4) if ports else None
        out.append({"station_id": st, "business_date": bd,
                    "total_charging": v["n"], "total_charging_time": v["t"],
                    "total_charging_energy": round(v["e"], 3),
                    "utilization_rate": util})
    return out
