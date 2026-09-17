"""Gold daily job (chạy 02h30 sau khi trạm chốt 02h00).

Đọc Silver parquet + station batch CSV + dims -> ghi 3 fact Gold ra parquet
và upsert vào DuckDB warehouse. Idempotent theo business_date (delete+insert).
"""
from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from vfdp.dq.checks import check_reconciliation
from vfdp.gold.aggregates import charging_sessions, daily_state, station_daily


def load_silver(silver_dir: str, business_date: str) -> list[dict]:
    import pyarrow.parquet as pq

    p = Path(silver_dir) / f"business_date={business_date}"
    if not p.exists():  # rolling window: gom cả hour partition rời rạc
        return []
    rows = []
    for fp in sorted(p.rglob("*.parquet")):
        for r in pq.read_table(fp).to_pylist():
            from datetime import datetime
            ts = r["timestamp"]
            if not isinstance(ts, datetime):
                ts = datetime.fromisoformat(str(ts))
            r["timestamp"] = ts.replace(tzinfo=__import__("datetime").timezone.utc) \
                if ts.tzinfo is None else ts
            rows.append(r)
    return rows


def load_csv(path: str) -> list[dict]:
    if not path or not Path(path).exists():
        return []
    with open(path) as f:
        return list(csv.DictReader(f))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--silver", default="data/silver/fact_vehicle_telemetry")
    ap.add_argument("--gold", default="data/gold")
    ap.add_argument("--warehouse", default="data/warehouse.duckdb")
    ap.add_argument("--business-date", required=True)  # YYYY-MM-DD
    ap.add_argument("--station-batch", default="")  # csv: station_id,business_date,total_charging,total_charging_time,total_charging_energy,car_chargers,moto_chargers
    ap.add_argument("--vehicle-dim", default="")    # csv: device_id_numeric,battery_capacity_kwh
    args = ap.parse_args()

    silver = load_silver(args.silver, args.business_date)
    print(f"silver input rows={len(silver)} for {args.business_date}")
    cap = {int(r["device_id_numeric"]): float(r["battery_capacity_kwh"])
           for r in load_csv(args.vehicle_dim)} if args.vehicle_dim else {}

    st_rows = load_csv(args.station_batch)
    st_energy: dict[tuple[int, int, str], float] = {}
    st_batch: dict[tuple[int, str], dict] = {}
    ports: dict[int, int] = {}
    for r in st_rows:
        devs = [d for d in str(r.get("device_ids", "")).split(";") if d]
        e_each = float(r["total_charging_energy"]) / max(len(devs), 1)
        for d in devs:
            st_energy[(int(d), int(r["station_id"]), r["business_date"])] = e_each
        st_batch[(int(r["station_id"]), r["business_date"])] = {
            "total_charging": int(r["total_charging"]),
            "total_charging_time": int(r["total_charging_time"]),
            "total_charging_energy": float(r["total_charging_energy"])}
        ports[int(r["station_id"])] = int(r.get("car_chargers", 4)) + int(r.get("moto_chargers", 0))

    if Path(args.warehouse).exists():
        import duckdb
        try:
            con_ports = duckdb.connect(args.warehouse)
            for st, cc, mc in con_ports.execute("SELECT station_id, car_chargers, moto_chargers FROM dim_station").fetchall():
                if st not in ports:
                    ports[st] = (cc or 0) + (mc or 0)
            con_ports.close()
        except Exception:
            pass

    daily = daily_state(silver)
    sess = charging_sessions(silver, st_energy, cap)
    st_daily = station_daily(sess, ports, st_batch or None)
    dq = check_reconciliation(sess)
    print(f"daily={len(daily)} sessions={len(sess)} stations={len(st_daily)} recon={dq}")

    import pyarrow as pa
    import pyarrow.parquet as pq
    out = Path(args.gold)
    for name, rows in [("fact_vehicle_daily_state", daily),
                       ("fact_charging_session", sess),
                       ("fact_station_daily", st_daily)]:
        d = out / name / f"business_date={args.business_date}"
        d.mkdir(parents=True, exist_ok=True)
        if rows:
            pq.write_table(pa.Table.from_pylist(rows), d / "gold.parquet", compression="zstd")

    import duckdb
    Path(args.warehouse).parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(args.warehouse)
    con.execute(open("sql/ddl/star_schema.sql").read())
    for tbl, rows, cols in [
        ("fact_vehicle_daily_state", daily,
         "(device_id,business_date,minutes_moving,min_soc,max_soc,charging_sessions_count)"),
        ("fact_charging_session", sess,
         "(device_id,station_id,business_date,session_start,session_end,total_charging_time,soc_start,soc_end,total_charging_energy,estimated_energy_kwh,reconciliation_mismatch)"),
        ("fact_station_daily", st_daily,
         "(station_id,business_date,total_charging,total_charging_time,total_charging_energy,utilization_rate)"),
    ]:
        con.execute(f"DELETE FROM {tbl} WHERE business_date = ?", [args.business_date])
        if rows:
            con.register("tmp_gold", pa.Table.from_pylist(rows))
            con.execute(f"INSERT INTO {tbl} SELECT * FROM tmp_gold")
            con.unregister("tmp_gold")
    print("gold upsert done")


if __name__ == "__main__":
    main()
