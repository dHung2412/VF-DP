"""Silver batch job (chạy 15-30-60 phút/lần). Bronze -> Silver parquet + DuckDB."""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from vfdp.dq.checks import check_silver
from vfdp.silver.clean import silver_from_bronze, write_silver_parquet


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bronze", default="data/bronze/telemetry")
    ap.add_argument("--silver", default="data/silver/fact_vehicle_telemetry")
    ap.add_argument("--warehouse", default="data/warehouse.duckdb")
    ap.add_argument("--base-epoch", default="2026-09-01T00:00:00+00:00")
    ap.add_argument("--known-vehicles", default="")  # csv dim_vehicle (device_id_numeric) optional
    args = ap.parse_args()

    base = datetime.fromisoformat(args.base_epoch)
    known = None
    if args.known_vehicles and Path(args.known_vehicles).exists():
        import csv
        with open(args.known_vehicles) as f:
            known = {int(r["device_id_numeric"]) for r in csv.DictReader(f)}

    rows = silver_from_bronze(args.bronze, base, known)
    dq = check_silver(rows)
    print(f"silver_rows={len(rows)} dq={dq}")
    if not rows:
        return
    files = write_silver_parquet(rows, args.silver)
    print(f"silver_files={files}")

    import duckdb
    Path(args.warehouse).parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(args.warehouse)
    con.execute(open("sql/ddl/star_schema.sql").read())
    con.execute("DELETE FROM fact_vehicle_telemetry WHERE business_date IN "
                "(SELECT DISTINCT CAST(business_date AS DATE) FROM read_parquet(?) )",
                [str(Path(args.silver) / "**" / "*.parquet")])
    con.execute("INSERT INTO fact_vehicle_telemetry SELECT * FROM read_parquet(?)",
                [str(Path(args.silver) / "**" / "*.parquet")])
    print("warehouse silver count:",
          con.execute("SELECT count(*) FROM fact_vehicle_telemetry").fetchone())


if __name__ == "__main__":
    main()
