"""Seed dims demo cho môi trường local (prod: master data từ MDM/đại lý)."""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import duckdb

Path("data").mkdir(parents=True, exist_ok=True)
con = duckdb.connect("data/warehouse.duckdb")
con.execute(open("sql/ddl/star_schema.sql").read())
for i in range(100, 105):
    con.execute("INSERT OR IGNORE INTO dim_station VALUES (?,?,?,4,2)",
                [i, 10.7 + i * 0.001, 106.6 + i * 0.001])
for i in range(1, 6):
    con.execute("INSERT OR IGNORE INTO dim_distributor VALUES (?,?,?,?)",
                [i, "HCM", f"Q{i}", f"showroom Q{i}"])
    con.execute("INSERT OR IGNORE INTO bridge_station_distributor VALUES (?,?,?)",
                [99 + i, i, ["owned_by_distributor", "partner", "public"][i % 3]])
for d in range(1, 51):
    con.execute("INSERT OR IGNORE INTO dim_vehicle VALUES (?,?,?,?,?,?,?,?)",
                [d, f"VF7_{d:07d}", f"priv{d}", "VF7",
                 "B" if d % 2 else "C", 50.0, d % 5 + 1, "2026-08-15"])

from datetime import date, timedelta
cur = date(2026, 8, 1)
end_d = date(2026, 10, 1)
while cur <= end_d:
    con.execute("INSERT OR IGNORE INTO dim_date VALUES (?,?,?,?,?,?,?)",
                [cur, cur, cur.isoweekday(), cur.month, (cur.month - 1) // 3 + 1,
                 cur.year, cur.isoweekday() >= 6])
    cur += timedelta(days=1)

import csv
with open("data/demo_distributor_sales.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["distributor_id", "province", "city", "location", "device_id", "segment", "sale_date"])
    for d in range(1, 51):
        dist_id = d % 5 + 1
        seg = "B" if d % 2 else "C"
        s_month = "2026-08" if d <= 25 else "2026-09"
        writer.writerow([dist_id, "HCM", f"Q{dist_id}", f"showroom Q{dist_id}", d, seg, f"{s_month}-15"])

print("seeded stations:", con.execute("SELECT count(*) FROM dim_station").fetchone()[0],
      "vehicles:", con.execute("SELECT count(*) FROM dim_vehicle").fetchone()[0],
      "dates:", con.execute("SELECT count(*) FROM dim_date").fetchone()[0],
      "distributor_sales_csv: data/demo_distributor_sales.csv")
