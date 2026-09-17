"""Distributor monthly job (chạy ngày 01 hàng tháng). CSV đại lý -> fact_distributor_monthly."""
from __future__ import annotations

import argparse
import csv
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)  # csv: distributor_id,province,city,location,device_id,segment,sale_date
    ap.add_argument("--month", required=True)  # YYYY-MM
    ap.add_argument("--gold", default="data/gold/fact_distributor_monthly")
    ap.add_argument("--warehouse", default="data/warehouse.duckdb")
    args = ap.parse_args()

    with open(args.input) as f:
        rows = [r for r in csv.DictReader(f)
                if str(r.get("sale_date", ""))[:7] == args.month]
    c = Counter((r["distributor_id"], args.month, r.get("segment", "unknown")) for r in rows)
    out = [{"distributor_id": int(k[0]), "month": k[1], "segment": k[2],
            "vehicles_sold": v} for k, v in sorted(c.items())]
    print(f"distributor rows={len(out)}")

    import pyarrow as pa
    import pyarrow.parquet as pq
    d = Path(args.gold) / f"month={args.month}"
    d.mkdir(parents=True, exist_ok=True)
    if out:
        pq.write_table(pa.Table.from_pylist(out), d / "gold.parquet", compression="zstd")

    import duckdb
    Path(args.warehouse).parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(args.warehouse)
    con.execute(open("sql/ddl/star_schema.sql").read())
    con.execute("DELETE FROM fact_distributor_monthly WHERE month = ?", [args.month])
    if out:
        con.register("tmp_d", pa.Table.from_pylist(out))
        con.execute("INSERT INTO fact_distributor_monthly SELECT * FROM tmp_d")
    print("distributor monthly done")


if __name__ == "__main__":
    main()
