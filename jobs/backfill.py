"""Backfill rolling window 3-7 ngày cho late-arriving data (idempotent re-run gold)."""
from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import date, timedelta


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--end-date", required=True)   # YYYY-MM-DD (business_date mới nhất)
    ap.add_argument("--window", type=int, default=7)
    args = ap.parse_args()

    end = date.fromisoformat(args.end_date)
    for i in range(args.window - 1, -1, -1):
        bd = str(end - timedelta(days=i))
        print(f"== backfill {bd}")
        r = subprocess.run([sys.executable, "jobs/gold_job.py", "--business-date", bd],
                           capture_output=True, text=True)
        print(r.stdout[-1500:])
        if r.returncode != 0:
            print(r.stderr[-2000:])
            raise SystemExit(r.returncode)


if __name__ == "__main__":
    main()
