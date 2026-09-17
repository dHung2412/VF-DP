import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from vfdp.protocol.decoder import crc16_ccitt, decode_frame
from vfdp.protocol.encoder import encode_frame16, encode_frame24
from vfdp.utils.dates import business_date_of

BASE = datetime(2026, 9, 1, 0, 0, tzinfo=timezone.utc)


def test_roundtrip_24B():
    p = encode_frame24(1, 3600, 60, 80, 35, 0, 10.762622, 106.660172, 5, 100)
    assert len(p) == 24
    f = decode_frame(p, BASE)
    assert f.device_id == 1 and f.crc_ok
    assert abs(f.lat - 10.76262) < 1e-4 and abs(f.lon - 106.66017) < 1e-4
    assert f.charging_station_id == 5 and f.sequence_id == 100


def test_backward_compat_16B():
    p = encode_frame16(7, 10, 50, 60, 30, 1, 10.7, 106.6)
    assert len(p) == 16
    f = decode_frame(p, BASE)
    assert f.device_id == 7 and f.charging_station_id == 0 and f.sequence_id is None


def test_crc_detects_corruption():
    p = bytearray(encode_frame24(2, 5, 10, 50, 30, 0, 10.7, 106.6, 0, 9))
    p[6] ^= 0xFF
    assert not decode_frame(bytes(p), BASE).crc_ok


def test_business_date_cutoff():
    # 2026-09-01 01:30 VN (= 2026-08-31 18:30 UTC) -> business_date 08-31
    dt = datetime(2026, 8, 31, 18, 30, tzinfo=timezone.utc)
    assert str(business_date_of(dt)) == "2026-08-31"
    dt2 = datetime(2026, 8, 31, 19, 30, tzinfo=timezone.utc)  # 02:30 VN
    assert str(business_date_of(dt2)) == "2026-09-01"


def test_silver_dedup_and_states():
    from vfdp.ingest.buffer import BronzeBuffer
    import tempfile
    tmp = tempfile.mkdtemp()
    buf = BronzeBuffer(f"{tmp}/bronze", flush_max_records=1000)
    ts = BASE
    p = encode_frame24(9, 0, 60, 15, 30, 0, 10.7, 106.6, 0, 42)
    buf.add(ts, ts, p, 9, 42)
    buf.add(ts, ts, p, 9, 42)  # trùng -> dedup còn 1
    buf.flush()
    from vfdp.silver.clean import silver_from_bronze
    rows = silver_from_bronze(f"{tmp}/bronze", BASE, {9})
    assert len(rows) == 1
    r = rows[0]
    assert r["is_moving"] and r["is_low_battery"] and not r["is_unknown_vehicle"]


def test_gold_reconciliation_flags_mismatch():
    from datetime import timedelta
    from vfdp.gold.aggregates import charging_sessions
    t0 = BASE
    silver = [{"device_id": 1, "timestamp": t0 + timedelta(seconds=i * 300),
               "business_date": "2026-09-01", "hour": 8, "speed": 0,
               "soc": 20 + i * 10, "battery_temp": 30, "alert_flags": 0,
               "charging_station_id": 5, "lat": 10.7, "lon": 106.6,
               "sequence_id": i, "is_moving": False, "is_charging": True,
               "is_idle": False, "is_low_battery": False,
               "is_unknown_vehicle": False, "payload_len": 24}
              for i in range(5)]  # soc 20->60, dE=0.4*50=20kWh
    sess = charging_sessions(silver, {(1, 5, "2026-09-01"): 5.0}, {1: 50.0})
    assert sess and sess[0]["reconciliation_mismatch"] is True
    ok = charging_sessions(silver, {(1, 5, "2026-09-01"): 21.0}, {1: 50.0})
    assert ok and ok[0]["reconciliation_mismatch"] is False


def test_station_daily_aggregation():
    from vfdp.gold.aggregates import station_daily
    sessions = [
        {"station_id": 101, "business_date": "2026-09-01", "total_charging_time": 60,
         "total_charging_energy": 25.0, "estimated_energy_kwh": 24.0},
        {"station_id": 101, "business_date": "2026-09-01", "total_charging_time": 30,
         "total_charging_energy": None, "estimated_energy_kwh": 12.0},
    ]
    st = station_daily(sessions, station_ports={101: 2})
    assert len(st) == 1
    assert st[0]["total_charging"] == 2
    assert st[0]["total_charging_time"] == 90
    assert abs(st[0]["total_charging_energy"] - 37.0) < 1e-3
    # utilization_rate = 90 / (2 * 24 * 60) = 90 / 2880 = 0.03125 -> 0.0312 (round-to-even)
    assert abs(st[0]["utilization_rate"] - 0.03125) < 1e-3


def test_split_at_cutoff_cross_midnight():
    from datetime import timedelta
    from vfdp.gold.aggregates import _split_at_cutoff
    # 01:30 VN (18:30 UTC) to 02:30 VN (19:30 UTC)
    t_start = datetime(2026, 8, 31, 18, 30, tzinfo=timezone.utc)
    t_end = datetime(2026, 8, 31, 19, 30, tzinfo=timezone.utc)
    segs = _split_at_cutoff(t_start, t_end)
    assert len(segs) == 2
    assert str(segs[0][0]) == "2026-08-31" and abs(segs[0][1] - 0.5) < 1e-4
    assert str(segs[1][0]) == "2026-09-01" and abs(segs[1][1] - 0.5) < 1e-4


def test_dq_checks():
    from vfdp.dq.checks import check_reconciliation, check_silver
    valid_rows = [{
        "device_id": 1, "sequence_id": 10, "timestamp": BASE,
        "soc": 50, "is_unknown_vehicle": False
    }]
    assert check_silver(valid_rows)["pass"] is True

    bad_soc_rows = [{
        "device_id": 1, "sequence_id": 10, "timestamp": BASE,
        "soc": 150, "is_unknown_vehicle": False
    }]
    res = check_silver(bad_soc_rows)
    assert res["pass"] is False
    assert any("soc_out_of_range" in x for x in res["issues"])

    sessions = [
        {"reconciliation_mismatch": False} for _ in range(99)
    ] + [{"reconciliation_mismatch": True}]
    r_ok = check_reconciliation(sessions, max_mismatch_ratio=0.02)
    assert r_ok["pass"] is True
    r_fail = check_reconciliation(sessions, max_mismatch_ratio=0.005)
    assert r_fail["pass"] is False


def test_bi_queries_run_cleanly():
    import duckdb
    con = duckdb.connect()
    con.execute(open("sql/ddl/star_schema.sql").read())
    # Verify SQL query parsing
    q1 = open("bi/queries/dashboard1_low_battery_heatmap.sql").read().replace(":business_date", "'2026-09-01'")
    con.execute(q1).fetchall()
    q2 = open("bi/queries/dashboard2_station_overload.sql").read().replace(":from_date", "'2026-09-01'").replace(":to_date", "'2026-09-02'")
    con.execute(q2).fetchall()
    q3 = open("bi/queries/dashboard3_expansion_proposal.sql").read().replace(":from_month", "'2026-08'").replace(":to_month", "'2026-09'").replace(":from_date", "'2026-09-01'").replace(":to_date", "'2026-09-02'")
    con.execute(q3).fetchall()

