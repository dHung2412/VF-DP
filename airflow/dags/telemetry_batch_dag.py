"""Airflow DAG: điều phối batch Bronze->Silver (hourly) -> Gold (02h30) -> DQ.

Prod dùng Airflow/Dagster; file này là DAG chuẩn, import được khi có Airflow.
Lịch: silver hourly; gold daily 02:30 chờ sensor file trạm 02:00.
"""
from __future__ import annotations

from datetime import datetime, timedelta

try:
    from airflow import DAG
    from airflow.operators.bash import BashOperator
    from airflow.sensors.filesystem import FileSensor
    HAS_AIRFLOW = True
except Exception:  # cho phép import/doc mà không cần airflow installed
    DAG = BashOperator = FileSensor = None  # type: ignore
    HAS_AIRFLOW = False

default_args = {"owner": "data-eng", "retries": 2,
                "retry_delay": timedelta(minutes=5)}

if HAS_AIRFLOW:
    with DAG("vfdp_telemetry_batch", default_args=default_args,
             schedule_interval="0 * * * *", start_date=datetime(2026, 9, 1),
             catchup=False, max_active_runs=1) as dag:
        silver = BashOperator(task_id="silver_hourly",
                              bash_command="python jobs/silver_job.py")
        wait_station = FileSensor(task_id="wait_station_02h",
                                  filepath="data/bronze/station/{{ ds }}.csv",
                                  poke_interval=300, timeout=7200)
        gold = BashOperator(task_id="gold_daily_0230",
                            bash_command="python jobs/gold_job.py --business-date {{ ds }}")
        silver >> wait_station >> gold  # noqa: B015
