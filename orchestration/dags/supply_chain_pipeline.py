# ================================================
# SUPPLY CHAIN INTELLIGENCE PLATFORM
# Airflow DAG — Orchestrates full pipeline
# Runs daily at 9am automatically
# ================================================

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import subprocess
import sys

# ── Default Arguments ────────────────────────────
# Why these defaults:
# - retries=2: if a task fails retry twice before marking failed
# - retry_delay=5mins: wait 5 mins between retries
# - email_on_failure=False: no email setup locally
default_args = {
    "owner"           : "lovepreet",
    "retries"         : 2,
    "retry_delay"     : timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry"  : False,
    "start_date"      : datetime(2026, 5, 13)
}

# ── DAG Definition ───────────────────────────────
# Why schedule_interval daily:
# New shipment data comes every day
# Pipeline refreshes KPIs every morning
# Dashboard always shows fresh data
with DAG(
    dag_id="supply_chain_pipeline",
    default_args=default_args,
    description="End to end supply chain data pipeline",
    schedule_interval="0 9 * * *",  # 9am every day
    catchup=False,  # don't backfill missed runs
    tags=["supply-chain", "data-engineering", "genai"]
) as dag:

    t1_kafka_producer = BashOperator(
        task_id="kafka_producer",
        bash_command="python3 /opt/airflow/ingestion/kafka_producer.py",
    )

    t2_kafka_consumer = BashOperator(
        task_id="kafka_consumer",
        bash_command="python3 /opt/airflow/ingestion/kafka_consumer.py",
    )

    t3_silver = BashOperator(
        task_id="silver_transformation",
        bash_command="python3 /opt/airflow/processing/silver_transform.py",
    )

    t4_gold = BashOperator(
        task_id="gold_transformation",
        bash_command="python3 /opt/airflow/processing/gold_transform.py",
    )

    # ── Task 5: Data Quality Checks ─────────────
    t5_quality = BashOperator(
        task_id="quality_checks",
        bash_command="python3 /opt/airflow/quality/silver_quality_checks.py",
        dag=dag
    )

    # ── Task Dependencies ────────────────────────
    # Why this order: each task depends on
    # previous task completing successfully
    # >> means "then run"
    t1_kafka_producer >> t2_kafka_consumer >> t3_silver >> t4_gold >>t5_quality