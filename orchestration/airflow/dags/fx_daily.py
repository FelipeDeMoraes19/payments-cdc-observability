from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

PROJECT = "/opt/project"

with DAG(
    dag_id="fx_daily",
    description="Daily PTAX rates from the central bank. Partitioned by trading date.",
    start_date=datetime(2026, 8, 1),
    schedule="0 6 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["ingestion", "batch"],
) as dag:
    BashOperator(
        task_id="extract_rates",
        cwd=PROJECT,
        env={
            "PYTHONPATH": PROJECT,
            "FX_START_DATE": "{{ data_interval_start | ds }}",
            "FX_END_DATE": "{{ data_interval_start | ds }}",
        },
        append_env=True,
        bash_command="python -m ingestion.batch.bcb_fx",
    )
