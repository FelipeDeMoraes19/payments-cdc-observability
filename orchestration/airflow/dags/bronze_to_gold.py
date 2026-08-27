from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

PROJECT = "/opt/project"

with DAG(
    dag_id="bronze_to_gold",
    description=(
        "Rebuild silver and gold from whatever bronze holds. Position based, not date "
        "based: the CDC service feeds bronze continuously and a slot has one position, "
        "not a calendar."
    ),
    start_date=datetime(2026, 8, 1),
    schedule="*/15 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["transform"],
) as dag:
    silver = BashOperator(
        task_id="build_silver",
        cwd=PROJECT,
        env={"PYTHONPATH": PROJECT},
        append_env=True,
        bash_command="spark-submit --master 'local[*]' transform/spark/bronze_to_silver.py",
    )

    gold = BashOperator(
        task_id="build_gold",
        cwd="{}/transform/dbt".format(PROJECT),
        env={
            "DBT_PROFILES_DIR": ".",
            "GOLD_DB": "{}/data/gold/payments.duckdb".format(PROJECT),
            "SILVER_ROOT": "{}/data/silver".format(PROJECT),
            "FX_BRONZE_ROOT": "{}/data/bronze/fx".format(PROJECT),
        },
        append_env=True,
        bash_command="dbt build",
    )

    silver >> gold
