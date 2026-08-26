import shutil
import subprocess
import sys
from pathlib import Path

import duckdb
import pytest

pytestmark = pytest.mark.e2e

ROOT = Path(__file__).resolve().parents[1]
DBT_DIR = ROOT / "transform" / "dbt"
SLOT = "payments_cdc_determinism"
BRONZE = ROOT / "data" / "test-bronze" / "determinism"
SILVER = ROOT / "data" / "test-silver" / "determinism"
GOLD = ROOT / "data" / "test-gold" / "determinism.duckdb"
STEP_TIMEOUT_SECONDS = 600

GOLD_TABLES = (
    "dim_customer",
    "dim_merchant",
    "dim_currency",
    "dim_date",
    "fct_payment",
)


@pytest.fixture
def pipeline(postgres, reset_source, drop_slot):
    reset_source(SLOT)
    for path in (BRONZE, SILVER):
        shutil.rmtree(path, ignore_errors=True)
    GOLD.parent.mkdir(parents=True, exist_ok=True)
    if GOLD.exists():
        GOLD.unlink()
    yield postgres
    drop_slot(SLOT)


def run_silver() -> None:
    completed = subprocess.run(
        [
            "docker", "compose", "--profile", "jobs", "run", "--rm",
            "-e", "CDC_BRONZE_ROOT={}".format(BRONZE.relative_to(ROOT).as_posix()),
            "-e", "SILVER_ROOT={}".format(SILVER.relative_to(ROOT).as_posix()),
            "spark",
            "/opt/spark/bin/spark-submit", "--master", "local[*]",
            "transform/spark/bronze_to_silver.py",
        ],
        cwd=str(ROOT), capture_output=True, timeout=STEP_TIMEOUT_SECONDS,
    )
    output = completed.stdout.decode("utf-8", "replace") + completed.stderr.decode("utf-8", "replace")
    assert completed.returncode == 0, output


def run_gold() -> None:
    environment = {
        "DBT_PROFILES_DIR": ".",
        "GOLD_DB": str(GOLD),
        "SILVER_ROOT": str(SILVER),
        "FX_BRONZE_ROOT": str(ROOT / "data" / "bronze" / "fx"),
    }
    completed = subprocess.run(
        [sys.executable, "-m", "dbt.cli.main", "build"],
        cwd=str(DBT_DIR),
        capture_output=True,
        timeout=STEP_TIMEOUT_SECONDS,
        env={**dict(__import__("os").environ), **environment},
    )
    output = completed.stdout.decode("utf-8", "replace") + completed.stderr.decode("utf-8", "replace")
    assert completed.returncode == 0, output


def gold_fingerprint() -> dict:
    connection = duckdb.connect(str(GOLD), read_only=True)
    try:
        return {
            table: connection.sql(
                "SELECT count(*), md5(string_agg(row_text, '|' ORDER BY row_text)) "
                "FROM (SELECT t::VARCHAR AS row_text FROM {} t)".format(table)
            ).fetchone()
            for table in GOLD_TABLES
        }
    finally:
        connection.close()


def seed(connection) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            "INSERT INTO customers (full_name, email, cpf) "
            "VALUES ('Determinism', 'd@example.invalid', '00000000003') RETURNING customer_id"
        )
        customer_id = cursor.fetchone()[0]
        cursor.execute(
            "INSERT INTO merchants (legal_name, category, country) "
            "VALUES ('Determinism LTDA', 'retail', 'BR') RETURNING merchant_id"
        )
        merchant_id = cursor.fetchone()[0]
        for index, currency in enumerate(("BRL", "USD", "EUR")):
            cursor.execute(
                "INSERT INTO payments (customer_id, merchant_id, amount, currency, status) "
                "VALUES (%s, %s, %s, %s, 'pending') RETURNING payment_id",
                (customer_id, merchant_id, 100 + index, currency),
            )
            cursor.execute(
                "UPDATE payments SET status = 'captured', updated_at = now() "
                "WHERE payment_id = %s",
                (cursor.fetchone()[0],),
            )
        cursor.execute(
            "UPDATE customers SET full_name = 'Determinism Renamed', updated_at = now() "
            "WHERE customer_id = %s",
            (customer_id,),
        )


def test_running_the_pipeline_twice_leaves_the_gold_identical(pipeline, run_consumer):
    seed(pipeline)

    fingerprints = []
    for attempt in range(2):
        result = run_consumer(SLOT, BRONZE)
        assert result.returncode == 0, result.stderr.decode("utf-8", "replace")
        run_silver()
        run_gold()
        fingerprints.append(gold_fingerprint())

    first, second = fingerprints
    assert all(count > 0 for count, _ in first.values()), (
        "the gold is empty, so identical fingerprints would prove nothing"
    )
    differing = {
        table: (first[table], second[table])
        for table in GOLD_TABLES
        if first[table] != second[table]
    }
    assert not differing, "the gold changed between two runs: {}".format(differing)
