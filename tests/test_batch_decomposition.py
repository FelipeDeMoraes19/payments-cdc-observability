import shutil
import subprocess
import sys
from pathlib import Path

import duckdb
import pytest

from ingestion.cdc import bronze

pytestmark = pytest.mark.e2e

ROOT = Path(__file__).resolve().parents[1]
DBT_DIR = ROOT / "transform" / "dbt"
SLOT_ONE_BATCH = "payments_cdc_one_batch"
SLOT_MANY_BATCHES = "payments_cdc_many_batches"
STEP_TIMEOUT_SECONDS = 600

GOLD_TABLES = ("dim_customer", "dim_merchant", "dim_currency", "dim_date", "fct_payment")


def workspace(name: str) -> tuple:
    bronze_root = ROOT / "data" / "test-bronze" / "decomposition" / name
    silver_root = ROOT / "data" / "test-silver" / "decomposition" / name
    gold_db = ROOT / "data" / "test-gold" / "decomposition-{}.duckdb".format(name)
    return bronze_root, silver_root, gold_db


@pytest.fixture
def two_slots(postgres, reset_source, drop_slot):
    reset_source(SLOT_ONE_BATCH)
    with postgres.cursor() as cursor:
        cursor.execute(
            "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
            "WHERE slot_name = %s",
            (SLOT_MANY_BATCHES,),
        )
        cursor.execute(
            "SELECT pg_create_logical_replication_slot(%s, 'pgoutput')",
            (SLOT_MANY_BATCHES,),
        )
    for name in ("one-batch", "many-batches"):
        bronze_root, silver_root, gold_db = workspace(name)
        shutil.rmtree(bronze_root, ignore_errors=True)
        shutil.rmtree(silver_root, ignore_errors=True)
        bronze_root.mkdir(parents=True, exist_ok=True)
        silver_root.mkdir(parents=True, exist_ok=True)
        gold_db.parent.mkdir(parents=True, exist_ok=True)
        if gold_db.exists():
            gold_db.unlink()
    yield postgres
    drop_slot(SLOT_ONE_BATCH)
    drop_slot(SLOT_MANY_BATCHES)


def seed(connection) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            "INSERT INTO customers (full_name, email, cpf) "
            "VALUES ('Decomposed', 'dec@example.invalid', '00000000004') "
            "RETURNING customer_id"
        )
        customer_id = cursor.fetchone()[0]
        cursor.execute(
            "INSERT INTO merchants (legal_name, category, country) "
            "VALUES ('Decomposed LTDA', 'retail', 'BR') RETURNING merchant_id"
        )
        merchant_id = cursor.fetchone()[0]
        for index in range(8):
            cursor.execute(
                "INSERT INTO payments (customer_id, merchant_id, amount, currency, status) "
                "VALUES (%s, %s, %s, %s, 'pending') RETURNING payment_id",
                (customer_id, merchant_id, 10 + index, ("BRL", "USD", "EUR")[index % 3]),
            )
            cursor.execute(
                "UPDATE payments SET status = 'captured', updated_at = now() "
                "WHERE payment_id = %s",
                (cursor.fetchone()[0],),
            )
        cursor.execute(
            "UPDATE customers SET full_name = 'Decomposed Again', updated_at = now() "
            "WHERE customer_id = %s",
            (customer_id,),
        )


def run_silver(bronze_root: Path, silver_root: Path) -> None:
    completed = subprocess.run(
        [
            "docker", "compose", "--profile", "jobs", "run", "--rm",
            "-e", "CDC_BRONZE_ROOT={}".format(bronze_root.relative_to(ROOT).as_posix()),
            "-e", "SILVER_ROOT={}".format(silver_root.relative_to(ROOT).as_posix()),
            "runner",
            "bash", "-c",
            "spark-submit --master 'local[*]' transform/spark/bronze_to_silver.py",
        ],
        cwd=str(ROOT), capture_output=True, timeout=STEP_TIMEOUT_SECONDS,
    )
    output = completed.stdout.decode("utf-8", "replace") + completed.stderr.decode("utf-8", "replace")
    assert completed.returncode == 0, output


def run_gold(silver_root: Path, gold_db: Path) -> None:
    import os

    completed = subprocess.run(
        [sys.executable, "-m", "dbt.cli.main", "build"],
        cwd=str(DBT_DIR), capture_output=True, timeout=STEP_TIMEOUT_SECONDS,
        env={
            **dict(os.environ),
            "DBT_PROFILES_DIR": ".",
            "GOLD_DB": str(gold_db),
            "SILVER_ROOT": str(silver_root),
            "FX_BRONZE_ROOT": str(ROOT / "data" / "bronze" / "fx"),
        },
    )
    output = completed.stdout.decode("utf-8", "replace") + completed.stderr.decode("utf-8", "replace")
    assert completed.returncode == 0, output


def events(bronze_root: Path) -> list:
    return sorted(
        str({name: value for name, value in record.items() if name != "ingested_at"})
        for record in bronze.read(bronze_root)
    )


def silver_rows(silver_root: Path) -> list:
    connection = duckdb.connect()
    try:
        return sorted(
            str(row)
            for table in ("customers", "merchants", "payments")
            for row in connection.sql(
                "SELECT * FROM read_parquet('{}/{}/*.parquet')".format(
                    silver_root.as_posix(), table
                )
            ).fetchall()
        )
    finally:
        connection.close()


def gold_fingerprint(gold_db: Path) -> dict:
    connection = duckdb.connect(str(gold_db), read_only=True)
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


def test_draining_in_many_batches_matches_draining_in_one(two_slots, run_consumer):
    """Compositionality for the half of the pipeline that has no dates.

    A change stream cannot be replayed by date, so the equivalent question is
    whether the size of the batch the consumer flushes leaks into the result. Two
    slots are created before any change, so both see exactly the same changes;
    one drains them in a single batch and the other one record at a time.

    Batch size is the axis where is_current would break if silver ever became
    incremental, which is why this test exists before that is attempted.
    """
    seed(two_slots)

    one_bronze, one_silver, one_gold = workspace("one-batch")
    many_bronze, many_silver, many_gold = workspace("many-batches")

    assert run_consumer(
        SLOT_ONE_BATCH, one_bronze,
        CDC_BATCH_MAX_RECORDS=100000, CDC_BATCH_MAX_SECONDS=3600,
    ).returncode == 0
    assert run_consumer(
        SLOT_MANY_BATCHES, many_bronze,
        CDC_BATCH_MAX_RECORDS=1, CDC_BATCH_MAX_SECONDS=3600,
    ).returncode == 0

    one_files = list(one_bronze.rglob("*.parquet"))
    many_files = list(many_bronze.rglob("*.parquet"))
    assert one_files and many_files, "one of the drains wrote nothing"
    assert len(many_files) > len(one_files), (
        "both drains produced the same number of files, so the batch size never "
        "differed and this test proves nothing"
    )

    assert events(one_bronze) == events(many_bronze)

    run_silver(one_bronze, one_silver)
    run_silver(many_bronze, many_silver)
    assert silver_rows(one_silver) == silver_rows(many_silver)

    run_gold(one_silver, one_gold)
    run_gold(many_silver, many_gold)
    assert gold_fingerprint(one_gold) == gold_fingerprint(many_gold)
