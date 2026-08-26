import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import psycopg2
import pytest

from contracts.postgres_types import TYPE_OIDS
from ingestion.env import load_env_file

ROOT = Path(__file__).resolve().parents[1]
BRONZE_ROOT = ROOT / "data" / "test-bronze" / "contract"
SLOT = "payments_cdc_contract_test"
DRAIN_TIMEOUT_SECONDS = 60


def source_params() -> dict:
    return dict(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "5434"),
        dbname=os.environ.get("POSTGRES_DB", "payments"),
        user=os.environ.get("POSTGRES_USER", "payments"),
        password=os.environ.get("POSTGRES_PASSWORD", "payments"),
    )


def restore_amount_column() -> str:
    return (
        "ALTER TABLE payments ALTER COLUMN amount TYPE numeric(14,2) "
        "USING amount::numeric(14,2)"
    )


@pytest.fixture
def source():
    load_env_file()
    connection = psycopg2.connect(**source_params())
    connection.autocommit = True
    with connection.cursor() as cursor:
        cursor.execute(restore_amount_column())
        cursor.execute(
            "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
            "WHERE slot_name = %s",
            (SLOT,),
        )
        cursor.execute("DELETE FROM payments")
        cursor.execute("DELETE FROM customers")
        cursor.execute("DELETE FROM merchants")
        cursor.execute("SELECT pg_create_logical_replication_slot(%s, 'pgoutput')", (SLOT,))
    shutil.rmtree(BRONZE_ROOT, ignore_errors=True)
    yield connection
    with connection.cursor() as cursor:
        cursor.execute(restore_amount_column())
        cursor.execute(
            "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
            "WHERE slot_name = %s",
            (SLOT,),
        )
    connection.close()


def run_consumer() -> subprocess.CompletedProcess:
    environment = dict(os.environ)
    environment["PYTHONPATH"] = str(ROOT)
    environment["CDC_SLOT"] = SLOT
    environment["CDC_BRONZE_ROOT"] = str(BRONZE_ROOT)
    environment["CDC_IDLE_TIMEOUT"] = "4"
    environment["CDC_BATCH_MAX_SECONDS"] = "1"
    return subprocess.run(
        [sys.executable, "-m", "ingestion.cdc.consumer"],
        cwd=str(ROOT),
        env=environment,
        capture_output=True,
        timeout=DRAIN_TIMEOUT_SECONDS,
    )


def bronze_records() -> list:
    records = []
    for path in sorted(BRONZE_ROOT.rglob("*.jsonl")):
        for line in path.read_text(encoding="utf-8").splitlines():
            if line:
                records.append(json.loads(line))
    return records


def seed_payment(connection, amount: str) -> None:
    with connection.cursor() as cursor:
        cursor.execute("SELECT customer_id FROM customers LIMIT 1")
        row = cursor.fetchone()
        if row is None:
            cursor.execute(
                "INSERT INTO customers (full_name, email, cpf) "
                "VALUES ('Test Customer', 'test@example.invalid', '00000000000') "
                "RETURNING customer_id"
            )
            customer_id = cursor.fetchone()[0]
            cursor.execute(
                "INSERT INTO merchants (legal_name, category, country) "
                "VALUES ('Test Merchant LTDA', 'retail', 'BR') RETURNING merchant_id"
            )
            merchant_id = cursor.fetchone()[0]
        else:
            customer_id = row[0]
            cursor.execute("SELECT merchant_id FROM merchants LIMIT 1")
            merchant_id = cursor.fetchone()[0]
        cursor.execute(
            "INSERT INTO payments (customer_id, merchant_id, amount, currency, status) "
            "VALUES (%s, %s, %s, 'BRL', 'pending')",
            (customer_id, merchant_id, amount),
        )


def test_registered_type_oids_match_the_server(source):
    with source.cursor() as cursor:
        cursor.execute(
            "SELECT typname, oid FROM pg_type WHERE typname = ANY(%s)",
            (list(TYPE_OIDS),),
        )
        server = dict(cursor.fetchall())
    assert server == TYPE_OIDS


def test_column_type_change_fails_loudly_and_writes_nothing_new(source):
    seed_payment(source, "199.90")
    healthy = run_consumer()
    assert healthy.returncode == 0, healthy.stderr.decode("utf-8", "replace")
    before = bronze_records()
    assert before, "nothing was captured before the schema changed"

    with source.cursor() as cursor:
        cursor.execute("ALTER TABLE payments ALTER COLUMN amount TYPE text")
    seed_payment(source, "424.20")

    broken = run_consumer()
    message = broken.stderr.decode("utf-8", "replace")

    assert broken.returncode != 0, "the consumer accepted a changed column type"
    assert "contract violation" in message
    assert "payments.amount" in message
    assert "numeric" in message and "text" in message

    assert bronze_records() == before, "a record of the new shape reached bronze"
