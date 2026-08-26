import json
import os
import shutil
import subprocess
import sys
import time
from collections import defaultdict
from pathlib import Path

import psycopg2
import pytest

from ingestion.env import load_env_file

ROOT = Path(__file__).resolve().parents[1]
BRONZE_ROOT = ROOT / "data" / "test-bronze" / "cdc"
SLOT = "payments_cdc_restart_test"
GENERATOR_SECONDS = 12
KILL_AFTER_SECONDS = 3
DRAIN_TIMEOUT_SECONDS = 60


def source_params() -> dict:
    return dict(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "5434"),
        dbname=os.environ.get("POSTGRES_DB", "payments"),
        user=os.environ.get("POSTGRES_USER", "payments"),
        password=os.environ.get("POSTGRES_PASSWORD", "payments"),
    )


def child_environment(**overrides) -> dict:
    environment = dict(os.environ)
    environment["PYTHONPATH"] = str(ROOT)
    environment["CDC_SLOT"] = SLOT
    environment["CDC_BRONZE_ROOT"] = str(BRONZE_ROOT)
    environment.update(overrides)
    return environment


@pytest.fixture
def clean_source():
    load_env_file()
    connection = psycopg2.connect(**source_params())
    connection.autocommit = True
    with connection.cursor() as cursor:
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
        cursor.execute(
            "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
            "WHERE slot_name = %s",
            (SLOT,),
        )
    connection.close()


def start_consumer(idle_timeout: str, **overrides) -> subprocess.Popen:
    settings = {"CDC_IDLE_TIMEOUT": idle_timeout, "CDC_BATCH_MAX_SECONDS": "1"}
    settings.update(overrides)
    return subprocess.Popen(
        [sys.executable, "-m", "ingestion.cdc.consumer"],
        cwd=str(ROOT),
        env=child_environment(**settings),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )


def seed_changes(connection, payment_count: int) -> None:
    with connection.cursor() as cursor:
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
        for index in range(payment_count):
            cursor.execute(
                "INSERT INTO payments (customer_id, merchant_id, amount, currency, status) "
                "VALUES (%s, %s, %s, 'BRL', 'pending') RETURNING payment_id",
                (customer_id, merchant_id, 10.00 + index),
            )
            cursor.execute(
                "UPDATE payments SET status = 'captured', updated_at = now() "
                "WHERE payment_id = %s",
                (cursor.fetchone()[0],),
            )


def event_of(record: dict) -> str:
    return json.dumps(
        {name: value for name, value in record.items() if name != "ingested_at"},
        sort_keys=True,
    )


def identity_of(record: dict) -> tuple:
    return (record["table"], json.dumps(record["key"], sort_keys=True), record["lsn"])


def read_bronze() -> list:
    records = []
    for path in sorted(BRONZE_ROOT.rglob("*.jsonl")):
        for line in path.read_text(encoding="utf-8").splitlines():
            if line:
                records.append(json.loads(line))
    return records


def source_payment_ids(connection) -> set:
    with connection.cursor() as cursor:
        cursor.execute("SELECT payment_id FROM payments")
        return {str(row[0]) for row in cursor.fetchall()}


def test_consumer_restart_loses_nothing_and_duplicates_are_exact(clean_source):
    generator = subprocess.Popen(
        [sys.executable, "-m", "generator.synthetic_load"],
        cwd=str(ROOT),
        env=child_environment(
            GEN_DURATION_SECONDS=str(GENERATOR_SECONDS), GEN_RATE_PER_SECOND="25"
        ),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    victim = start_consumer(idle_timeout="0")
    time.sleep(KILL_AFTER_SECONDS)
    assert victim.poll() is None, "consumer exited on its own before it could be killed"
    victim.kill()
    victim.wait(timeout=10)

    generator.wait(timeout=GENERATOR_SECONDS + 30)

    survivor = start_consumer(idle_timeout="4")
    survivor.wait(timeout=DRAIN_TIMEOUT_SECONDS)
    assert survivor.returncode == 0, survivor.stderr.read().decode("utf-8", "replace")

    records = read_bronze()
    assert records, "no records reached bronze"

    inserted_ids = {
        record["key"]["payment_id"]
        for record in records
        if record["table"] == "payments" and record["action"] == "insert"
    }
    assert inserted_ids == source_payment_ids(clean_source)

    grouped = defaultdict(set)
    for record in records:
        grouped[identity_of(record)].add(event_of(record))
    conflicting = {key: len(events) for key, events in grouped.items() if len(events) > 1}
    assert not conflicting, "same (table, key, lsn) carried different events: {}".format(
        conflicting
    )


def test_records_written_but_never_confirmed_are_replayed_identically(clean_source):
    seed_changes(clean_source, payment_count=12)

    crashing = start_consumer(
        idle_timeout="10",
        CDC_BATCH_MAX_RECORDS="1000",
        CDC_FAIL_BEFORE_FEEDBACK="1",
    )
    crashing.wait(timeout=DRAIN_TIMEOUT_SECONDS)
    assert crashing.returncode == 17, crashing.stderr.read().decode("utf-8", "replace")

    after_crash = read_bronze()
    assert after_crash, "the injected failure fired before anything was written"

    survivor = start_consumer(
        idle_timeout="4", CDC_BATCH_MAX_RECORDS="3", CDC_BATCH_MAX_SECONDS="30"
    )
    survivor.wait(timeout=DRAIN_TIMEOUT_SECONDS)
    assert survivor.returncode == 0, survivor.stderr.read().decode("utf-8", "replace")

    records = read_bronze()
    grouped = defaultdict(set)
    for record in records:
        grouped[identity_of(record)].add(event_of(record))

    replayed = len(records) - len(grouped)
    assert replayed > 0, "nothing was replayed, so nothing about duplicates was proven"

    conflicting = {key: len(events) for key, events in grouped.items() if len(events) > 1}
    assert not conflicting, "same (table, key, lsn) carried different events: {}".format(
        conflicting
    )

    assert {
        record["key"]["payment_id"]
        for record in records
        if record["table"] == "payments" and record["action"] == "insert"
    } == source_payment_ids(clean_source)
