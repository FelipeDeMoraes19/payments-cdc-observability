import json
import shutil
import subprocess
import sys
import time
from collections import defaultdict
from pathlib import Path

import pytest

pytestmark = pytest.mark.e2e

ROOT = Path(__file__).resolve().parents[1]
BRONZE_ROOT = ROOT / "data" / "test-bronze" / "restart"
SLOT = "payments_cdc_restart_test"
GENERATOR_SECONDS = 12
KILL_AFTER_SECONDS = 3
DRAIN_TIMEOUT_SECONDS = 60


@pytest.fixture
def source(postgres, reset_source, drop_slot):
    reset_source(SLOT)
    shutil.rmtree(BRONZE_ROOT, ignore_errors=True)
    yield postgres
    drop_slot(SLOT)


def event_of(record: dict) -> str:
    return json.dumps(
        {name: value for name, value in record.items() if name != "ingested_at"},
        sort_keys=True,
        default=str,
    )


def identity_of(record: dict) -> tuple:
    return (record["table_name"], json.dumps(record["key"], sort_keys=True), record["lsn"])


def source_payment_ids(connection) -> set:
    with connection.cursor() as cursor:
        cursor.execute("SELECT payment_id FROM payments")
        return {str(row[0]) for row in cursor.fetchall()}


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


def test_consumer_restart_loses_nothing(source, consumer_env, read_bronze):
    """Kill the consumer mid-stream and prove the bronze still matches the source.

    Popen.kill() is TerminateProcess on Windows, not a literal SIGKILL: Windows
    has no POSIX signals. It is the closest equivalent, immediate and impossible
    for the process to catch or handle, which is what this test needs. On a POSIX
    host the same call sends SIGKILL.

    This test proves the first half of the milestone criterion, that nothing is
    lost. The second half, that every duplicate is byte identical as an event, is
    proved by the injected failure test below: the window between the durable
    write and the confirmation is microseconds wide, so a kill at an arbitrary
    moment practically never lands in it and produces no duplicates at all.
    """
    generator = subprocess.Popen(
        [sys.executable, "-m", "generator.synthetic_load"],
        cwd=str(ROOT),
        env=consumer_env(
            SLOT,
            BRONZE_ROOT,
            GEN_DURATION_SECONDS=GENERATOR_SECONDS,
            GEN_RATE_PER_SECOND=25,
        ),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    victim = subprocess.Popen(
        [sys.executable, "-m", "ingestion.cdc.consumer"],
        cwd=str(ROOT),
        env=consumer_env(SLOT, BRONZE_ROOT, CDC_IDLE_TIMEOUT=0),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    time.sleep(KILL_AFTER_SECONDS)
    assert victim.poll() is None, "consumer exited on its own before it could be killed"
    victim.kill()
    victim.wait(timeout=10)

    generator.wait(timeout=GENERATOR_SECONDS + 30)

    survivor = subprocess.run(
        [sys.executable, "-m", "ingestion.cdc.consumer"],
        cwd=str(ROOT),
        env=consumer_env(SLOT, BRONZE_ROOT),
        capture_output=True,
        timeout=DRAIN_TIMEOUT_SECONDS,
    )
    assert survivor.returncode == 0, survivor.stderr.decode("utf-8", "replace")

    records = read_bronze(BRONZE_ROOT)
    assert records, "no records reached bronze"

    inserted = {
        record["key"]["payment_id"]
        for record in records
        if record["table_name"] == "payments" and record["action"] == "insert"
    }
    assert inserted == source_payment_ids(source)

    grouped = defaultdict(set)
    for record in records:
        grouped[identity_of(record)].add(event_of(record))
    conflicting = {key: len(events) for key, events in grouped.items() if len(events) > 1}
    assert not conflicting, "same (table, key, lsn) carried different events: {}".format(
        conflicting
    )


def test_records_written_but_never_confirmed_are_replayed_identically(
    source, run_consumer, read_bronze
):
    seed_changes(source, payment_count=12)

    crashing = run_consumer(
        SLOT,
        BRONZE_ROOT,
        CDC_IDLE_TIMEOUT=10,
        CDC_BATCH_MAX_RECORDS=1000,
        CDC_FAIL_BEFORE_FEEDBACK=1,
    )
    assert crashing.returncode == 17, crashing.stderr.decode("utf-8", "replace")
    assert read_bronze(BRONZE_ROOT), "the injected failure fired before anything was written"

    survivor = run_consumer(
        SLOT, BRONZE_ROOT, CDC_BATCH_MAX_RECORDS=3, CDC_BATCH_MAX_SECONDS=30
    )
    assert survivor.returncode == 0, survivor.stderr.decode("utf-8", "replace")

    records = read_bronze(BRONZE_ROOT)
    grouped = defaultdict(set)
    for record in records:
        grouped[identity_of(record)].add(event_of(record))

    assert len(records) - len(grouped) > 0, "nothing was replayed, so nothing was proven"

    conflicting = {key: len(events) for key, events in grouped.items() if len(events) > 1}
    assert not conflicting, "same (table, key, lsn) carried different events: {}".format(
        conflicting
    )

    assert {
        record["key"]["payment_id"]
        for record in records
        if record["table_name"] == "payments" and record["action"] == "insert"
    } == source_payment_ids(source)
