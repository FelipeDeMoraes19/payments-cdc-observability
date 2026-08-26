import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from ingestion.cdc.bronze import BronzeWriter, PendingRecord

ROOT = Path(__file__).resolve().parents[1]
SPARK_TIMEOUT_SECONDS = 600

INCOMPRESSIBLE = (
    "SELECT string_agg(md5(random()::text), '') FROM generate_series(1, 300)"
)


def run_spark(bronze_root: Path, silver_root: Path):
    """Run the silver job in its container and return the exit code and all output.

    docker compose run folds the container's stdout and stderr together onto its
    own stdout, keeping its own stderr for compose's messages. Asserting on
    result.stderr alone silently misses everything the job printed, so the two
    streams are joined here rather than at every call site.
    """
    completed = subprocess.run(
        [
            "docker", "compose", "--profile", "jobs", "run", "--rm",
            "-e", "CDC_BRONZE_ROOT={}".format(bronze_root.relative_to(ROOT).as_posix()),
            "-e", "SILVER_ROOT={}".format(silver_root.relative_to(ROOT).as_posix()),
            "spark",
            "/opt/spark/bin/spark-submit", "--master", "local[*]",
            "transform/spark/bronze_to_silver.py",
        ],
        cwd=str(ROOT),
        capture_output=True,
        timeout=SPARK_TIMEOUT_SECONDS,
    )
    output = completed.stdout.decode("utf-8", "replace")
    output += completed.stderr.decode("utf-8", "replace")
    return completed.returncode, output


def read_silver(silver_root: Path, table: str) -> list:
    return pq.read_table(silver_root / table).to_pylist()


@pytest.fixture
def scenario(postgres, reset_source, drop_slot, request):
    slot = "payments_cdc_silver_{}".format(request.node.name[-12:].replace("[", ""))
    bronze = ROOT / "data" / "test-bronze" / "silver" / request.node.name
    silver = ROOT / "data" / "test-silver" / request.node.name
    reset_source(slot)
    shutil.rmtree(bronze, ignore_errors=True)
    shutil.rmtree(silver, ignore_errors=True)
    yield postgres, slot, bronze, silver
    drop_slot(slot)


def drain(run_consumer, slot, bronze) -> None:
    result = run_consumer(slot, bronze)
    assert result.returncode == 0, result.stderr.decode("utf-8", "replace")


def test_delete_keeps_the_row_and_an_unchanged_toast_column_survives(
    scenario, run_consumer, read_bronze
):
    postgres, slot, bronze, silver = scenario
    with postgres.cursor() as cursor:
        cursor.execute(
            "INSERT INTO customers (full_name, email, cpf) "
            "VALUES ('Doomed Customer', 'doomed@example.invalid', '00000000000') "
            "RETURNING customer_id"
        )
        customer_id = cursor.fetchone()[0]
        cursor.execute(
            "INSERT INTO merchants (legal_name, category, country) "
            "SELECT ({}), 'retail', 'BR' RETURNING merchant_id".format(INCOMPRESSIBLE)
        )
        merchant_id = cursor.fetchone()[0]
        cursor.execute("SELECT legal_name FROM merchants WHERE merchant_id = %s", (merchant_id,))
        long_name = cursor.fetchone()[0]
    drain(run_consumer, slot, bronze)

    with postgres.cursor() as cursor:
        cursor.execute(
            "UPDATE merchants SET category = 'marketplace', updated_at = now() "
            "WHERE merchant_id = %s",
            (merchant_id,),
        )
        cursor.execute("DELETE FROM customers WHERE customer_id = %s", (customer_id,))
    drain(run_consumer, slot, bronze)

    events = read_bronze(bronze)
    toast_events = [
        event for event in events
        if event["table_name"] == "merchants" and event["action"] == "update"
    ]
    assert toast_events, "no merchant update reached bronze"
    assert toast_events[0]["unchanged_toast_columns"] == ["legal_name"], (
        "the update did not arrive with an unchanged TOAST marker, so this test "
        "would prove nothing"
    )
    assert any(
        event["table_name"] == "customers" and event["action"] == "delete"
        for event in events
    ), "no customer delete reached bronze"

    returncode, output = run_spark(bronze, silver)
    assert returncode == 0, output

    merchants = {row["merchant_id"]: row for row in read_silver(silver, "merchants")}
    assert merchants[merchant_id]["legal_name"] == long_name
    assert merchants[merchant_id]["category"] == "marketplace"
    assert merchants[merchant_id]["is_deleted"] is False

    customers = {row["customer_id"]: row for row in read_silver(silver, "customers")}
    assert customers[customer_id]["is_deleted"] is True
    assert customers[customer_id]["cpf"] is not None, (
        "the deleted row lost the values it had before it died"
    )


def test_truncate_marks_every_earlier_key_as_deleted(scenario, run_consumer, read_bronze):
    postgres, slot, bronze, silver = scenario
    with postgres.cursor() as cursor:
        cursor.execute(
            "INSERT INTO customers (full_name, email, cpf) "
            "VALUES ('Truncated Customer', 'gone@example.invalid', '00000000001')"
        )
    drain(run_consumer, slot, bronze)

    with postgres.cursor() as cursor:
        cursor.execute("TRUNCATE payments, customers, merchants")
    drain(run_consumer, slot, bronze)

    assert any(
        event["action"] == "truncate" for event in read_bronze(bronze)
    ), "no truncate reached bronze"

    returncode, output = run_spark(bronze, silver)
    assert returncode == 0, output

    customers = read_silver(silver, "customers")
    assert customers, "the truncate removed the rows instead of marking them"
    for row in customers:
        assert row["is_deleted"] is True
        assert row["truncated_at_lsn"] is not None


def test_a_value_bronze_cannot_type_fails_loudly_naming_the_column(scenario):
    _, _, bronze, silver = scenario
    writer = BronzeWriter(bronze)
    writer.write(
        [
            PendingRecord(
                lsn=1,
                table="payments",
                partition="2026-08-26",
                payload={
                    "source": "postgres",
                    "ingested_at": datetime.now(timezone.utc),
                    "lsn": "0/1",
                    "lsn_numeric": 1,
                    "xid": 1,
                    "commit_time": datetime.now(timezone.utc),
                    "action": "insert",
                    "schema_name": "public",
                    "table_name": "payments",
                    "key": {"payment_id": "1"},
                    "before": None,
                    "after": {
                        "payment_id": "1",
                        "customer_id": "1",
                        "merchant_id": "1",
                        "amount": "not-a-number",
                        "currency": "BRL",
                        "status": "pending",
                        "created_at": "2026-08-26 00:00:00+00",
                        "updated_at": "2026-08-26 00:00:00+00",
                    },
                    "unchanged_toast_columns": [],
                    "truncate_cascade": None,
                    "truncate_restart_identity": None,
                },
            )
        ]
    )

    returncode, message = run_spark(bronze, silver)

    assert returncode != 0, "silver accepted a value it cannot type"
    assert "public.payments.amount" in message, "the failure does not name the column"
    assert "not-a-number" in message, "the failure does not show the observed value"
    assert "decimal(14,2)" in message, "the failure does not name the expected type"
