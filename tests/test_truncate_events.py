import shutil
from pathlib import Path

import pytest

BRONZE_ROOT = Path(__file__).resolve().parents[1] / "data" / "test-bronze" / "truncate"
SLOT = "payments_cdc_truncate_test"


@pytest.fixture
def source(postgres, reset_source, drop_slot):
    reset_source(SLOT)
    shutil.rmtree(BRONZE_ROOT, ignore_errors=True)
    yield postgres
    drop_slot(SLOT)


def seed_one_of_each(connection) -> None:
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
        cursor.execute(
            "INSERT INTO payments (customer_id, merchant_id, amount, currency, status) "
            "VALUES (%s, %s, 12.34, 'BRL', 'pending')",
            (customer_id, merchant_id),
        )


def truncate_records(records: list) -> list:
    return [record for record in records if record["action"] == "truncate"]


def test_truncate_becomes_one_event_per_table(source, run_consumer, read_bronze):
    seed_one_of_each(source)
    with source.cursor() as cursor:
        cursor.execute("TRUNCATE payments, customers, merchants RESTART IDENTITY")

    result = run_consumer(SLOT, BRONZE_ROOT)
    assert result.returncode == 0, result.stderr.decode("utf-8", "replace")

    events = truncate_records(read_bronze(BRONZE_ROOT))
    assert {event["table_name"] for event in events} == {"payments", "customers", "merchants"}
    assert len({event["lsn"] for event in events}) == 1
    for event in events:
        assert event["key"] is None
        assert event["before"] is None
        assert event["after"] is None
        assert event["truncate_cascade"] is False
        assert event["truncate_restart_identity"] is True


def test_truncate_is_decoded_without_a_prior_change_to_the_table(
    source, run_consumer, read_bronze
):
    with source.cursor() as cursor:
        cursor.execute("TRUNCATE payments")

    result = run_consumer(SLOT, BRONZE_ROOT)
    assert result.returncode == 0, result.stderr.decode("utf-8", "replace")

    events = truncate_records(read_bronze(BRONZE_ROOT))
    assert [event["table_name"] for event in events] == ["payments"]
    assert events[0]["truncate_cascade"] is False
    assert events[0]["truncate_restart_identity"] is False
