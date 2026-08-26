import re
import shutil
from pathlib import Path

import pytest

from contracts.postgres_types import TYPE_OIDS

BRONZE_ROOT = Path(__file__).resolve().parents[1] / "data" / "test-bronze" / "contract"
SLOT = "payments_cdc_contract_test"

RESTORE_AMOUNT = (
    "ALTER TABLE payments ALTER COLUMN amount TYPE numeric(14,2) "
    "USING amount::numeric(14,2)"
)


@pytest.fixture
def source(postgres, reset_source, drop_slot):
    with postgres.cursor() as cursor:
        cursor.execute(RESTORE_AMOUNT)
    reset_source(SLOT)
    shutil.rmtree(BRONZE_ROOT, ignore_errors=True)
    yield postgres
    with postgres.cursor() as cursor:
        cursor.execute(RESTORE_AMOUNT)
    drop_slot(SLOT)


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


def test_column_type_change_fails_loudly_and_writes_nothing_new(
    source, run_consumer, read_bronze
):
    seed_payment(source, "199.90")
    healthy = run_consumer(SLOT, BRONZE_ROOT)
    assert healthy.returncode == 0, healthy.stderr.decode("utf-8", "replace")
    before = read_bronze(BRONZE_ROOT)
    assert before, "nothing was captured before the schema changed"

    with source.cursor() as cursor:
        cursor.execute("ALTER TABLE payments ALTER COLUMN amount TYPE text")
    seed_payment(source, "424.20")

    broken = run_consumer(SLOT, BRONZE_ROOT)
    message = broken.stderr.decode("utf-8", "replace")

    assert broken.returncode != 0, "the consumer accepted a changed column type"
    assert "contract violation" in message
    assert "public.payments" in message, "the message does not name the table"
    assert "payments.amount" in message, "the message does not name the column"
    assert "numeric" in message, "the message does not name the expected type"
    assert "text" in message, "the message does not name the observed type"
    assert re.search(r"LSN [0-9A-F]+/[0-9A-F]+", message), "the message does not name the LSN"

    assert read_bronze(BRONZE_ROOT) == before, "a record of the new shape reached bronze"
