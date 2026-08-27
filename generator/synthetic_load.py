import os
import random
import sys
import time

import psycopg2

from ingestion.env import load_env_file

STATUSES = ("authorized", "captured", "refunded", "failed")
CURRENCIES = ("BRL", "USD", "EUR")
CATEGORIES = ("retail", "marketplace", "services", "travel")


def connection_params() -> dict:
    return dict(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "5434"),
        dbname=os.environ.get("POSTGRES_DB", "payments"),
        user=os.environ.get("POSTGRES_USER", "payments"),
        password=os.environ.get("POSTGRES_PASSWORD", "payments"),
    )


def ensure_dimensions(connection, customer_target: int, merchant_target: int) -> tuple:
    with connection.cursor() as cursor:
        cursor.execute("SELECT count(*) FROM customers")
        for index in range(cursor.fetchone()[0], customer_target):
            cursor.execute(
                "INSERT INTO customers (full_name, email, cpf) VALUES (%s, %s, %s)",
                (
                    "Customer {:04d}".format(index),
                    "customer{:04d}@example.invalid".format(index),
                    "{:011d}".format(index),
                ),
            )
        cursor.execute("SELECT count(*) FROM merchants")
        for index in range(cursor.fetchone()[0], merchant_target):
            cursor.execute(
                "INSERT INTO merchants (legal_name, category, country) VALUES (%s, %s, %s)",
                (
                    "Merchant {:04d} LTDA".format(index),
                    CATEGORIES[index % len(CATEGORIES)],
                    "BR",
                ),
            )
        connection.commit()
        cursor.execute("SELECT customer_id FROM customers ORDER BY customer_id")
        customers = [row[0] for row in cursor.fetchall()]
        cursor.execute("SELECT merchant_id FROM merchants ORDER BY merchant_id")
        merchants = [row[0] for row in cursor.fetchall()]
    return customers, merchants


def run(connection, customers, merchants, duration: float, rate: float, rng, lateness: int = 0) -> tuple:
    """A duration of zero runs until the process is stopped.

    That is how it runs as a service. The heartbeat alert compares against a
    baseline, and a generator that only runs when somebody types make seed makes
    that baseline zero almost always: the alert would fire permanently, which is
    as useless as never firing.
    """
    interval = 1.0 / rate if rate > 0 else 0.0
    deadline = None if duration <= 0 else time.monotonic() + duration
    recent = []
    inserted = 0
    updated = 0
    while deadline is None or time.monotonic() < deadline:
        with connection.cursor() as cursor:
            cursor.execute(
                "INSERT INTO payments (customer_id, merchant_id, amount, currency, "
                "status, created_at) VALUES (%s, %s, %s, %s, 'pending', "
                "now() - make_interval(hours => %s)) RETURNING payment_id",
                (
                    rng.choice(customers),
                    rng.choice(merchants),
                    round(rng.uniform(5.0, 5000.0), 2),
                    rng.choice(CURRENCIES),
                    lateness,
                ),
            )
            recent.append(cursor.fetchone()[0])
            inserted += 1
            if len(recent) > 50:
                recent.pop(0)
            if rng.random() < 0.4:
                cursor.execute(
                    "UPDATE payments SET status = %s, updated_at = now() WHERE payment_id = %s",
                    (rng.choice(STATUSES), rng.choice(recent)),
                )
                updated += 1
            if rng.random() < 0.1:
                cursor.execute(
                    "UPDATE customers SET full_name = full_name || %s, updated_at = now() "
                    "WHERE customer_id = %s",
                    (".", rng.choice(customers)),
                )
                updated += 1
        connection.commit()
        time.sleep(interval)
    return inserted, updated


def main() -> int:
    load_env_file()
    rng = random.Random(int(os.environ.get("GEN_SEED", "42")))
    duration = float(os.environ.get("GEN_DURATION_SECONDS", "10"))
    rate = float(os.environ.get("GEN_RATE_PER_SECOND", "20"))
    connection = psycopg2.connect(**connection_params())
    try:
        customers, merchants = ensure_dimensions(
            connection,
            int(os.environ.get("GEN_CUSTOMERS", "20")),
            int(os.environ.get("GEN_MERCHANTS", "5")),
        )
        inserted, updated = run(
            connection, customers, merchants, duration, rate, rng,
            int(os.environ.get("GEN_LATENESS_HOURS", "0")),
        )
    finally:
        connection.close()
    print(
        "inserted {} payments, applied {} updates".format(inserted, updated),
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
