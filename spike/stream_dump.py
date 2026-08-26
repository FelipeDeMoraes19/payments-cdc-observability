import os
import select
import sys
from contextlib import closing
from datetime import datetime, timedelta

import psycopg2
from psycopg2.extras import LogicalReplicationConnection

SLOT = os.environ.get("CDC_SLOT", "spike_stream")
PUBLICATION = os.environ.get("CDC_PUBLICATION", "payments_pub")
IDLE_TIMEOUT_SECONDS = float(os.environ.get("CDC_IDLE_TIMEOUT", "5"))
MAX_MESSAGES = int(os.environ.get("CDC_MAX_MESSAGES", "100"))


def connection_params():
    return dict(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "5434"),
        dbname=os.environ.get("POSTGRES_DB", "payments"),
        user=os.environ.get("POSTGRES_USER", "payments"),
        password=os.environ.get("POSTGRES_PASSWORD", "payments"),
    )


def format_lsn(value):
    return "{:X}/{:X}".format(value >> 32, value & 0xFFFFFFFF)


def slot_position():
    with closing(psycopg2.connect(**connection_params())) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT restart_lsn::text, confirmed_flush_lsn::text "
                "FROM pg_replication_slots WHERE slot_name = %s",
                (SLOT,),
            )
            row = cur.fetchone()
    if row is None:
        return "absent"
    return "restart={} confirmed_flush={}".format(row[0], row[1])


def ensure_slot():
    with closing(psycopg2.connect(**connection_params())) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM pg_replication_slots WHERE slot_name = %s", (SLOT,)
            )
            if cur.fetchone() is None:
                cur.execute(
                    "SELECT pg_create_logical_replication_slot(%s, 'pgoutput')", (SLOT,)
                )
                return "created"
            return "reused"


def generate_changes():
    with closing(psycopg2.connect(**connection_params())) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO payments (customer_id, merchant_id, amount, currency, status) "
                "VALUES (1, 1, 42.00, 'BRL', 'pending') RETURNING payment_id"
            )
            payment_id = cur.fetchone()[0]
            cur.execute(
                "UPDATE payments SET status = 'authorized', updated_at = now() "
                "WHERE payment_id = %s",
                (payment_id,),
            )
            cur.execute(
                "UPDATE payments SET status = 'captured', updated_at = now() "
                "WHERE payment_id = %s",
                (payment_id,),
            )
        conn.commit()
    return payment_id


def stream():
    conn = psycopg2.connect(
        connection_factory=LogicalReplicationConnection, **connection_params()
    )
    cur = conn.cursor()
    cur.start_replication(
        slot_name=SLOT,
        decode=False,
        options={"proto_version": "1", "publication_names": PUBLICATION},
    )
    seen = []
    deadline = datetime.now() + timedelta(seconds=IDLE_TIMEOUT_SECONDS)
    while len(seen) < MAX_MESSAGES and datetime.now() < deadline:
        message = cur.read_message()
        if message is None:
            select.select([cur], [], [], 1.0)
            continue
        deadline = datetime.now() + timedelta(seconds=IDLE_TIMEOUT_SECONDS)
        kind = chr(message.payload[0])
        seen.append((kind, message.data_start))
        print(
            "{:>3}  data_start={:<12} wal_end={:<12} type={}  bytes={}".format(
                len(seen),
                format_lsn(message.data_start),
                format_lsn(message.wal_end),
                kind,
                len(message.payload),
            )
        )
        print("     " + message.payload[:32].hex())
    cur.close()
    conn.close()
    return seen


def main():
    print("slot: {} ({})".format(SLOT, ensure_slot()))
    print("position before: " + slot_position())
    print("generated changes for payment_id " + str(generate_changes()))
    print("--- stream ---")
    seen = stream()
    print("--- summary ---")
    print("messages received: " + str(len(seen)))
    updates = [lsn for kind, lsn in seen if kind == "U"]
    print(
        "update messages: {}  distinct data_start: {}".format(
            len(updates), len(set(updates))
        )
    )
    print("position after: " + slot_position())
    return 0 if seen else 1


if __name__ == "__main__":
    sys.exit(main())
