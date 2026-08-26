import json
import os
import select
import sys
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg2
from psycopg2.extras import LogicalReplicationConnection

from ingestion.cdc.pgoutput import (
    Begin,
    Commit,
    Decoder,
    ProtocolError,
    Relation,
    format_lsn,
)
from ingestion.env import load_env_file, required


def connection_params() -> dict:
    return dict(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "5434"),
        dbname=os.environ.get("POSTGRES_DB", "payments"),
        user=required("CDC_USER"),
        password=required("CDC_PASSWORD"),
    )


def slot_name() -> str:
    return os.environ.get("CDC_SLOT", "payments_cdc")


def publication_name() -> str:
    return os.environ.get("CDC_PUBLICATION", "payments_pub")


def idle_timeout_seconds() -> float:
    return float(os.environ.get("CDC_IDLE_TIMEOUT", "0"))


class JsonlSink:
    def __init__(self, path=None) -> None:
        if path:
            target = Path(path)
            target.parent.mkdir(parents=True, exist_ok=True)
            self._stream = target.open("a", encoding="utf-8", newline="\n")
            self._owns_stream = True
        else:
            self._stream = sys.stdout
            self._owns_stream = False

    def write(self, record: dict) -> None:
        self._stream.write(json.dumps(record) + "\n")
        self._stream.flush()

    def close(self) -> None:
        if self._owns_stream:
            self._stream.close()


def ensure_slot() -> str:
    with closing(psycopg2.connect(**connection_params())) as connection:
        connection.autocommit = True
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT 1 FROM pg_replication_slots WHERE slot_name = %s", (slot_name(),)
            )
            if cursor.fetchone() is not None:
                return "reused"
            cursor.execute(
                "SELECT pg_create_logical_replication_slot(%s, 'pgoutput')",
                (slot_name(),),
            )
            return "created"


def build_record(change, lsn: int, transaction: Begin) -> dict:
    source_values = change.old_values if change.action == "delete" else change.new_values
    key = None
    if source_values is not None and change.relation.key_columns:
        missing = [
            name for name in change.relation.key_columns if name not in source_values
        ]
        if missing:
            raise ProtocolError(
                "key columns {} are absent from the {} tuple of {} at LSN {}; "
                "the deduplication key cannot be built".format(
                    missing, change.action, change.relation.qualified_name, format_lsn(lsn)
                )
            )
        key = {name: source_values[name] for name in change.relation.key_columns}
    record = {
        "source": "postgres",
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "lsn": format_lsn(lsn),
        "xid": transaction.xid,
        "commit_time": transaction.commit_time.isoformat(),
        "action": change.action,
        "schema": change.relation.namespace,
        "table": change.relation.name,
        "key": key,
        "before": change.old_values,
        "after": change.new_values,
    }
    if change.unchanged_columns:
        record["unchanged_toast_columns"] = list(change.unchanged_columns)
    return record


def _next_deadline(idle_timeout: float):
    if idle_timeout <= 0:
        return None
    return datetime.now(timezone.utc) + timedelta(seconds=idle_timeout)


def run(sink: JsonlSink) -> int:
    decoder = Decoder()
    connection = psycopg2.connect(
        connection_factory=LogicalReplicationConnection, **connection_params()
    )
    cursor = connection.cursor()
    cursor.start_replication(
        slot_name=slot_name(),
        decode=False,
        options={"proto_version": "1", "publication_names": publication_name()},
    )
    idle_timeout = idle_timeout_seconds()
    deadline = _next_deadline(idle_timeout)
    transaction = None
    emitted = 0
    try:
        while True:
            message = cursor.read_message()
            if message is None:
                if deadline is not None and datetime.now(timezone.utc) >= deadline:
                    break
                select.select([cursor], [], [], 1.0)
                continue
            deadline = _next_deadline(idle_timeout)
            decoded = decoder.decode(message.payload)
            if isinstance(decoded, Begin):
                transaction = decoded
            elif isinstance(decoded, Commit):
                transaction = None
            elif isinstance(decoded, Relation):
                continue
            else:
                if transaction is None:
                    raise ProtocolError(
                        "change on {} arrived outside a transaction; the stream is out "
                        "of sync".format(decoded.relation.qualified_name)
                    )
                sink.write(build_record(decoded, message.data_start, transaction))
                emitted += 1
    finally:
        cursor.close()
        connection.close()
    return emitted


def main() -> int:
    load_env_file()
    print("slot {} ({})".format(slot_name(), ensure_slot()), file=sys.stderr)
    sink = JsonlSink(os.environ.get("CDC_OUTPUT"))
    try:
        emitted = run(sink)
    finally:
        sink.close()
    print("records emitted: {}".format(emitted), file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
