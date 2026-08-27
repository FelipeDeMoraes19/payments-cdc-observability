import os
import select
import sys
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg2
from opentelemetry.metrics import Observation
from psycopg2.extras import LogicalReplicationConnection

from contracts.tables import contract_for
from contracts.validation import ContractViolation, validate_change, validate_relation
from ingestion.cdc.bronze import BronzeWriter, PendingRecord
from ingestion.cdc.pgoutput import (
    Begin,
    Commit,
    Decoder,
    ProtocolError,
    Relation,
    Truncate,
    format_lsn,
)
from ingestion.env import load_env_file, required
from observability.otel.metrics import serve


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


def bronze_root() -> Path:
    return Path(os.environ.get("CDC_BRONZE_ROOT", "data/bronze/cdc"))


def idle_timeout_seconds() -> float:
    return float(os.environ.get("CDC_IDLE_TIMEOUT", "0"))


def batch_max_records() -> int:
    return int(os.environ.get("CDC_BATCH_MAX_RECORDS", "500"))


def batch_max_seconds() -> float:
    return float(os.environ.get("CDC_BATCH_MAX_SECONDS", "5"))


def metrics_enabled() -> bool:
    return os.environ.get("CDC_METRICS", "") not in ("", "0")


def fail_before_feedback() -> bool:
    return os.environ.get("CDC_FAIL_BEFORE_FEEDBACK", "") not in ("", "0")


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
    return {
        "source": "postgres",
        "ingested_at": datetime.now(timezone.utc),
        "lsn": format_lsn(lsn),
        "lsn_numeric": lsn,
        "xid": transaction.xid,
        "commit_time": transaction.commit_time,
        "action": change.action,
        "schema_name": change.relation.namespace,
        "table_name": change.relation.name,
        "key": key,
        "before": change.old_values,
        "after": change.new_values,
        "unchanged_toast_columns": list(change.unchanged_columns),
        "truncate_cascade": None,
        "truncate_restart_identity": None,
    }


def build_truncate_record(relation, truncate: Truncate, lsn: int, transaction: Begin) -> dict:
    return {
        "source": "postgres",
        "ingested_at": datetime.now(timezone.utc),
        "lsn": format_lsn(lsn),
        "lsn_numeric": lsn,
        "xid": transaction.xid,
        "commit_time": transaction.commit_time,
        "action": "truncate",
        "schema_name": relation.namespace,
        "table_name": relation.name,
        "key": None,
        "before": None,
        "after": None,
        "unchanged_toast_columns": None,
        "truncate_cascade": truncate.cascade,
        "truncate_restart_identity": truncate.restart_identity,
    }


def _deadline(seconds: float):
    if seconds <= 0:
        return None
    return datetime.now(timezone.utc) + timedelta(seconds=seconds)


def _batch_is_full(pending: list, opened_at: datetime) -> bool:
    if len(pending) >= batch_max_records():
        return True
    elapsed = (datetime.now(timezone.utc) - opened_at).total_seconds()
    return elapsed >= batch_max_seconds()


class Telemetry:
    def __init__(self, meter=None) -> None:
        self._confirmed_lsn = 0
        if meter is None:
            self._written = None
            return
        self._written = meter.create_counter(
            "cdc_records_written",
            unit="1",
            description="Change events durably written to bronze",
        )
        meter.create_observable_gauge(
            "cdc_confirmed_lsn",
            callbacks=[lambda options: [Observation(self._confirmed_lsn)]],
            unit="1",
            description="Highest LSN confirmed to the replication slot",
        )

    def record(self, pending: list, confirm_lsn: int) -> None:
        self._confirmed_lsn = confirm_lsn
        if self._written is None:
            return
        for record in pending:
            self._written.add(1, {"table": record.table})


def _flush(writer: BronzeWriter, cursor, pending: list, confirm_lsn: int, telemetry) -> int:
    paths = writer.write(pending)
    if fail_before_feedback():
        print(
            "fault injection: dying after writing {} records, before confirming {}".format(
                len(pending), format_lsn(confirm_lsn)
            ),
            file=sys.stderr,
        )
        sys.stderr.flush()
        os._exit(17)
    cursor.send_feedback(flush_lsn=confirm_lsn, force=True)
    telemetry.record(pending, confirm_lsn)
    print(
        "flushed {} records into {} file(s), confirmed {}".format(
            len(pending), len(paths), format_lsn(confirm_lsn)
        ),
        file=sys.stderr,
    )
    return len(pending)


def run(writer: BronzeWriter, telemetry: Telemetry) -> int:
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
    exit_deadline = _deadline(idle_timeout_seconds())
    pending = []
    opened_at = None
    transaction = None
    confirm_lsn = None
    written = 0
    try:
        while True:
            message = cursor.read_message()
            if message is None:
                if pending and transaction is None:
                    written += _flush(writer, cursor, pending, confirm_lsn, telemetry)
                    pending = []
                    opened_at = None
                if exit_deadline is not None and datetime.now(timezone.utc) >= exit_deadline:
                    break
                select.select([cursor], [], [], 1.0)
                continue
            exit_deadline = _deadline(idle_timeout_seconds())
            decoded = decoder.decode(message.payload)
            if isinstance(decoded, Begin):
                transaction = decoded
            elif isinstance(decoded, Relation):
                validate_relation(
                    decoded,
                    contract_for(decoded.namespace, decoded.name),
                    format_lsn(transaction.final_lsn) if transaction else None,
                )
            elif isinstance(decoded, Commit):
                transaction = None
                confirm_lsn = message.wal_end
                if pending and _batch_is_full(pending, opened_at):
                    written += _flush(writer, cursor, pending, confirm_lsn, telemetry)
                    pending = []
                    opened_at = None
            elif isinstance(decoded, Truncate):
                if transaction is None:
                    raise ProtocolError(
                        "truncate arrived outside a transaction; the stream is out of sync"
                    )
                if not pending:
                    opened_at = datetime.now(timezone.utc)
                for relation in decoded.relations:
                    validate_relation(
                        relation,
                        contract_for(relation.namespace, relation.name),
                        format_lsn(message.data_start),
                    )
                    pending.append(
                        PendingRecord(
                            lsn=message.data_start,
                            table=relation.name,
                            partition=transaction.commit_time.date().isoformat(),
                            payload=build_truncate_record(
                                relation, decoded, message.data_start, transaction
                            ),
                        )
                    )
            else:
                if transaction is None:
                    raise ProtocolError(
                        "change on {} arrived outside a transaction; the stream is out "
                        "of sync".format(decoded.relation.qualified_name)
                    )
                validate_change(
                    decoded,
                    contract_for(decoded.relation.namespace, decoded.relation.name),
                    format_lsn(message.data_start),
                )
                if not pending:
                    opened_at = datetime.now(timezone.utc)
                pending.append(
                    PendingRecord(
                        lsn=message.data_start,
                        table=decoded.relation.name,
                        partition=transaction.commit_time.date().isoformat(),
                        payload=build_record(decoded, message.data_start, transaction),
                    )
                )
    finally:
        cursor.close()
        connection.close()
    return written


def main() -> int:
    load_env_file()
    writer = BronzeWriter(bronze_root())
    discarded = writer.discard_stale_staging()
    if discarded:
        print("discarded {} stale staging file(s)".format(discarded), file=sys.stderr)
    print("slot {} ({})".format(slot_name(), ensure_slot()), file=sys.stderr)
    telemetry = Telemetry(serve("cdc-consumer") if metrics_enabled() else None)
    try:
        written = run(writer, telemetry)
    except ContractViolation as violation:
        print("contract violation: {}".format(violation), file=sys.stderr)
        return 2
    print("records written: {}".format(written), file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
