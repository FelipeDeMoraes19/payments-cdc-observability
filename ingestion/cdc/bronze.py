import os
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


@dataclass(frozen=True)
class PendingRecord:
    lsn: int
    table: str
    partition: str
    payload: dict

SCHEMA = pa.schema(
    [
        pa.field("source", pa.string(), nullable=False),
        pa.field("ingested_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("lsn", pa.string(), nullable=False),
        pa.field("lsn_numeric", pa.int64(), nullable=False),
        pa.field("xid", pa.int64(), nullable=False),
        pa.field("commit_time", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("action", pa.string(), nullable=False),
        pa.field("schema_name", pa.string(), nullable=False),
        pa.field("table_name", pa.string(), nullable=False),
        pa.field("key", pa.map_(pa.string(), pa.string())),
        pa.field("before", pa.map_(pa.string(), pa.string())),
        pa.field("after", pa.map_(pa.string(), pa.string())),
        pa.field("unchanged_toast_columns", pa.list_(pa.string())),
        pa.field("truncate_cascade", pa.bool_()),
        pa.field("truncate_restart_identity", pa.bool_()),
    ]
)

COMPRESSION = "zstd"
SUFFIX = ".parquet"
STAGING_SUFFIX = SUFFIX + ".tmp"


class BronzeWriter:
    def __init__(self, root: Path) -> None:
        self._root = root

    def discard_stale_staging(self) -> int:
        if not self._root.exists():
            return 0
        stale = list(self._root.rglob("*" + STAGING_SUFFIX))
        for path in stale:
            path.unlink()
        return len(stale)

    def write(self, pending) -> list:
        groups = {}
        for record in pending:
            groups.setdefault((record.table, record.partition), []).append(record)
        return [
            self._write_group(table, partition, groups[(table, partition)])
            for table, partition in sorted(groups)
        ]

    def _write_group(self, table: str, partition: str, records: list) -> Path:
        directory = self._root / table / "dt={}".format(partition)
        directory.mkdir(parents=True, exist_ok=True)
        lsns = [record.lsn for record in records]
        name = "part-{:016X}-{:016X}".format(min(lsns), max(lsns))
        target = directory / (name + SUFFIX)
        staging = directory / (name + STAGING_SUFFIX)
        arrow_table = pa.Table.from_pylist(
            [record.payload for record in records], schema=SCHEMA
        )
        with staging.open("wb") as stream:
            pq.write_table(arrow_table, stream, compression=COMPRESSION)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(staging, target)
        return target


def read(root) -> list:
    records = []
    for path in sorted(Path(root).rglob("*" + SUFFIX)):
        for row in pq.read_table(path).to_pylist():
            records.append(_with_dicts(row))
    return records


def _with_dicts(row: dict) -> dict:
    restored = dict(row)
    for name in ("key", "before", "after"):
        if restored[name] is not None:
            restored[name] = dict(restored[name])
    return restored
