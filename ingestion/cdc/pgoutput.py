import struct
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple

POSTGRES_EPOCH = datetime(2000, 1, 1, tzinfo=timezone.utc)

REPLICA_IDENTITY_NAMES = {
    "d": "default",
    "n": "nothing",
    "f": "full",
    "i": "index",
}

SUPPORTED_MESSAGE_TYPES = "B, C, R, I, U, D"


class ProtocolError(Exception):
    pass


def format_lsn(value: int) -> str:
    return "{:X}/{:X}".format(value >> 32, value & 0xFFFFFFFF)


def to_datetime(microseconds: int) -> datetime:
    return POSTGRES_EPOCH + timedelta(microseconds=microseconds)


@dataclass(frozen=True)
class Column:
    name: str
    type_oid: int
    type_modifier: int
    is_key: bool


@dataclass(frozen=True)
class Relation:
    oid: int
    namespace: str
    name: str
    replica_identity: str
    columns: Tuple[Column, ...]

    @property
    def qualified_name(self) -> str:
        return "{}.{}".format(self.namespace, self.name)

    @property
    def key_columns(self) -> Tuple[str, ...]:
        return tuple(column.name for column in self.columns if column.is_key)


@dataclass(frozen=True)
class Begin:
    final_lsn: int
    commit_time: datetime
    xid: int


@dataclass(frozen=True)
class Commit:
    commit_lsn: int
    end_lsn: int
    commit_time: datetime


@dataclass(frozen=True)
class Change:
    action: str
    relation: Relation
    new_values: Optional[dict]
    old_values: Optional[dict]
    unchanged_columns: Tuple[str, ...]


class _Reader:
    def __init__(self, data: bytes) -> None:
        self._data = data
        self._offset = 0

    def uint8(self) -> int:
        return self._unpack(">B", 1)

    def int16(self) -> int:
        return self._unpack(">h", 2)

    def int32(self) -> int:
        return self._unpack(">i", 4)

    def uint32(self) -> int:
        return self._unpack(">I", 4)

    def int64(self) -> int:
        return self._unpack(">q", 8)

    def char(self) -> str:
        return chr(self.uint8())

    def string(self) -> str:
        end = self._data.find(b"\x00", self._offset)
        if end < 0:
            raise ProtocolError(
                "unterminated string starting at offset {}".format(self._offset)
            )
        value = self._data[self._offset : end].decode("utf-8")
        self._offset = end + 1
        return value

    def raw(self, size: int) -> bytes:
        self._require(size)
        value = self._data[self._offset : self._offset + size]
        self._offset += size
        return value

    def _unpack(self, fmt: str, size: int) -> int:
        self._require(size)
        value = struct.unpack_from(fmt, self._data, self._offset)[0]
        self._offset += size
        return value

    def _require(self, size: int) -> None:
        if self._offset + size > len(self._data):
            raise ProtocolError(
                "message truncated: {} bytes needed at offset {}, message has {}".format(
                    size, self._offset, len(self._data)
                )
            )


class Decoder:
    def __init__(self) -> None:
        self._relations = {}

    def relation_for(self, oid: int) -> Relation:
        relation = self._relations.get(oid)
        if relation is None:
            raise ProtocolError(
                "change refers to relation oid {} but no Relation message was received "
                "for it in this session".format(oid)
            )
        return relation

    def decode(self, payload: bytes):
        reader = _Reader(payload)
        tag = reader.char()
        if tag == "B":
            return self._begin(reader)
        if tag == "C":
            return self._commit(reader)
        if tag == "R":
            return self._relation(reader)
        if tag == "I":
            return self._insert(reader)
        if tag == "U":
            return self._update(reader)
        if tag == "D":
            return self._delete(reader)
        raise ProtocolError(
            "unsupported pgoutput message type {!r}; this consumer handles {}".format(
                tag, SUPPORTED_MESSAGE_TYPES
            )
        )

    def _begin(self, reader: _Reader) -> Begin:
        final_lsn = reader.int64()
        commit_time = to_datetime(reader.int64())
        xid = reader.uint32()
        return Begin(final_lsn, commit_time, xid)

    def _commit(self, reader: _Reader) -> Commit:
        flags = reader.uint8()
        if flags != 0:
            raise ProtocolError("commit message carries unexpected flags {}".format(flags))
        commit_lsn = reader.int64()
        end_lsn = reader.int64()
        commit_time = to_datetime(reader.int64())
        return Commit(commit_lsn, end_lsn, commit_time)

    def _relation(self, reader: _Reader) -> Relation:
        oid = reader.uint32()
        namespace = reader.string()
        name = reader.string()
        replica_identity = reader.char()
        column_count = reader.int16()
        columns = []
        for _ in range(column_count):
            flags = reader.uint8()
            column_name = reader.string()
            type_oid = reader.uint32()
            type_modifier = reader.int32()
            columns.append(Column(column_name, type_oid, type_modifier, bool(flags & 1)))
        relation = Relation(
            oid,
            namespace,
            name,
            REPLICA_IDENTITY_NAMES.get(replica_identity, replica_identity),
            tuple(columns),
        )
        self._relations[oid] = relation
        return relation

    def _insert(self, reader: _Reader) -> Change:
        relation = self.relation_for(reader.uint32())
        tag = reader.char()
        if tag != "N":
            raise ProtocolError(
                "insert on {} expected a new tuple marked 'N', got {!r}".format(
                    relation.qualified_name, tag
                )
            )
        new_values, unchanged = _read_tuple(reader, relation)
        return Change("insert", relation, new_values, None, unchanged)

    def _update(self, reader: _Reader) -> Change:
        relation = self.relation_for(reader.uint32())
        old_values = None
        tag = reader.char()
        if tag in ("K", "O"):
            old_values, _ = _read_tuple(reader, relation)
            tag = reader.char()
        if tag != "N":
            raise ProtocolError(
                "update on {} expected a new tuple marked 'N', got {!r}".format(
                    relation.qualified_name, tag
                )
            )
        new_values, unchanged = _read_tuple(reader, relation)
        return Change("update", relation, new_values, old_values, unchanged)

    def _delete(self, reader: _Reader) -> Change:
        relation = self.relation_for(reader.uint32())
        tag = reader.char()
        if tag not in ("K", "O"):
            raise ProtocolError(
                "delete on {} expected an old tuple marked 'K' or 'O', got {!r}; "
                "REPLICA IDENTITY may be set to NOTHING".format(
                    relation.qualified_name, tag
                )
            )
        old_values, _ = _read_tuple(reader, relation)
        return Change("delete", relation, None, old_values, ())


def _read_tuple(reader: _Reader, relation: Relation):
    column_count = reader.int16()
    if column_count != len(relation.columns):
        raise ProtocolError(
            "tuple for {} carries {} columns but the relation declares {}".format(
                relation.qualified_name, column_count, len(relation.columns)
            )
        )
    values = {}
    unchanged = []
    for column in relation.columns:
        kind = reader.char()
        if kind == "n":
            values[column.name] = None
        elif kind == "u":
            unchanged.append(column.name)
        elif kind == "t":
            values[column.name] = reader.raw(reader.int32()).decode("utf-8")
        elif kind == "b":
            raise ProtocolError(
                "column {}.{} arrived in binary format; this consumer requests text".format(
                    relation.qualified_name, column.name
                )
            )
        else:
            raise ProtocolError(
                "unknown tuple data kind {!r} for column {}.{}".format(
                    kind, relation.qualified_name, column.name
                )
            )
    return values, tuple(unchanged)
