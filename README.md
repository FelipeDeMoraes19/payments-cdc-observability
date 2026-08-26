# payments-cdc-observability

A data platform that fits in a `docker compose up` and hides none of the boring parts.

> **Status: work in progress.** Milestone 1 of 3 (ingestion and schema contracts) is under
> construction. This README grows with the project; sections marked *planned* are not built
> yet.

Most portfolio data projects prove the happy path: call an API, load, transform, show a
dashboard. This one is about the parts that production demands and portfolios skip —
schema contracts, lineage, PII masking, idempotent backfill, observability, and failure
modes you can trigger with one command.

The source is a local Postgres OLTP read through **logical replication**, decoded by a
CDC consumer written for this repository — no Debezium, no Airbyte. Change data capture
is the risky part, so it was built first.

## Failure modes

*Planned — the table lands with Milestone 3.* Every row will name the command that injects
the failure, what should catch it, and whether it actually does. Including the row that
says "no, and here is exactly why".

## Architecture

*Planned — diagram lands with Milestone 2.*

## Requirements

- Docker Desktop, running
- Python 3.11 or newer

## Quick start

```
cp .env.example .env
docker compose up -d
pip install -r requirements.txt
python -m ingestion.cdc.consumer
```

The consumer writes one JSON object per change into
`data/bronze/cdc/<table>/dt=<commit date>/part-<first LSN>-<last LSN>.jsonl`, and confirms
its position to Postgres only after the file is on disk. Set `CDC_IDLE_TIMEOUT` to a
number of seconds to make it exit once the stream goes quiet.

Killing the consumer and starting it again loses nothing: it resumes from the last
position it confirmed. Anything written but not yet confirmed arrives a second time, with
the same `(key, lsn)` and the same content, which is what the silver layer deduplicates
on.

`.env` holds local-only credentials for the throwaway container and is not committed.
`.env.example` carries the same values and is — there is no secret here to protect.

Postgres is published on **port 5434**, not 5432, to avoid colliding with an existing
local instance.

### Schema contracts

Every published table is declared in `contracts/`: its columns, their Postgres types, and
a Pydantic model for the values. The declaration is checked against the `Relation` message
that `pgoutput` sends ahead of any row, so a type change in the source is caught before a
single record of the new shape is written:

```
contract violation: column public.payments.amount changed type at LSN 0/1AD3C68:
the contract expects numeric (oid 1700), the stream carries text (oid 25)
```

The consumer exits with status 2 and confirms nothing, so the changes still in flight are
replayed once the contract and the source agree again.

A table that is published without a contract is also a violation. Publishing without
contracting is how data nobody can explain gets into a warehouse.

### Changing the database schema

The scripts in `db/init/` run **only when the Postgres volume is empty**. Editing them and
restarting the container does nothing. To apply schema changes, destroy the volume:

```
make reset
```

This drops the database, its data, and every replication slot. There is no in-place
migration path by design: the OLTP here is a synthetic fixture, not something to preserve.

## Decision records

Short documents in `docs/adr/`, written before the decision, in Portuguese. The field that
matters most is *rejected alternatives and why*.

| ADR | Decision |
|---|---|
| 0001 | Own CDC consumer reading `pgoutput`, instead of Debezium, Airbyte or `wal2json` |
| 0002 | Deduplication keyed on per-message LSN, not commit LSN and not `updated_at` |
| 0008 | State lives in the replication slot and in the data, never in a side file |
| 0009 | Dedicated replication role, and where its password lives |
| 0010 | Where the durability boundary sits, and how batches are flushed |
| 0011 | The contract is checked on the `Relation` message, and what it deliberately skips |
| 0012 | `TRUNCATE` is recorded as an event, and what that does not yet solve |

## Scope

No cloud, no Kafka, no real streaming, no custom front end, no machine learning, no
authentication, no performance tuning. The volume is small on purpose. The point is
completeness of practice in miniature, not scale.

All data is synthetic or from public sources.
