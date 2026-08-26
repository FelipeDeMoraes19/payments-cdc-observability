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

`make up`, `make seed`, `make cdc` and `make test` wrap the same commands. Running the
tests needs `pip install -r requirements-dev.txt` as well.

The consumer writes one row per change into
`data/bronze/cdc/<table>/dt=<commit date>/part-<first LSN>-<last LSN>.parquet`, and
confirms its position to Postgres only after the file is on disk. Set `CDC_IDLE_TIMEOUT`
to a number of seconds to make it exit once the stream goes quiet.

Killing the consumer and starting it again loses nothing: it resumes from the last
position it confirmed. Anything written but not yet confirmed arrives a second time, with
the same `(key, lsn)` and the same content, which is what the silver layer deduplicates
on.

`.env` holds local-only credentials for the throwaway container and is not committed.
`.env.example` carries the same values and is — there is no secret here to protect.

Postgres is published on **port 5434**, not 5432, to avoid colliding with an existing
local instance.

## Querying the bronze

Values are kept exactly as Postgres sent them, as text inside a `MAP`, because deciding
that a string is a `numeric` is the next layer's job. The layer is still queryable
directly, with nothing loaded anywhere:

```sql
SELECT table_name, action, count(*) AS events
FROM read_parquet('data/bronze/cdc/**/*.parquet')
GROUP BY table_name, action ORDER BY table_name, action;
```

```
customers | update | 27
payments  | insert | 191
payments  | update | 73
```

Change data capture only gives a stream of changes, so the current state of a row is the
version with the highest LSN. That is one query:

```sql
SELECT key['payment_id'] AS payment_id,
       after['status']   AS status,
       after['amount']   AS amount,
       lsn
FROM read_parquet('data/bronze/cdc/payments/**/*.parquet')
WHERE action <> 'truncate'
QUALIFY row_number() OVER (PARTITION BY key['payment_id'] ORDER BY lsn_numeric DESC) = 1
ORDER BY payment_id::BIGINT LIMIT 5;
```

```
198 | failed     | 3709.04 | 0/1B08670
199 | pending    | 473.01  | 0/1B07FD8
200 | refunded   | 1106.10 | 0/1B08E58
201 | refunded   | 781.62  | 0/1B0C9C0
202 | authorized | 1326.28 | 0/1B0D458
```

Note the ordering column. The LSN is stored twice: `lsn` as the text Postgres prints, and
`lsn_numeric` as an integer. Sorting the text is wrong — `'0/9FFFFFF'` sorts after
`'0/10000000'` as a string and before it as a number — and picking the wrong version of a
row is the kind of bug that only shows up after a few hundred megabytes of WAL. ADR 0013
has the measurement.

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
| 0013 | Bronze in Parquet, the tuple as a `MAP`, and the LSN stored twice |

## Scope

No cloud, no Kafka, no real streaming, no custom front end, no machine learning, no
authentication, no performance tuning. The volume is small on purpose. The point is
completeness of practice in miniature, not scale.

All data is synthetic or from public sources.
