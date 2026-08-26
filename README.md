# payments-cdc-observability

A data platform that fits in a `docker compose up` and hides none of the boring parts.

**[Browse the data model, its lineage and its tests](https://felipedemoraes19.github.io/payments-cdc-observability/)** — the
`dbt docs` site, served straight from this repository. Nothing to clone or run.

> **Status:** Milestone 1 is complete and the silver layer of Milestone 2 is built. Both
> sources land in bronze with schema contracts enforced at the boundary, and Spark turns
> the change log into a typed, masked silver. The dimensional model in dbt and the Airflow
> orchestration are next. This README grows with the project; rows and sections marked
> *planned* are not built yet.

Most portfolio data projects prove the happy path: call an API, load, transform, show a
dashboard. This one is about the parts that production demands and portfolios skip —
schema contracts, lineage, PII masking, idempotent backfill, observability, and failure
modes you can trigger with one command.

There are two sources on purpose, because a real platform never has one ingestion
pattern. A local Postgres OLTP is read through **logical replication**, decoded by a CDC
consumer written for this repository — no Debezium, no Airbyte. Daily exchange rates come
from the Brazilian central bank as an **incremental batch** with a watermark. Change data
capture is the risky part, so it was built first.

## Failure modes

The table every row of which names how the failure is injected, what is supposed to catch
it, and whether it actually does. Rows that are already true cite the test that proves
them; the rest are marked *planned* and are not claims yet.

| Failure mode | Injected by | Should be caught by | Caught? |
|---|---|---|---|
| A record written but never confirmed is replayed | `CDC_FAIL_BEFORE_FEEDBACK=1` kills the consumer between the write and the confirm | deduplication on `(key, lsn)`; replayed copies must carry an identical event | **Yes** — `tests/test_restart_consistency.py` |
| The consumer dies mid-stream | `SIGKILL` while the generator is running | the slot resumes from the last confirmed LSN, so nothing is lost | **Yes** — `tests/test_restart_consistency.py` |
| A column changes type at the source | `ALTER TABLE payments ALTER COLUMN amount TYPE text` | the contract checked against the `Relation` message, before any row of the new shape is written | **Yes** — `tests/test_schema_contract.py` |
| A published table has no contract | adding a table to the publication without declaring it | the same contract check, which refuses to ingest what nobody declared | **Yes** — `contracts/validation.py` |
| A source table is truncated | `TRUNCATE` on a published table | recorded as an event in bronze, then applied in silver: every key whose last change predates it is marked deleted | **Yes** — `tests/test_silver.py` |
| A row is deleted at the source | `DELETE` on a published table | silver keeps the row with its last known values and marks `is_deleted`, so the date of death survives | **Yes** — `tests/test_silver.py` |
| An unchanged TOASTed column arrives absent | an `UPDATE` touching only another column of a row with a large value | the marker recorded in bronze, and the last known value carried forward in silver | **Yes** — `tests/test_silver.py` |
| A value in bronze cannot be typed | a bronze file written outside the ingestion path | silver refuses to run, naming the column, the observed value and the expected type | **Yes** — `tests/test_silver.py` |
| A replication slot is left without a consumer | `make chaos-orphan-slot` | a Grafana alert on the WAL the slot retains | *Planned, Milestone 3* |
| Data arrives late | `make chaos-late` | a dbt freshness test | *Planned, Milestone 3* |
| A day of exchange rates is missing | `make chaos-fx-gap` | a `not_null` test on `amount_brl` | *Planned, Milestone 3* |
| PII reaches the gold layer | `make chaos-pii` | a custom test for CPF patterns in gold | *Planned, Milestone 3* |
| Nothing is being ingested at all | `make chaos-empty` | a heartbeat alert in Grafana | *Planned, Milestone 3* |
| **A blind alert** | `make chaos-blind` | **nothing — the alert cannot fire** | *Planned, Milestone 3* — **and it will stay "no", on purpose** |

The last row is the point of the exercise, and it is written down before it exists: an
alert whose threshold the metric can never reach. It stays in the repository, documented,
because "we do not detect this, and here is exactly why" is a sentence most systems need
and few say out loud.

## Architecture

```
 postgres (wal_level=logical)                    BCB SGS API
   payments, customers, merchants                daily PTAX rates
         |                                             |
         | logical replication, pgoutput               | incremental batch
         | decoded by this repository                  | watermark from the data
         v                                             v
   bronze/cdc/<table>/dt=<commit date>/        bronze/fx/<currency>/month=<YYYY-MM>/
         the raw change log, values as text     typed observations
                        |
                        | PySpark, in a container
                        | dedup by (key, lsn), TOAST carried forward,
                        | typing from the contract, PII masked with HMAC
                        v
                    silver/<table>          one row per key, is_deleted kept
                        |
                        v
                    gold                    planned: fct_payment and the dimensions
                                            in dbt, dim_customer as SCD2
```

Everything above the `gold` line exists and is covered by tests. Schema contracts sit at
the bronze boundary and again before typing in silver, so a change at the source stops the
pipeline instead of quietly reshaping the data.

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

### Where the two sources meet

The reason there are two ingestion patterns rather than one: a payment in a foreign
currency is only worth something in BRL once it is crossed with the rate published for the
day it happened. That join is the whole point of the milestone, and the raw bronze already
answers it.

```sql
WITH current_payments AS (
    SELECT key['payment_id']               AS payment_id,
           after['currency']               AS currency,
           after['amount']::DECIMAL(14,2)  AS amount,
           after['created_at'][1:10]::DATE AS payment_date
    FROM read_parquet('data/bronze/cdc/payments/**/*.parquet')
    WHERE action <> 'truncate'
    QUALIFY row_number() OVER (PARTITION BY key['payment_id'] ORDER BY lsn_numeric DESC) = 1
)
SELECT p.payment_id, p.currency, p.amount, r.rate_brl,
       round(p.amount * coalesce(r.rate_brl, 1), 2) AS amount_brl
FROM current_payments p
LEFT JOIN read_parquet('data/bronze/fx/**/*.parquet') r
       ON r.currency = p.currency AND r.quote_date = p.payment_date
WHERE p.currency <> 'BRL'
ORDER BY p.payment_id::BIGINT LIMIT 5;
```

```
3 | EUR | 1106.10 | 6.01290000 |  6650.87
4 | USD |  781.62 | 5.16040000 |  4033.47
6 | EUR | 2762.44 | 6.01290000 | 16610.28
8 | USD | 4333.09 | 5.16040000 | 22360.48
9 | EUR | 3510.59 | 6.01290000 | 21108.83
```

Milestone 2 turns this into a modelled `fct_payment` with dbt. The `LEFT JOIN` and the
`coalesce` are deliberate here: a payment made on a day with no published rate keeps its
amount and loses its conversion, which is exactly the hole a `not_null` test on
`amount_brl` is meant to find.

### Exchange rates

```
make fx
```

Daily PTAX sell rates from the Brazilian central bank, one SGS series per currency, into
`data/bronze/fx/<currency>/month=<YYYY-MM>/observations.parquet`. The whole month is
rewritten on every run, so resuming and backfilling are the same operation and running it
twice changes no observation.

The watermark is the latest quote date already in the bronze — there is no state file to
disagree with the data. Set `FX_START_DATE` and `FX_END_DATE` to extract an explicit
window instead.

Days with no quote are simply absent: weekends and holidays have no PTAX. That gap is
real, and later it is what a missing `amount_brl` will be traced back to.

### Silver

```
make spark-build
make silver
```

Spark reads the change log and writes one row per key into `data/silver/<table>`: current
version chosen by the highest LSN, unchanged TOASTed columns carried forward from the last
version that had them, values typed from the same contract the ingestion uses, and CPF and
e-mail replaced by an HMAC digest under a key that never enters the repository.

Rows that were deleted or truncated stay, marked `is_deleted`. A dimension that tracks
history needs to know when a row died, and an absent row has no date of death.

Spark runs inside a container on Java 21. Nothing about it is installed on the host, and
that is deliberate — ADR 0015 has the measurements.

### Gold

```
make gold
make docs
```

`make gold` builds the dimensional model into a DuckDB file with dbt and runs its tests.
`make docs` regenerates the lineage site linked at the top of this file.

`dim_customer` is a Type 2 dimension built **from the change log**, not from periodic
snapshots, so every version a customer ever had is there with the interval it was really
valid for. `fct_payment` joins to the customer version that was valid *when the payment
happened*, which is the entire reason the dimension keeps history. ADR 0017 explains why
`dbt snapshot` was rejected, and is careful to reject it for a reason that survives
reading dbt's own macros.

`amount_brl` comes from crossing the payment with the PTAX rate published for its day. It
is deliberately nullable: a day with no published rate leaves it empty rather than
guessing, which is what a `not_null` test can then catch.

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
| 0005 | PII masked in silver with HMAC under a managed key, and why that is pseudonymisation |
| 0008 | State lives in the replication slot and in the data, never in a side file |
| 0009 | Dedicated replication role, and where its password lives |
| 0010 | Where the durability boundary sits, and how batches are flushed |
| 0011 | The contract is checked on the `Relation` message, and what it deliberately skips |
| 0012 | `TRUNCATE` is recorded as an event, and what that does not yet solve |
| 0013 | Bronze in Parquet, the tuple as a `MAP`, and the LSN stored twice |
| 0014 | The BCB extractor: SGS series, monthly windows, watermark in the data |
| 0015 | Spark runs in a container, and why Spark is here at all |
| 0016 | Silver semantics: one row per key, and what happens to what died |

## Scope

No cloud, no Kafka, no real streaming, no custom front end, no machine learning, no
authentication, no performance tuning. The volume is small on purpose. The point is
completeness of practice in miniature, not scale.

All data is synthetic or from public sources.
