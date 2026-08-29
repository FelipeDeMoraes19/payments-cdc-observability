# Running the pipeline, stage by stage

Reference for someone who wants to drive each layer by hand. The README covers the
two commands that get the whole thing running; this is everything else.

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

## Running it, stage by stage

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

**Determinism.** Running the whole pipeline twice over the same source leaves the gold
identical, table by table and row by row, and there is a test that proves it. No model
reads an execution clock; every surrogate key is derived from the data.

**Backfill by interval is not built yet, and saying so is the point.** Today the pipeline
rebuilds everything from bronze, so "run it for the 12th" has no meaning to express. That
half of the milestone's acceptance criterion arrives with the Airflow DAG, which is what
introduces a date parameter in the first place. It is pending, not passing.

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

## Orchestration

```
make airflow                              # http://localhost:8082
make backfill-fx FROM=2026-08-17 TO=2026-08-23
```

There are **two DAGs, not one**, and the reason is the most interesting decision of this
milestone.

`data_interval` is a promise that a run corresponds to a slice of time. It is a contract,
not decoration. The exchange rate extractor can keep that promise: a quote belongs to a
day, so "run it for the 12th" means something and backfilling a week is a real operation.
The change stream cannot: you never ask Postgres for "the changes of the 12th", you ask
for "whatever is after LSN N". A commit timestamp is an attribute of a change, not a
handle you can seek by, and a replication slot has one position rather than a calendar.

Putting both in one DAG would make `data_interval` true for half of it and a lie for the
other half. The lie is operational, not aesthetic: someone clears an old run expecting to
reprocess that day, and the CDC drains the present instead.

So `fx_daily` is date partitioned and backfillable, and `cdc_to_gold` is position based
and is not. Both have `catchup=False`, which is a deliberate declaration rather than an
oversight: in Airflow 3 that is already the default, and backfill became an explicit
command instead of a side effect of activating a DAG. A command you can demonstrate beats
a side effect you have to explain.

**They are not coupled, and that is also deliberate.** Gold can run before the day's rate
exists. Making it wait would trade a visible failure for an invisible one — a null
`amount_brl` that a `not_null` test points at, replaced by a DAG that simply never ran.
And a day without a quote is *normal*: weekends and holidays have no PTAX, so anything
waiting for one would wait forever every Saturday.

### The acceptance criterion this milestone changed

The plan said: *a seven day backfill must give the same result as running day by day.* That
sentence was written before the architecture existed, and on this architecture it would
have become **vacuous** — silver and gold rebuild from bronze, so nothing downstream reads
the date parameter, and the test would have passed even with the pipeline broken.

The question underneath backfill is not about dates. It is **compositionality**: does
processing an interval give the same result as processing its parts? That question
survives; only the unit changes, to the one each half can actually measure.

| Half | Unit | What is asserted |
|---|---|---|
| Exchange rates | date | one call for the 1st to the 7th leaves what seven single day calls leave |
| Change stream | batch | draining in many small batches leaves the same bronze, silver and gold as draining in one |

Neither passes by accident. Each refuses to run if its own setup was degenerate — the
window test fails if every observation landed on one day, and the batch test fails if both
drains happened to produce the same number of files, which would mean the batch size never
differed at all.

The criterion was **translated, not shrunk**. That distinction is the point: an acceptance
criterion that quietly becomes trivially true is worse than one that fails honestly.

