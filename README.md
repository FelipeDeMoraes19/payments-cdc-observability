# payments-cdc-observability

**A data platform that fits in a `docker compose up` and hides none of the boring parts.**

Most data portfolios prove the happy path. This one is about what production demands and
portfolios skip: schema contracts, lineage, PII masking, idempotent backfill, and failure
modes you can trigger with one command — including one this system deliberately cannot see.

[**Browse the data model and its lineage**](https://felipedemoraes19.github.io/payments-cdc-observability/) —
the `dbt` docs site, served from this repository. Nothing to clone.

## Failure modes

Every row names how the failure is injected, what should catch it, and whether it does.
Rows claiming proof cite a file you can open; a test asserts they all exist, because a
table promising a detector nobody wrote is this project's own thesis happening in the
document that explains it.

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
| Reverting a schema change does not unblock the stream | change a column type, let the consumer stop, then revert the column | nothing automatic: the slot's backlog still carries the old shape, so the consumer stays stopped until someone decides what to do with it | **Yes, it stays stopped** — deliberately; recovery is a human decision (ADR 0018) |
| Running the whole pipeline twice changes the gold | run ingestion, silver and dbt twice over the same source | nothing should change; every model is a deterministic function of the data, with no execution clock anywhere | **Yes** — `tests/test_gold_determinism.py` |
| The size of the consumer's batch leaks into the result | two slots created before any change, one drained in a single batch and one a record at a time | nothing should differ; batch size may change the file layout but never the data | **Yes** — `tests/test_batch_decomposition.py` |
| Extracting a window differs from extracting its days | fetch the 1st to the 7th in one call, and again as seven single day calls | nothing should differ, because a window rewrites its whole month | **Yes** — `tests/test_fx_extractor.py` |
| An init script fails and the database comes up incomplete | drop an artifact the init scripts create, standing in for the script that failed to create it | the healthcheck, which asks whether the init finished rather than whether the port answers | **Yes** — `tests/test_healthcheck_detects_incomplete_init.py` |
| Bronze outlives the database that produced it | recreate the volume by hand and keep `data/bronze`, or copy bronze between machines | `source_system_id` recorded per row; silver refuses bronze holding more than one database instance, and refuses bronze with none | **Yes** — `transform/spark/bronze_to_silver.py`, `make reset` prevents the common case (ADR 0021) |
| A replication slot is left without a consumer | `make chaos-orphan-slot` stops the consumer and leaves the slot | a Grafana alert reading retained WAL from Postgres by SQL, because a dead consumer cannot report its own death | **Yes** — `tests/test_alerts_can_fire.py` proves the alert can fire |
| Data arrives late | `make chaos-late` restarts the generator emitting events dated 48h in the past; ingestion volume is unchanged, so the heartbeat stays quiet | `dbt source freshness` on `created_at`, the business event time, not on the moment of ingestion | **Yes** — measured: `ERROR STALE` at a one minute threshold, `PASS` at the normal ten |
| A day of exchange rates is missing | `make chaos-fx-gap` | a `not_null` test on `amount_brl` | *Planned, Milestone 3* |
| PII reaches the gold layer | `make chaos-pii` | a custom test for CPF patterns in gold | *Planned, Milestone 3* |
| Nothing is being ingested at all | `make chaos-empty` stops the generator | a heartbeat alert over `increase(cdc_records_written_total[10m])` | **Yes** — `tests/test_alerts_can_fire.py` proves the alert can fire |
| **A blind alert** | `make chaos-blind` injects nothing, because nothing can be injected | **nothing — the alert queries a label no series carries and treats no data as normal** | **No, on purpose** — and `tests/test_alerts_can_fire.py` asserts it is still blind for both reasons |

**The last row is the point.** An alert whose query filters a label no series carries, with
missing data configured as normal. Two choices, each defensible alone, fatal together —
which is how alerts go blind in production. It stays, documented, because *"we do not
detect this, and here is exactly why"* is a sentence most systems need and few say.

## Run it

```
make up        # Postgres, the CDC consumer, a load generator, Prometheus, Grafana
make alerts    # dashboards and alert rules, provisioned with Terraform
```

`make up` writes `.env` on first run with generated secrets — nothing real is committed and
a fresh clone needs no file you do not have. Only Docker and Python are required; Spark,
dbt, Terraform and Airflow all run in containers.

**Both the consumer and the generator keep writing while you read this**, at about one
payment every two seconds. Stop them with `docker compose stop cdc generator`.

## Break it

```
make chaos-orphan-slot     # stop the consumer, leave its replication slot behind
make slots                 # watch retained WAL climb — the alert reads this by SQL
make chaos-heal            # put it back
```

Grafana is on `:3000`, Prometheus on `:9090`, Airflow on `:8082`. The dashboard shows the
three metrics and the alert list, where one rule is permanently healthy on purpose.

Other injectors: `chaos-empty`, `chaos-late`, `chaos-fx-gap`, `chaos-pii`, `chaos-blind`.
Each fails loudly if the alert it depends on was never provisioned, rather than injecting a
failure nobody is watching for.

## How it works

```
 postgres (wal_level=logical)              BCB SGS API
   payments, customers, merchants          daily PTAX rates
         |                                       |
         | logical replication, pgoutput         | incremental batch,
         | decoded by this repository            | watermark from the data
         v                                       v
   bronze/cdc/<table>/dt=<commit date>/   bronze/fx/<currency>/month=<YYYY-MM>/
         the raw change log, values as text
                        |
                        | PySpark: dedup by (key, lsn), TOAST carried forward,
                        | typing from the contract, PII masked with HMAC
                        v
                    silver/<table>       the cleaned change log, is_current marked
                        |
                        | dbt: dimensional model in DuckDB
                        v
                    gold                 fct_payment, dim_customer as SCD Type 2
```

Two sources on purpose, because a real platform never has one ingestion pattern. A payment
in a foreign currency is only worth something in BRL once crossed with the rate published
for its day, and that join is why both exist.

`dim_customer` is built **from the change log**, not from `dbt snapshot`: a snapshot only
sees the state that existed when it ran, so a change born and dead between two runs never
existed for it. Two Airflow DAGs, not one, because `data_interval` is a promise that a run
matches a slice of time — the rate extractor can keep it and a replication slot cannot.

- [Running each stage by hand](docs/running.md) — querying bronze, silver, gold, backfill
- [What went wrong while building this](docs/what-went-wrong.md) — three defects that were
  the same defect: a wrong assumption about the normal a detector measures against

## Known limitations

**Bronze grows without bound.** Roughly 1440 small files a day at the default rate, and
nothing compacts or expires them. Out of scope, and written down because a repository about
documenting what it does not detect should document what it does not clean up.

**CDC captures change, not state.** A replication slot only carries what happens after it
exists, so a row inserted before the consumer started is invisible until something changes
it again. Five merchants were inserted once at startup and never reached bronze at all. The
production answer is an initial snapshot, which Debezium has and this consumer does not;
here the synthetic generator touches its own dimensions instead.

**The vacuity guard covers one kind of blindness.** It finds alerts whose query returns no
series. An alert with a healthy query and an unreachable threshold would pass it.

**Alert rules come from `make alerts`, not from `docker compose up`.** On a stack where
nobody ran it there are no alerts at all, which the guard catches by asserting the rule
count before inspecting anything.

## Decision records

Short documents in `docs/adr/`, written **before** the decision, in Portuguese. The field
that matters most is *rejected alternatives and why*.

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
| 0016 | Silver semantics: the cleaned change log, and what happens to what died |
| 0017 | SCD Type 2 built from the change log, and why `dbt snapshot` was rejected |
| 0018 | Recovering from a contract violation, and why reverting the source is not enough |
| 0019 | What a date means in this pipeline, and why there are two DAGs |
| 0020 | What is detected by an alert and what is detected by a test |
| 0021 | An LSN is only comparable within one database instance |

## Tests

```
make test        # ~20s, needs only Postgres
make test-e2e    # ~8min, drives containers; stops the writing services and restarts them
make test-chaos  # ~25s, injects failures and asserts the detectors fire
```

`make test-e2e` runs before closing a milestone, not on every change.

## Scope

No cloud, no Kafka, no real streaming, no custom front end, no machine learning, no
authentication, no performance tuning. The volume is small on purpose: the point is
completeness of practice in miniature, not scale.

All data is synthetic or from public sources.
