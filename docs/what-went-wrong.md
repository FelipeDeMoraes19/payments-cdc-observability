# What went wrong while building this

The defects worth keeping, because each one taught something the code alone does not
show. Summarised in the README; this is the long version.

## Three bugs that were the same bug

The observability milestone found three defects, and none of them was in the detection
logic. All three were the same mistake wearing different clothes: **a wrong assumption
about the normal that the detector measures against.**

**The consumer was an Airflow task.** A heartbeat alert reads a counter over a window. The
consumer lived 25 seconds out of every 900 and its counter restarted from zero on every
run, so the series existed 2.8% of the time. The alert would have been useless in both
directions at once: blind if missing data counted as normal, screaming if it counted as
alerting. The consumer is now a service.

**The healthcheck used `retries` where it needed `start_period`.** Thirty retries at five
seconds was there to tolerate a slow database init. It also meant a *running* database took
150 seconds of continuous failure before Docker would call it unhealthy. The detector was
not wrong, it was slow, and a slow detector is where a blind one starts. With
`start_period` for startup and three retries for steady state, the flip takes 20 seconds.

**The generator ran on demand.** `sum(increase(cdc_records_written_total[10m]))` compares
against a baseline. With load arriving only when somebody typed `make seed`, that baseline
was zero almost always and the alert would have fired permanently. The generator is now a
service too, at a low rate, which is what gives "it stopped" something to mean.

The pattern is worth more than the three fixes. Choosing a threshold is the easy half of an
alert. **The hard half is being right about what normal looks like**, and that answer lives
in architecture decisions made long before anyone opens the alerting page — how often a
process runs, how long it lives, whether anything is producing at all. An alert can be
blind because of a scheduling choice nobody connected to alerting.

That is also why this repository keeps a deliberately blind alert and a guard that hunts
for blind ones. The guard is in `tests/test_alerts_can_fire.py`, it asserts the rule count
before inspecting anything, and it detects blindness by empty series — not blindness by
unreachable threshold, which is a second kind it does not cover.


## The consumer is a service, and that is not streaming

The CDC consumer runs continuously, as a container that `docker compose up` starts. It
holds the replication slot, writes bronze, and exports two metrics on `:9108/metrics`. A
synthetic load generator runs continuously beside it, at a deliberately low rate.

**Both are writing while you read this.** The generator inserts about one payment every two
seconds, configurable with `GEN_RATE_PER_SECOND`, which is roughly 1800 rows an hour. Left
running overnight it grows the database by tens of thousands of rows and the bronze by
about sixty small files an hour. Stop them with `docker compose stop cdc generator` when
you are done looking.

**This does not move the scope fence.** The project rules out real streaming, and a long
running consumer is not stream processing: it reads the write ahead log and writes files.
Every transformation after it is still a batch, every fifteen minutes, triggered by
Airflow. No sliding windows, no streaming state, no Kafka and no Flink. What changed is
that the ingestion stopped pretending to have a schedule — it never had one, which is the
whole argument for having two DAGs.

The change was forced by measurement, not by taste. As an Airflow task the consumer lived
about 25 seconds out of every 900, and its counter restarted from zero on every run. A
heartbeat alert over that series would have been useless in both directions: with missing
data treated as normal it would have been permanently blind, and with missing data treated
as alerting it would have screamed continuously. ADR 0019 has the numbers.

**Duration is not measured here.** Airflow already records task duration and dbt records it
per model in `run_results.json`. Exporting it to Prometheus would need a Pushgateway for
the batch steps, adding a service to feed a number no alert reads. The orchestrator already
measures it; this does not duplicate it.

## The mechanism I invented, and how long it stood

This repository is about detectors that report "fine" while seeing nothing: an alert that
cannot fire, a healthcheck that answers the easy question, a test that passes for the wrong
reason. What follows is that same failure happening to the person writing it — twice, and
the second time was worse than the first.

The public repository was cloned and run, which is the one test nobody had done. `make
alerts` failed with a bare `401` from Terraform — a command that mentions no passwords
anywhere.

### What is true, and measured

**Grafana reads `GF_SECURITY_ADMIN_PASSWORD` only when it first creates its own database.**
After that the volume holds whatever was set then, and later edits to `.env` are ignored in
silence. Measured by changing only `.env` and recreating the container:

```
new password from .env, after recreating the container : 401
password the volume still holds                        : 200
```

A setting that looks applied and is not. That is the finding, and it survives.

### What was not true, and stood in this repository for a day

This file, and ADR 0009, both stated that `grafana cli admin reset-admin-password` prints
`Admin password changed successfully ✔` **without taking effect** — and offered that as a
measured fact, with three data points behind it.

**It is false.** Re-tested the next day, in both invocation forms: the command works. The new
password authenticates with `200`, the previous one drops to `401`.

What happened on the day was a `401` observed shortly after running the command, from which
a causal mechanism was written down. The variable was never isolated. The real cause of that
`401` was never diagnosed, and it is recorded here as undiagnosed rather than explained,
because a convincing wrong explanation in a permanent record is worse than an admitted gap.

### The part that is actually the lesson

The first failure was ordinary: a bad diagnosis under time pressure. The second was not.

**A confident causal mechanism went into a permanent record on the strength of one
unisolated observation**, was committed, was reported as understood, and was then written
into an ADR and into this file as a measured finding. It stood for a day. It came apart the
moment someone tried to build on it — which is the only reason it came apart at all.

Three things sit in that, and each is what this repository is about:

**A plausible explanation is not a tested one.** The `401` was real and the command had
just run, and the story joining them was never checked. Nothing about it looked wrong, which
is precisely why it survived being re-read.

**"Measured" is a claim about method, not about confidence.** Three data points were
collected and not one of them isolated the variable. Writing numbers down made the
conclusion feel settled. A test can pass for the wrong reason, and so can a person.

**What caught it was reproduction, not review.** The correction did not come from re-reading
the ADR, which had been re-read. It came from running the command again, cold, because
somebody was about to depend on it.

### What was done about it

The password stopped being a generated secret. It is now a fixed declared value, on the same
argument [ADR 0009](adr/0009-dedicated-replication-role.md) already makes for the local
Postgres: a dashboard on `localhost` over synthetic data is not a secret, and generating one
bought a rotation problem and no security. The PII key stays generated, and the difference
between the two is the point.

`make alerts` checks authentication before applying and explains the failure with the command
that resolves it, instead of letting Terraform emit a `401` that explains nothing.

And both the ADR and this section were rewritten to say what is known and to mark what is
not.
