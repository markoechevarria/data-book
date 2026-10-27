# Fault Tolerance in Data Pipelines

> Every distributed system you will ever build *will* fail — partially, intermittently, and at the
> worst possible time. The senior differentiator is not "writing code that doesn't crash" (impossible)
> but **designing pipelines whose failures are contained, observable, and automatically recoverable**.
> This chapter is about retries, checkpoints, dead-letter queues, transactional boundaries, and the
> delivery semantics you actually get versus the ones you think you bought.

---

## TL;DR

- **Failure is a design input, not an exception.** Assume: network partitions, a source 500, a sink
  timeout, a worker OOM, a clock skew, and a poison record in the batch.
- **Retries are only safe if the operation is idempotent.** Retries without idempotency create
  duplicates; idempotency without retries creates manual toil. They are one mechanism. See
  [03-idempotency.md](./03-idempotency.md).
- **Checkpointing turns "restart from zero" into "resume from the last committed point"** — offsets,
  operator state, and output commits must be checkpointed *together*.
- **A DLQ is a promise, not a trash can.** A DLQ with no consumer, alert, or replay path is a
  data-loss mechanism with better branding.
- **"Exactly-once" at the pipeline level is a combination of:** at-least-once delivery + idempotent
  or transactional sinks + deterministic replay. No single system gives it to you end-to-end.
- **Partial failure is the norm at scale.** Design task boundaries so a partial failure is *isolated
  to one partition or one record*, and everything else can proceed.
- **Transactions are the strongest tool** — but only if all writers go through the same transactional
  boundary (Delta/Iceberg commit, warehouse transaction). Two writers on one non-transactional sink
  cannot be made exactly-once.
- **Observability is part of fault tolerance.** A failure nobody sees is an outage that never ends.

---

## 1. Definitions and core vocabulary

| Term | Definition |
|------|-----------|
| **Fault** | An unexpected internal defect (bug, disk failure). |
| **Failure** | The observable consequence: wrong/absent output. |
| **Error** | A detectable fault that a program can handle. |
| **At-least-once (ALO)** | Every record is processed ≥1 time; duplicates possible. The realistic default. |
| **At-most-once (AMO)** | Every record processed ≤1 time; loss possible on failure. Rarely acceptable. |
| **Exactly-once (EOS)** | Every record has exactly one *effect* on the output. |
| **Idempotency** | Applying an operation N times ≡ applying it once. |
| **Retry** | Re-executing a failed operation, ideally with backoff + jitter. |
| **Backoff** | Increasing delay between retries to reduce load on a struggling system. |
| **Jitter** | Randomization of retry delay to avoid synchronized retry storms. |
| **Circuit breaker** | Stop calling a failing dependency after N failures; fail fast, probe later. |
| **Checkpoint** | Durable record of progress (offsets, state, output watermark). |
| **Write-Ahead Log (WAL)** | Log written before applying state, enabling replay after a crash. |
| **Dead-letter queue (DLQ)** | A side channel for records that could not be processed. |
| **Poison message** | A record that will *always* fail (malformed, malicious, schema-incompatible). |
| **Transactional sink** | A sink that supports atomic multi-record commits (DB, Kafka txn, Delta/Iceberg). |
| **Two-phase commit (2PC)** | Protocol for atomically committing across multiple participants. |
| **Watermark** | Event-time signal that all events before T have (probably) arrived. |
| **Fencing token / epoch** | Monotonic ID used to reject writes from a superseded writer/zombie. |
| **Blast radius** | How much of the pipeline/output is affected by one failure. |

---

## 2. What problem it solves / why it matters

Picture an hourly pipeline with five stages. If each stage has 99.9% reliability, the pipeline's
success rate is `0.999^5 ≈ 99.5%` — **about 3–4 failures per month**. Add an 8-task DAG and it drops
further. At scale, failures are not exceptional events; they are the steady state.

Fault tolerance answers four questions:

1. **What happens when a task fails mid-way?** Does it corrupt output, duplicate rows, or leave a
   half-written table?
2. **How does the pipeline resume?** From the beginning (expensive, and only safe if idempotent), or
   from a checkpoint (cheap, but requires correct state management)?
3. **What happens to the records that cannot be processed?** Are they dropped silently, or quarantined
   and surfaced?
4. **Does anyone know?** Can the on-call engineer reconstruct what happened and fix the interval?

And it matters commercially: silent data loss or duplication corrupts dashboards, mis-invoices
customers, and trains analysts to distrust the platform — the most expensive failure mode of all.

---

## 3. How it works internally

### 3.1 The delivery-semantics ladder

```text
Producer ──ack?──▶ Broker/Store ──commit?──▶ Consumer ──commit?──▶ Sink
   │                   │                        │                  │
  AMO/ALO          durability              offset commit      transactional?
```

| Level | Guarantee | How you get it | What it costs |
|-------|-----------|----------------|---------------|
| Delivery ALO + non-idempotent sink | Duplicates | Default retry behavior | Duplicate rows/counts |
| Delivery ALO + idempotent sink | Effectively EOS | Upserts/MERGE on a stable key | Compute per dedup |
| Transactional ALO + transactional sink | True EOS within the boundary | Kafka txn + Delta/Iceberg commit | Latency, complexity, boundary limits |
| AMO | Possible loss | Commit offset *before* processing | Data loss on failure |

> **Senior note:** The phrase "exactly-once" almost always means *"exactly-once effects on the
> output, assuming retries and a deduplicating/idempotent sink"*. Any side effect outside the
> transactional boundary (an email, a charge, a REST call) is **not** covered and needs its own
> idempotency key.

### 3.2 Kubernetes/OS-level node failures — what the engine does

- **Spark:** a lost executor is detected; failed tasks are re-run on surviving executors. Lost driver
  = whole job fails (cluster mode) or the client dies (client mode). Data in shuffle files on a lost
  executor is recomputed from lineage (RDD/plan recomputation).
- **Flink:** state is checkpointed to durable storage (S3/HDFS); on failure, the job restarts from the
  last checkpoint and re-reads offsets from the source. `EXACTLY_ONCE` checkpointing uses barriers +
  2PC for sinks with transactional support.
- **Kafka:** replication factor + `min.insync.replicas` + `acks=all` decide durability. ISR
  (in-sync replicas) shrinking is the early warning of an at-risk topic.

### 3.3 Checkpoint internals

```text
Flink checkpoint:
  source partitions --[barrier]--> operators (snapshot state) --[barrier]--> sinks (pre-commit)
        │                                  │                                    │
        └── offset snapshot ───────────────┴────────── state snapshot ─────────┴─ txn pre-commit
                                    (all in one coordinated snapshot)
```

Key property: **the snapshot is a consistent cut across all operators**. If the checkpoint succeeds,
the job can resume as if it never crashed — no duplicates inside the boundary, no lost records.

Spark Structured Streaming: the checkpoint directory holds `offsets/`, `commits/`, and `state/`
(for stateful ops). Recovery = read the last committed offset, replay from there. Combined with an
idempotent `foreachBatch` sink, you get EOS.

**Checkpoint pitfalls:**

| Pitfall | Consequence |
|---------|-------------|
| Changing the query/plan incompatibly | `SparkException: ... incompatible checkpoint` — cannot resume |
| Sharing a checkpoint dir between jobs | Corruption / cross-talk |
| Checkpoint on ephemeral storage | Loss on node replacement |
| Different code but same checkpoint (streaming) | Silent wrong results |
| No checkpoint (batch) | Every retry reprocesses everything |

### 3.4 DLQ internals

```text
        ┌────────────────┐   ok     ┌───────────┐
source ─▶ processing logic ├─────────▶ sink
        └───────┬────────┘
                │ error (with metadata: payload, error, timestamp, attempt, source offset)
                ▼
        ┌────────────────┐    ┌──────────────┐    ┌────────────────┐
        │   DLQ topic/   │───▶│  Triage /    │───▶│ Replay (fixed  │
        │   table/bucket │    │  alerting    │    │  + reprocessed)│
        └────────────────┘    └──────────────┘    └────────────────┘
```

A DLQ record must carry **enough context to replay deterministically**: the original payload, the
source offset/partition/timestamp, the error class, the attempt count, and the schema version.
Without the offset you cannot safely re-inject without duplicating.

### 3.5 Transactional guarantees in sinks

| Sink | Mechanism | Boundary |
|------|-----------|----------|
| Postgres/RDBMS | `BEGIN … COMMIT`, MVCC, row locks | One transaction |
| Delta Lake | Optimistic concurrency; atomic commit via `_delta_log` | One commit |
| Iceberg | Snapshot + atomic catalog swap (rename) | One snapshot |
| Kafka | Transaction coordinator + `read_committed` | One transaction |
| Files on S3 | **No atomicity** across objects (only per-object `PutObject`) | One object |
| Snowflake/BigQuery | Single-statement transactions (auto-commit), DML atomicity | One statement |

> **Senior note:** Object storage is the weak link. If you write 10,000 Parquet files for a partition,
> a crash at file #6,001 leaves a partially-written partition that *looks* valid. This is exactly why
> table formats (Delta/Iceberg/Hudi) exist: **the commit is a metadata operation**, so partial writes
> are invisible until the commit lands.

---

## 4. How to implement it

### 4.1 Retries with exponential backoff and jitter

```python
import random, time
from tenacity import retry, stop_after_attempt, wait_exponential_jitter, retry_if_exception_type

class RetryableSourceError(Exception): ...
class FatalSourceError(Exception): ...          # e.g. 401/403 — do NOT retry

@retry(
    stop=stop_after_attempt(6),
    wait=wait_exponential_jitter(initial=1, max=60, jitter=5),
    retry=retry_if_exception_type(RetryableSourceError),
    reraise=True,
)
def fetch_page(url: str, params: dict) -> dict:
    r = session.get(url, params=params, timeout=(5, 30))
    if r.status_code in (429, 500, 502, 503, 504):
        raise RetryableSourceError(f"transient {r.status_code}")
    if r.status_code >= 400:
        raise FatalSourceError(f"permanent {r.status_code}")
    return r.json()
```

Rules: **retry only idempotent operations**, only transient errors; add jitter; respect `Retry-After`;
and always have a maximum attempt count with a visible terminal failure.

### 4.2 Airflow-level retries and timeouts

```python
default_args = {
    "retries": 3,
    "retry_delay": pendulum.duration(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": pendulum.duration(hours=1),
    "execution_timeout": pendulum.duration(hours=2),      # kill stuck tasks
    "sla": pendulum.duration(hours=3),                    # alert if too slow (not a kill)
    "on_failure_callback": notify_slack,
}

# Idempotency is what makes these retries safe:
with DAG("orders_daily", default_args=default_args, max_active_runs=1, ...) as dag:
    ...
```

Also consider `on_retry_callback`, `email_on_retry=False` (avoid alert fatigue), and **task-level
`retries=0`** for non-idempotent steps you want to fail loudly rather than double-apply.

### 4.3 Spark Structured Streaming — checkpoint + idempotent `foreachBatch`

```python
def upsert_orders(batch_df, batch_id):
    # Deterministic, keyed, idempotent write: re-running batch_id N is a no-op
    (batch_df.dropDuplicates(["order_id"])
             .write.mode("overwrite")
             .option("replaceWhere", f"ingest_batch = {batch_id}")   # Delta
             .saveAsTable("raw.orders"))
    # Or MERGE on order_id — safe under at-least-once replay

query = (
    spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", brokers)
        .option("subscribe", "orders")
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "true")      # don't silently skip missing offsets
        .load()
        .selectExpr("CAST(value AS STRING) AS json")
        .writeStream
        .option("checkpointLocation", "s3://chk/orders_v3")   # unique dir per job
        .option("maxOffsetsPerTrigger", 200000)               # bounded micro-batch
        .trigger(processingTime="1 minute")
        .foreachBatch(upsert_orders)
        .start()
)
```

Note `failOnDataLoss=true`: if Kafka deleted the offsets you still need, silently skipping is data loss.
Fail loudly; decide manually whether skipping is acceptable.

### 4.4 Dead-letter queue for a batch job

```python
def process_batch(rows: list[dict]) -> None:
    good, bad = [], []
    for i, row in enumerate(rows):
        try:
            validate(row)                       # schema + business rules
            good.append(transform(row))
        except Exception as e:
            bad.append({
                "payload": row,
                "error_class": type(e).__name__,
                "error_message": str(e)[:1000],
                "source": batch_source,
                "source_offset": offsets[i],
                "attempt": attempt,
                "failed_at": utc_now_iso(),
                "schema_version": "orders_v7",
            })

    if good:
        # transactional, idempotent write of the good rows only
        merge_into_target(good)

    if bad:
        write_jsonl(bad, f"s3://dlq/orders/dt={today}/part-{uuid4()}.jsonl")
        emit_metric("dlq_records", len(bad), tags={"pipeline": "orders"})
        if len(bad) / len(rows) > 0.05:          # >5% failure -> treat as an incident
            raise AirflowFailException("DLQ rate above threshold; halting pipeline")
```

**Replay path (mandatory):**

```bash
# After the bug is fixed:
python replay_dlq.py --dlq s3://dlq/orders/dt=2026-01-15/ --target raw.orders \
                     --dedupe-key order_id --dry-run
# Dry-run → inspect → real run (idempotent MERGE) → mark DLQ records as replayed
```

### 4.5 Kafka EOS with transactions

```java
props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "orders-etl-1");
props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);   // implied by transactions
props.put(ProducerConfig.ACKS_CONFIG, "all");

producer.initTransactions();
ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
producer.beginTransaction();
try {
    for (ConsumerRecord<String, String> r : records) {
        producer.send(new ProducerRecord<>("orders.enriched", enrich(r)));
    }
    // THE KEY STEP: commit the consumer offset inside the SAME transaction
    Map<TopicPartition, OffsetAndMetadata> offsets = currentOffsets(records);
    producer.sendOffsetsToTransaction(offsets, consumer.groupMetadata());
    producer.commitTransaction();
} catch (Exception e) {
    producer.abortTransaction();
    // re-poll; the aborted writes are invisible to read_committed consumers
}
```

Set `isolation.level=read_committed` on the consumer side, otherwise you read uncommitted (aborted)
records. And remember: this covers Kafka→Kafka. Writing to an external DB still needs its own
idempotency or 2PC.

### 4.6 Delta/Iceberg transactional commit (the WAP boundary)

```python
# Delta: overwrite only the affected partition -> atomic, idempotent
(spark.read.format("delta").load("s3://lake/orders")
      .filter("order_date = '2026-01-15'")
      .write.mode("overwrite")
      .option("replaceWhere", "order_date = '2026-01-15'")
      .save("s3://lake/orders"))
```

```sql
-- Iceberg: write to a staging branch, validate, then fast-forward the main branch
ALTER TABLE prod.orders CREATE BRANCH audit_2026_01_15;
-- ... write into audit_2026_01_15 ...
-- run quality checks against the branch, then:
ALTER TABLE prod.orders FAST FORWARD main TO audit_2026_01_15;
```

---

## 5. Where to implement it (tools and when each fits)

| Concern | Tools | Notes |
|---------|-------|-------|
| Task retries/backoff | Airflow retries, Dagster `RetryPolicy`, Prefect `retries`, Argo `retryStrategy` | Config-first; never hand-roll |
| Checkpointing (streaming) | Spark SS checkpoints, Flink savepoints/checkpoints, Kafka Streams state dir | *Savepoints* are the triggered, addressable variant (needed for upgrades) |
| DLQ | Kafka DLQ topic, SQS DLQ + redrive, S3 quarantine bucket, DB table | Must have an owner, alert, and replay tool |
| Transactional sinks | Delta, Iceberg, Hudi; Postgres; Kafka transactions; Snowflake/BigQuery DML | The only real EOS boundary |
| CDC / exactly-once replication | Debezium + outbox, Fivetran, DMS | Replication slots + WAL = durable change capture |
| Circuit breaking | Resilience4j, `tenacity` (via attempt limits), Envoy/Istio, AWS App Mesh | Protect failing dependencies |
| Idempotency keys | Application-level; DB unique constraint; Kafka key + compaction | See [03-idempotency.md](./03-idempotency.md) |
| Poison message isolation | Schema registry + DLQ; validation gate before processing | Fail fast, isolate, alert |
| Chaos testing | Chaos Monkey/Chaos Mesh, `kill -9` drills, network fault injection | Prove your recovery path works |

**When each fits:**

- **Retries** for transient errors (5xx, timeouts, throttles). Never for validation/business errors.
- **DLQ** when a small fraction of records are bad and you must not block the batch.
- **Checkpointing** for any long-running or streaming job.
- **Transactional sinks** when duplicates are unacceptable (billing, inventory, regulatory).
- **Circuit breakers** when a downstream dependency's failure would otherwise cascade.

---

## 6. Patterns and anti-patterns

### 6.1 Patterns

| Pattern | Purpose |
|---------|---------|
| **Retry + jitter + cap** | Survive transient faults without thundering herds |
| **Circuit breaker** | Fail fast; stop hammering a dead dependency |
| **Idempotent consumer** | Makes ALO delivery safe (the foundation of EOS effects) |
| **Outbox / inbox pattern** | Atomically persist business data + an event to publish (CDC friendliness) |
| **DLQ + replay tool** | Isolate poison records; re-inject after fixing the cause |
| **Checkpoint + watermark + idempotent sink** | End-to-end exactly-once effects |
| **Two-phase commit (Flink/Kafka txn)** | Atomic across sink + offset |
| **Bulkhead / isolation** | One tenant/partition failure cannot take down the rest |
| **Fencing tokens / epoch numbers** | Prevent zombie writers after a failover |
| **Graceful degradation** | Serve stale-but-correct data rather than nothing |
| **Poison-pill quarantine with rate limit** | 5% bad is a bug, 0.001% is a data issue |
| **Staging table + atomic swap** | Never expose partially-written data |

### 6.2 Anti-patterns

| Anti-pattern | Why it is dangerous |
|--------------|---------------------|
| **Retry without idempotency** | Duplicates appear; reconciliations break |
| **Retry on non-transient errors** | Amplifies load on a system that is fundamentally rejecting you |
| **Infinite retries** | The job never fails, so nobody is paged, and lag grows forever |
| **DLQ with no consumer** | Silent data loss with extra steps |
| **DLQ that loses context** | Unreplayable; you must re-extract everything |
| **Catching all exceptions** | Hides bugs; the pipeline "succeeds" with wrong data |
| **`mode("append")` with retries** | Duplicate partitions on every retry |
| **`mode("overwrite")` on the whole table for a partition update** | Destroys other partitions |
| **Sharing a checkpoint directory** | Corruption across jobs |
| **Committing offsets before the sink** | At-most-once: data loss on crash |
| **Exactly-once claims outside the transactional boundary** | Email/charge/API call duplicated |
| **One giant atomic transaction** | Long locks, huge rollbacks, no partial progress |
| **No `failOnDataLoss`** | Silent gap in the stream |
| **Testing only the happy path** | The failure path is where the bugs live |
| **Ignoring clock skew** | Watermarks and windows behave incorrectly |

---

## 7. Real-world use cases

**Payments/billing.** Duplicate charges and missing invoices are both catastrophic. Design: Kafka
transactions into Delta, idempotency keys on every external call, a reconciliation job that compares
ledger vs. processor daily. Retries are permitted because the sink is idempotent by key.

**Retail inventory sync.** A partial failure that updates 60% of SKUs corrupts the catalog. Design:
process per-warehouse partitions (blast radius = one warehouse), stage to a temp table, validate, then
swap. A DLQ catches malformed supplier feeds.

**Marketing attribution.** Late-arriving click events. Design: watermarks tolerate lateness; the
recompute of a recent window is idempotent (delete+insert by day); a DLQ catches events that fail
schema validation, with a replay after the upstream fix.

**IoT fleet telemetry.** 1 pipeline, 1M devices, frequent network blips. Design: device-side buffering,
at-least-once delivery, dedup by (device_id, sequence_no) in the sink, and a *deliberate* sampling
policy for non-critical metrics when backpressure hits.

**Healthcare data exchange.** Every record must be accounted for (regulatory). Design: an intake
ledger that records every received/processed/rejected file; DLQ with full audit; no silent drops ever;
reconciliation reporting is a first-class deliverable.

**Financial market data.** A missed tick matters, but a duplicated tick also corrupts derived signals.
Design: idempotent writes keyed by (symbol, exchange_seq), circuits to isolate a misbehaving feed,
and a "gap detector" that flags missing sequences.

---

## 8. Senior-level deep dive

### 8.1 The EOS decision framework

```text
Are duplicates in the output acceptable (dashboards, approximate metrics)?
├─ YES → At-least-once + retries. Keep it simple. Deduplicate at read time if needed.
└─ NO  → Is the sink transactional (Delta/Iceberg/RDBMS/Kafka txn)?
        ├─ YES → ALO delivery + idempotent MERGE, OR transactional exactly-once if the boundary
        │        covers both offset and sink.
        └─ NO  → Make the effect idempotent yourself:
                 - write to an intermediate transactional store, then publish
                 - or attach a unique idempotency key per record and dedupe on write
                 - or funnel all writes through a single writer that enforces the invariant
```

**You cannot get EOS across a boundary you don't control.** Be explicit about the boundary in your
design docs.

### 8.2 Blast radius as a design metric

| Design | Blast radius of one bad record | One bad task |
|--------|-------------------------------|--------------|
| One giant task per pipeline | Whole batch | Whole pipeline |
| Task per partition | One partition | One partition |
| Task per partition + DLQ | One record | One partition |
| Task per record (streaming) | One record | One record |

Smaller blast radius = more tasks = more orchestration overhead. The senior skill is finding the
sweet spot (usually partition-level) and *making each partition independently recoverable*.

### 8.3 Failure-mode catalog

| Failure | Detection | Immediate response | Long-term fix |
|---------|-----------|--------------------|---------------|
| Transient API 5xx | Retry counter, error rate | Retry with backoff | Backoff + circuit breaker |
| Rate limit (429) | HTTP 429, throttle metrics | Backoff, honor `Retry-After` | Token bucket + pools |
| Poison message | DLQ growth, task failure | Quarantine, alert | Validation gate + schema registry |
| Sink timeout | Task failure, latency spike | Retry (idempotent), reduce concurrency | Staging table, batch writes |
| Worker OOM | Executor/task OOM logs | Rerun smaller, increase memory | Fix skew, repartition |
| Zombie writer | Two commits to same target | Fencing token rejects stale writer | Epoch/lease on the writer |
| Partial partition write | Row count mismatch, freshness gap | Delete partition, re-run | Table format commit; staging + swap |
| Clock skew | Watermark anomalies, window errors | Widen windows | NTP, event-time not processing-time |
| Checkpoint incompatibility | Job refuses to start | Restart from scratch / new checkpoint | Version checkpoints; savepoint discipline |
| Split brain (Kafka) | Duplicate consumption, ISR churn | Fence the old coordinator | `min.insync.replicas`, `acks=all` |
| Silently skipped offsets | Gap detection | Investigate; decide to replay | `failOnDataLoss=true` |
| Cascading failure | One dependency down → all jobs fail | Circuit breaker, degradation | Bulkheads per dependency |

### 8.4 Testing fault tolerance (do it deliberately)

1. **Unit:** simulate a mid-processing exception → assert no partial write and a DLQ record.
2. **Retry:** force the sink to fail twice → assert the third attempt succeeds and produces one row.
3. **Idempotency:** run the same batch twice → assert identical row counts and checksums.
4. **Replay:** kill the process mid-batch → resume from checkpoint → assert exactly-once effects.
5. **Chaos:** `kill -9` a worker during a run; drop network to a source; fill the DLQ.
6. **Poison:** inject a malformed record → assert quarantine + alert + continued processing of others.
7. **Clock:** run with a shifted clock → assert event-time logic is unaffected.

> **Senior note:** A pipeline without a **runbook** is not fault-tolerant. Document, for each failure
> class: how to detect it, what to do first, how to re-run the affected interval, and how to verify.
> This must be written before the incident, not during it.

### 8.5 Performance implications of fault tolerance

| Mechanism | Overhead | Notes |
|-----------|----------|-------|
| Managed retries | Human-free but adds latency | Cap total time; use `execution_timeout` |
| Checkpointing every 10s vs 10min | Write amplification vs recovery time | Tune to your RTO |
| Kafka transactions | Higher latency (commit overhead) | Batch size matters; keep transactions short |
| `MERGE` vs `INSERT` | Shuffle + write amplification | Cheaper than a wrong result |
| Staging + swap | 2× write cost | Buys atomicity and rollback |
| Synchronous replication (`acks=all`) | Higher latency | Necessary for durability guarantees |

**RTO/RPO thinking:** decide explicitly how long you can be down (RTO) and how much data you can lose
(RPO). Checkpoint frequency and replication factor are *purchases* you make with latency and cost.

---

## 9. Common mistakes and how to avoid them

1. **Retrying non-idempotent work.** *Fix:* make writes keyed/upserts before enabling retries.
2. **Treating "the job succeeded" as "the data is correct".** *Fix:* add validation and freshness
   checks; downstream tests.
3. **Catching exceptions broadly and continuing.** *Fix:* only catch what you can handle; let fatal
   errors propagate.
4. **A DLQ without replay.** *Fix:* build the replay tool with the DLQ, in the same PR.
5. **Unbounded retries.** *Fix:* cap attempts, alert on exhaustion, and fail the *run* clearly.
6. **No `execution_timeout`.** *Fix:* a task without a timeout can hold a worker for days.
7. **Committing offsets prematurely.** *Fix:* commit after the sink is durable (or in the same txn).
8. **Sharing checkpoints between environments.** *Fix:* one checkpoint directory per job+environment.
9. **Assuming object storage is atomic per partition.** *Fix:* use a table format; write to staging and
   swap.
10. **Not measuring the failure rate.** *Fix:* track retry counts, DLQ rates, and MTTR like any SLO.
11. **Ignoring alert fatigue.** *Fix:* alerts on SLOs/actionable conditions, not on every task error.
12. **No chaos drills.** *Fix:* schedule failure injection; the recovery path is untested code until
    you use it.
13. **Conflating "processing" with "effect".** *Fix:* think in terms of *effects on the output*, not
    messages processed.

---

## 10. Exercises / interview questions

1. A Kafka→Postgres pipeline uses `enable_auto_commit=true`. Explain precisely which records you can
   lose and which you can duplicate. How do you fix it without transactions? *(Hint: commit after
   upsert; make the upsert keyed.)*
2. Your Spark job uses `mode("append")` and failed at 80%. What is the state of the output? How do you
   make the retry safe? *(Hint: append-only landing + dedupe; or overwrite the partition.)*
3. Design an end-to-end exactly-once pipeline from S3 to Snowflake via Spark. Where is the boundary,
   and what happens with a crash 1 second before the Snowflake commit? *(Hint: the boundary is the
   warehouse statement; use staging + `MERGE` with a batch id.)*
4. You see 0.1% of messages in the DLQ with `NullPointerException`. Do you halt the pipeline or let it
   continue? What is your decision rule and what data do you need? *(Hint: threshold, business impact,
   time pressure.)*
5. Explain Flink's checkpoint barriers and why aligning them is necessary for a consistent snapshot.
   *(Hint: without alignment you get a cut that mixes pre- and post-failure state.)*
6. What is a fencing token and why is it needed in a pipeline with a standby writer?
7. Compare `acks=1` and `acks=all` in Kafka in terms of RPO and latency. When would `acks=1` be
   defensible? *(Hint: derived/recomputable data.)*
8. Your pipeline must tolerate the source being down for 8 hours without data loss. Where does the
   buffer live, and what is its bound? *(Hint: Kafka retention/key-based retention, not consumer RAM.)*
9. How do you make a *streaming* job safe to upgrade? *(Hint: savepoints, state schema evolution,
   versioned checkpoint dirs, WAP.)*
10. Write the runbook outline for "the transform task failed with OOM for interval X".
11. What is the difference between a *fault*, an *error*, and a *failure*? Give a data-pipeline example
    of each.
12. Your pipeline is "exactly-once" but it also sends a confirmation email. What is the guarantee for
    the email, and how do you make it exactly-once? *(Hint: idempotency key + provider dedupe window.)*

---

## References

- Apache Kafka — Exactly-once semantics and transactions: https://kafka.apache.org/documentation/#semantics
- Apache Kafka — Idempotent producer & transactional API design (KIP-98): https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging
- Confluent — Transactions in Apache Kafka: https://developer.confluent.io/courses/architecture/transactions/
- Apache Spark — Fault tolerance & checkpointing (Structured Streaming): https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
- Apache Flink — Checkpointing & exactly-once: https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/fault-tolerance/checkpointing/
- Apache Flink — Savepoints: https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/savepoints/
- Delta Lake — Concurrency control & ACID guarantees: https://docs.delta.io/concurrency-control/
- Databricks — What are ACID guarantees: https://docs.databricks.com/aws/en/lakehouse/acid
- Apache Iceberg — Reliability / atomic commits: https://iceberg.apache.org/docs/latest/reliability/
- AWS — Retry with backoff pattern: https://docs.aws.amazon.com/prescriptive-guidance/latest/cloud-design-patterns/retry-backoff.html
- AWS — SQS dead-letter queues: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html
- AWS — Designing resilient SQS + Lambda pipelines: https://builder.aws.com/content/3KV2wkCAoH1PjjP6g4ywzIWXD73/designing-resilient-sqs-and-lambda-pipelines-retries-partial-failures-and-idempotency
- Airflow — Task retries & timeouts: https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/tasks.html
- Google Cloud — Dataflow exactly-once / fault tolerance: https://cloud.google.com/dataflow/docs/concepts/streaming-pipelines
