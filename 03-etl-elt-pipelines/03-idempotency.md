# Idempotency: Making Pipelines Safe to Re-Run

> Idempotency is the property that makes everything else in this chapter possible. Orchestrators
> retry. Backfills re-run history. Streams replay after a crash. If a task produces a different
> result each time it runs — or appends a second copy — then every retry, backfill, and recovery
> operation *corrupts your data*. Idempotency is not a nice-to-have; it is the precondition for
> fault tolerance ([02-fault-tolerance.md](./02-fault-tolerance.md)) and recoverability
> ([05-recoverability.md](./05-recoverability.md)).

---

## TL;DR

- **Idempotent = run it N times, get the same final state as running it once.** Not "the same number of
  operations" — the same *state*.
- **The three pillars:** (1) a **deterministic key** per record/partition, (2) a **deterministic
  transform** (no `now()`, no random without seeding, no "latest"), and (3) an **idempotent write**
  (upsert/MERGE, partition overwrite, or dedupe-on-read).
- **Never `INSERT` in a retried task — use `UPSERT`/`MERGE`.** This is the single most violated rule in
  data engineering (it is literally in Airflow's best-practices doc).
- **Partition-scoped overwrite is the cheapest form of idempotency** for fact tables: recompute a whole
  `dt=` partition and replace it atomically.
- **Idempotency keys** are the mechanism for external side effects (payments, emails, APIs) that you
  cannot wrap in a transaction.
- **Deterministic transforms**: hash-based surrogate keys, seeded random, stable sort, no
  environment-dependent behavior.
- **Watermarking by run date** gives you a *stable identity for the interval*, which is what makes
  backfills reproducible.
- **Exactly-once effects = at-least-once delivery + idempotent sink.** You rarely need true exactly-once
  delivery; you almost always need idempotent writes.
- **Test it:** run the task twice and assert the row count, checksum, and key uniqueness are unchanged.

---

## 1. Definitions and core vocabulary

| Term | Definition |
|------|-----------|
| **Idempotence** | `f(f(x)) = f(x)`. Applying the operation twice ≡ once. |
| **Idempotency key** | A unique identifier attached to an operation so duplicates can be detected/rejected. |
| **Upsert** | Insert-or-update by key. The canonical idempotent write. |
| **MERGE** | SQL `MERGE INTO … WHEN MATCHED … WHEN NOT MATCHED …`. The upsert implementation. |
| **Deduplication** | Removing duplicate rows by a key (at read time or write time). |
| **Deterministic transform** | Output depends only on inputs and config, not on time, order, or environment. |
| **Surrogate key** | A system-generated key. Hash-based (`md5(a||b)`) is deterministic and stable. |
| **Natural key / business key** | The source's own identifier (e.g., `order_id`) — the best dedup key when stable. |
| **Grain** | What one row represents. Unique key must be unique *at the grain*. |
| **Tombstone / deletes** | A record signaling deletion (needed for CDC + MERGE correctness). |
| **Write-audit-publish (WAP)** | Write to staging, validate, then atomically publish. |
| **Run-date watermark** | Scoping a run to the interval it represents (`data_interval_start`). |
| **High-watermark** | `max(updated_at)` used to bound incremental reads (with lookback). |
| **Late / out-of-order data** | Events arriving after their window has closed. |
| **Reconciliation** | Comparing source and target counts/checksums to prove correctness. |

---

## 2. What problem it solves / why it matters

### 2.1 Why retries force idempotency

Every production pipeline has *many* re-run triggers:

| Trigger | Frequency |
|---------|-----------|
| Task retry after transient failure | daily |
| Manual re-run after a data bug | weekly |
| Backfill after a logic change | monthly |
| Stream replay after a crash/rebalance | weekly |
| Duplicate message delivered by the broker (at-least-once) | constantly |
| Overlapping runs (catchup + manual trigger) | occasionally |
| Source re-sends a file (SFTP/S3 re-upload) | occasionally |

If any of these can produce duplicates or partial updates, your pipeline is fragile by construction.

### 2.2 The cost of non-idempotency

| Failure | Business impact |
|---------|-----------------|
| Duplicate orders in a revenue report | Overstated revenue, wrong decisions |
| Duplicate charges | Customer complaints, refunds, regulatory risk |
| Duplicate rows in a dimension | Fan-out in joins → inflated facts everywhere downstream |
| Partial update (some rows updated, some not) | Inconsistent state, breaks invariants |
| Non-reproducible backfill | "Why does last quarter's number keep changing?" |
| Idempotency assumed but not verified | Silent corruption discovered months later |

> **Senior note:** Idempotency is also a *devex* property. When tasks are idempotent, engineers can
> re-run anything confidently, incident response becomes mechanical, and the whole team moves faster.
> When they are not, every re-run is a change-control event.

---

## 3. How it works internally

### 3.1 The three pillars

```text
                 ┌───────────────────────────┐
   Same INPUT ──▶│ 1. DETERMINISTIC KEY       │
                 │    (identity of the row)   │
                 └─────────────┬─────────────┘
                               │
                 ┌─────────────▼─────────────┐
                 │ 2. DETERMINISTIC TRANSFORM │  no now(), no unseeded rand(),
                 │    f(input, config)        │  no read-the-latest, stable order
                 └─────────────┬─────────────┘
                               │
                 ┌─────────────▼─────────────┐
                 │ 3. IDEMPOTENT WRITE        │  MERGE / overwrite-partition /
                 │    state = g(state, delta) │  dedupe-on-read / append+dedupe
                 └───────────────────────────┘
```

Break any pillar and you break idempotency:

- No key → you cannot tell a duplicate from a genuine new record.
- Non-deterministic transform → re-running produces different values, so "replace by key" flips data
  back and forth.
- Non-idempotent write → the write itself duplicates or corrupts.

### 3.2 Why `INSERT` breaks idempotency and `MERGE` fixes it

```text
State: empty. Task runs, inserts rows 1,2,3. ✅ 3 rows.
Task retried (network error after commit), inserts rows 1,2,3 again. ❌ 6 rows.
```

```text
State: empty. Task runs, MERGEs rows 1,2,3. ✅ 3 rows.
Task retried, MERGEs rows 1,2,3 → matched on key, updates in place. ✅ 3 rows.
```

`MERGE` is idempotent *if and only if*:
1. The `ON` clause keys are **unique in the source** (dedupe first, usually with `row_number()`).
2. The keys are **unique in the target** (grain invariant).
3. The `WHEN MATCHED` update is **deterministic** (same source row → same target row values).

### 3.3 Internals: how table formats make writes atomic

Delta Lake: writes go to a `_delta_log/00000000000000000N.json` commit file. Readers reconstruct the
table from the log. A commit either exists (whole partition visible) or doesn't. This is what makes
`replaceWhere` an *atomic partition swap* rather than a sequence of file writes.

Iceberg: each write produces a new **snapshot** with manifests; the catalog swap is atomic. Branches
and tags give you cheap staging (`audit` branch) and the ability to `FAST FORWARD` `main` only after
validation — the WAP pattern.

Snowflake/BigQuery: `MERGE`/DML statements are atomic per statement, but a multi-statement
`DELETE` + `INSERT` is **not** atomic unless wrapped in an explicit transaction (Snowflake
`BEGIN … COMMIT`; BigQuery multi-statement transactions).

> **Senior note:** Know your sink's *atomic unit*. Object storage = per object. Warehouse statement =
> per statement. Delta/Iceberg = per commit. Kafka = per transaction. Everything else is a
> best-effort sequence.

### 3.4 Deduplication internals

Two places to dedupe:

- **Write-time (preferred for correctness):** dedupe the source before the MERGE so the merge is
  `1 source row : 1 target row`. Use `ROW_NUMBER() OVER (PARTITION BY key ORDER BY updated_at DESC)`.
- **Read-time (fallback):** `QUALIFY row_number() over (partition by key order by ts desc) = 1` in
  every consumer. Convenient, but pushes the cost and the risk onto every reader.

**Late-arriving duplicates** are the hard case: a record for yesterday arrives today. If your
incremental filter is `updated_at >= max(updated_at)`, you *miss* it. This is why incremental loads
need a **lookback window** (re-read the last N days) plus a MERGE that resolves ties by `updated_at`.

---

## 4. How to implement it

### 4.1 The canonical Airflow + SQL pattern

```sql
-- Idempotent partition recompute (best for fact tables)
BEGIN;
DELETE FROM analytics.fct_orders
 WHERE order_date = DATE '{{ data_interval_start | ds }}';

INSERT INTO analytics.fct_orders
SELECT o.order_id, o.customer_id, o.order_total, o.order_date
  FROM raw.orders o
 WHERE o.order_date = DATE '{{ data_interval_start | ds }}';
COMMIT;
```

Re-running this transaction for the same interval yields exactly the same state. Safe for retries and
backfills, and cheap because it only touches one partition.

### 4.2 Deduped MERGE (the workhorse)

```sql
-- Source may contain duplicates (at-least-once delivery, CDC bursts).
-- Step 1: collapse to one row per key, deterministically.
WITH src AS (
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY order_id
           ORDER BY updated_at DESC, _ingested_at DESC   -- deterministic tiebreak
         ) AS rn
  FROM staging.orders_delta
)
MERGE INTO raw.orders AS t
USING (SELECT * FROM src WHERE rn = 1) AS s
   ON t.order_id = s.order_id
WHEN MATCHED AND s.updated_at > t.updated_at THEN UPDATE SET
     t.customer_id = s.customer_id,
     t.order_total = s.order_total,
     t.updated_at  = s.updated_at,
     t.deleted     = s.deleted
WHEN NOT MATCHED THEN INSERT
     (order_id, customer_id, order_total, updated_at, deleted)
VALUES (s.order_id, s.customer_id, s.order_total, s.updated_at, s.deleted);
```

Key details:
- The `WHEN MATCHED … AND s.updated_at > t.updated_at` guard makes the update **monotonic** — re-running
  an old batch cannot overwrite newer data.
- The deterministic tiebreak in `ROW_NUMBER()` ensures the same winner on every run.
- Deletes are handled by a `deleted` flag (soft delete) or `WHEN MATCHED THEN DELETE`.

### 4.3 PySpark — Delta `replaceWhere` (atomic partition overwrite)

```python
from delta.tables import DeltaTable

def write_orders_partition(spark, day: str, df):
    """Idempotent: replaces exactly one partition atomically."""
    (df.write
       .format("delta")
       .mode("overwrite")
       .option("replaceWhere", f"order_date = '{day}'")   # atomic partition swap
       .saveAsTable("analytics.fct_orders"))

# Re-running with the same `day` produces identical state — safe under any retry.
```

For row-level merges:

```python
target = DeltaTable.forName(spark, "analytics.dim_customer")
source = (spark.read.table("staging.customer_delta")
          .dropDuplicates(["customer_id"]))           # dedupe in-source

(target.alias("t")
  .merge(source.alias("s"), "t.customer_id = s.customer_id")
  .whenMatchedUpdate(condition="s.updated_at > t.updated_at", set={
       "name": "s.name", "email": "s.email", "updated_at": "s.updated_at"})
  .whenNotMatchedInsertAll()
  .execute())
```

### 4.4 Deterministic transforms — the rules

```python
# ❌ BAD: environment/time dependent
df.withColumn("load_date", F.current_date())
df.withColumn("id", F.monotonically_increasing_id())      # non-deterministic across runs!
df.withColumn("r", F.rand())                              # unseeded
df.orderBy("ts").limit(10)                                # ties -> unstable

# ✅ GOOD: deterministic given the interval
df.withColumn("order_date", F.lit(day))                   # from data_interval_start
df.withColumn("id", F.md5(F.concat_ws("|", "order_id", "line_no")))  # stable hash key
df.withColumn("r", F.rand(seed=42))                       # seeded
df.orderBy("ts", "order_id").limit(10)                    # deterministic tiebreak
```

Deterministic surrogate keys:

```sql
-- Stable across re-runs and warehouses; no sequences, no autoincrement
MD5(CONCAT_WS('|', COALESCE(order_id,''), COALESCE(line_no::text,''))) AS order_line_sk
```

> **Senior note:** `monotonically_increasing_id()` and database `SEQUENCE`/`IDENTITY` columns are
> *not* deterministic. If you join or update by them, a re-run produces different keys and you get
> duplicates. Always derive keys from data, not from execution order.

### 4.5 Idempotency keys for external side effects

```python
import hashlib, requests

def make_idempotency_key(order_id: str, event_type: str) -> str:
    return hashlib.sha256(f"{order_id}:{event_type}".encode()).hexdigest()

def charge_customer(order_id: str, amount_cents: int):
    key = make_idempotency_key(order_id, "charge")
    r = requests.post(
        f"{PAYMENTS_URL}/charges",
        json={"amount": amount_cents, "currency": "usd"},
        headers={"Idempotency-Key": key},        # provider dedupes on this
        timeout=30,
    )
    r.raise_for_status()
    return r.json()

# Belt-and-braces: record the key in your DB with a UNIQUE constraint BEFORE calling out,
# and use the same key on retry. Now retries can never double-charge.
```

```sql
-- The outbox/inbox table that makes the side effect at-most-once-safe
CREATE TABLE side_effects (
  idempotency_key STRING NOT NULL,
  effect_type     STRING NOT NULL,
  order_id        STRING NOT NULL,
  amount_cents    BIGINT,
  status          STRING NOT NULL,   -- 'pending' | 'succeeded' | 'failed'
  request_hash    STRING NOT NULL,   -- detect conflicting retries with same key
  created_at      TIMESTAMP NOT NULL,
  updated_at      TIMESTAMP NOT NULL,
  PRIMARY KEY (idempotency_key)
);
```

If a retry arrives with the same key but a *different* `request_hash`, that is a **conflict** — fail
loudly rather than silently using the old result.

### 4.6 Watermarking by run date

The interval is *the* identity of the work. Everything must be scoped to it:

```python
@task
def transform(interval_start: str, interval_end: str):
    # Deterministic: same interval -> same output, forever
    run_filter = f"event_date >= '{interval_start[:10]}' AND event_date < '{interval_end[:10]}'"
    ...
```

```yaml
# dbt: prefer a stable partition key over "max(ts)" when possible
# (max(ts) is a moving target; a date partition is stable)
```

```sql
-- Deterministic windowing of late data: assign late events to their EVENT day,
-- then reprocess that day's partition — not "today's".
```

Why this matters: if your pipeline is a pure function of `(interval, config)`, then backfilling
`2026-01-15` six months later produces the *same* output as it did on the day — provided the raw zone
is immutable and the transform is deterministic. That is **reproducibility**, and it is what makes
audits and "why did this number change?" answerable.

### 4.7 Verify idempotency (a test, not a hope)

```python
def test_task_is_idempotent(spark, tmp_warehouse):
    """The single most valuable test in a data pipeline."""
    interval = ("2026-01-15", "2026-01-16")
    run_transform(interval)                      # first run
    snapshot_1 = table_checksum("analytics.fct_orders", "order_date = '2026-01-15'")
    n1 = count_rows("analytics.fct_orders", "order_date = '2026-01-15'")

    run_transform(interval)                      # re-run the SAME interval
    snapshot_2 = table_checksum("analytics.fct_orders", "order_date = '2026-01-15'")
    n2 = count_rows("analytics.fct_orders", "order_date = '2026-01-15'")

    assert n1 == n2, "row count changed on re-run -> not idempotent"
    assert snapshot_1 == snapshot_2, "content changed on re-run -> non-deterministic"
```

```sql
-- Cheap checksum in SQL
SELECT COUNT(*) AS n, SUM(order_total) AS total, COUNT(DISTINCT order_id) AS distinct_keys,
       MD5(STRING_AGG(CONCAT(order_id, '|', order_total), ',' ORDER BY order_id)) AS checksum
FROM analytics.fct_orders WHERE order_date = DATE '2026-01-15';
```

SQL Server offers `CHECKSUM_AGG`; the portable approach is `md5(string_agg(... order by ...))`.

---

## 5. Where to implement it (tools and when each fits)

| Need | Tool / feature | Notes |
|------|----------------|-------|
| Upsert on a warehouse | `MERGE` (Snowflake, BigQuery, Redshift, Postgres 15+, Spark) | Dedupe the source first |
| Upsert on a lake | Delta `merge`, Iceberg `MERGE` (Spark/Flink) | Row-level; watch file churn |
| Whole-partition swap | Delta `replaceWhere`, Iceberg `overwritePartitions`, Hive `INSERT OVERWRITE` | Cheapest idempotency for facts |
| Dedupe on read | `QUALIFY row_number()…`, `SELECT DISTINCT ON` | Fallback; costs every reader |
| Deterministic keys | `md5`/`sha` hash of natural keys, `dbt_utils.generate_surrogate_key` | Never sequences |
| Idempotent streaming | Kafka txn + `read_committed`; Flink 2PC into Delta/Iceberg | Boundary-limited |
| External side effects | Idempotency keys + outbox/inbox table | Provider support varies |
| dbt incremental | `unique_key`, `incremental_strategy`, `merge_update_columns` | See below |
| Tests | dbt `unique`/`not_null` tests, `dbt_utils.unique_combination_of_columns` | Encode the grain invariant |

**dbt specifics:**

```sql
{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='merge',           -- merge | delete+insert | insert_overwrite | append
    merge_update_columns=['status','updated_at'],
    on_schema_change='sync_all_columns'
) }}
```

| `incremental_strategy` | Idempotent? | When |
|------------------------|-------------|------|
| `merge` | Yes (if `unique_key` is unique) | Dimension/fact upserts |
| `delete+insert` | Yes | Partition/date-scoped updates; warehouse-optimal |
| `insert_overwrite` (partition-based) | Yes | BigQuery/Spark partitioned `insert_overwrite` |
| `append` | **No** (by itself) | Immutable event logs only, with downstream dedupe |

**When each fits:** use `merge` for record-level truth, `replaceWhere`/`insert_overwrite` for
partition-level recompute, and `append` only where duplicates are genuinely harmless or explicitly
handled downstream.

---

## 6. Patterns and anti-patterns

### 6.1 Patterns

| Pattern | Idempotency mechanism |
|---------|-----------------------|
| **Partition-scoped recompute** | DELETE/INSERT or overwrite by `dt` |
| **MERGE with deduped source** | Key-based upsert |
| **Atomic partition swap (replaceWhere)** | Whole-partition replacement |
| **Staging + WAP** | Write → validate → publish atomically |
| **Idempotency key + unique constraint** | Side-effect dedupe |
| **Outbox/inbox** | Exactly-once *effects* across a boundary |
| **Hash surrogate keys** | Deterministic identity |
| **Lookback window + monotonic merge guard** | Late-data correctness with idempotency |
| **Batch/run id in target** (`_batch_id`) | Detect and remove partial writes |
| **Dedupe-on-write** | Clean source before merge |
| **Reconciliation job** | Proves the invariant holds |

### 6.2 Anti-patterns

| Anti-pattern | Consequence |
|--------------|-------------|
| **`INSERT` in a retried task** | Duplicate rows |
| **`mode("append")` for a re-runnable task** | Duplicate partitions |
| **`mode("overwrite")` on the whole table** | Wipes unrelated partitions |
| **`CURRENT_TIMESTAMP`/`now()` in the transform** | Different output on re-run |
| **`monotonically_increasing_id()` as a persisted key** | New keys on re-run → duplicates |
| **Unseeded `rand()`** | Different sampling each run |
| **Non-deterministic ordering / `LIMIT` without tiebreak** | Different rows chosen |
| **`max(ts)` as the watermark without lookback** | Misses late data; not reproducible |
| **Non-unique merge key** | MERGE cardinality error or silent fan-out |
| **Nullable merge key columns** | `NULL != NULL` → unmatched → duplicates |
| **Assuming exactly-once delivery** | Duplicates appear anyway |
| **Side effects without idempotency keys** | Double charges/emails |
| **Testing only the first run** | The bug appears only on re-run |
| **"We'll dedupe downstream"** | Cost and risk spread to every consumer |

---

## 7. Real-world use cases

**Payments platform.** Every charge carries an idempotency key derived from `(order_id, attempt_id)`.
The ledger is a `MERGE` on `payment_id`; the outbox table guarantees that a retry never double-charges.
The daily reconciliation compares the ledger to the processor's settlement file.

**Subscription billing (SCD2).** A customer's plan changes. The SCD2 merge checks that the new
`valid_from` is later than the current one, closing the previous row and opening a new one
atomically. Re-running the same ETL date is a no-op — critical because billing runs are re-executed
frequently.

**CDC from Postgres to a warehouse.** Debezium emits insert/update/delete with `op` and
`source.lsn`. The sink dedupes on `(pk, lsn)` and applies the *latest* LSN per key, making the pipeline
idempotent even with at-least-once delivery and consumer rebalances.

**Web analytics sessionization.** Session IDs are deterministically derived from
`(user_id, session_start_bucket)`. Recomputing a day's sessions reproduces the identical assignment —
which matters because downstream funnels depend on it.

**Inventory sync from multiple suppliers.** Late supplier feeds re-send yesterday's stock levels. The
merge guard `s.updated_at > t.updated_at` prevents a stale feed from clobbering today's numbers.

**ML feature pipelines.** Features must be reproducible for training/serving parity. Point-in-time
feature writes are keyed by `(entity_id, feature_ts)`; a replay of the same logical time produces
identical feature values.

**Airline/hotel booking ingestion.** Supplier APIs paginate and sometimes re-deliver. Bookings are
merged on `(supplier, booking_ref)`; the same feed ingested twice changes nothing.

---

## 8. Senior-level deep dive

### 8.1 Idempotency vs. exactly-once vs. determinism — the relationship

```text
Determinism        → same input gives same output (enables reproducibility)
    + Idempotent write → re-running is harmless (enables retries/backfills)
        + At-least-once delivery → no data loss
            = Exactly-once EFFECTS on the output (what people actually want)
```

You almost never need exactly-once *delivery*. You need exactly-once *effects*, which is cheaper and
more robust because it survives failures the delivery layer can't see.

### 8.2 Choosing the idempotency strategy

```text
What is the natural unit of recompute?
├─ A time interval (day/hour) → partition overwrite / delete+insert by partition  ← start here
├─ A record (row key)         → MERGE/upsert on unique key
├─ An immutable event         → append + dedupe-on-read (Kafka compacted topic / dedupe view)
└─ An external side effect    → idempotency key + unique constraint + outbox
```

| Strategy | Cost per run | Complexity | Best for |
|----------|--------------|-----------|----------|
| Partition overwrite | O(partition) | Low | Facts, daily snapshots |
| MERGE (row) | O(changes + shuffle) | Medium | Dimensions, CDC, mutable rows |
| Delete+insert (interval) | O(affected) | Low | Date-partitioned tables |
| Append + dedupe-on-read | O(append) | Low write / high read | High-volume events |
| Idempotency key | O(1) per effect | Medium | External APIs, payments |
| Full table rebuild | O(table) | Low | Small tables; logic changes |

> **Senior note:** The *cheapest* correct idempotency is usually **partition overwrite**. Push the
> grain of recompute up to the partition whenever the business logic allows it. Row-level MERGE is
> more precise but more expensive (shuffle, file churn, small-file problems).

### 8.3 The late-data problem, precisely

```text
Event time:     Jan 15         Jan 16
Processing:     ─── run A ──────── run B ────
Arrival:                ▲ Jan 15 event arrives (late) during run B
```

If run B filters `event_date = '2026-01-16'`, the late event is dropped → **permanent data loss**.
If run B filters `event_date >= '2026-01-15'`, it recomputes Jan 15 → correct, and idempotent if it
uses partition overwrite. The general rule:

> **Recompute the *affected* partitions, not just the *current* one, and make the recompute idempotent.**

In streaming, watermarks decide when a window is "closed" and late events are either (a) allowed and
trigger a re-emit, (b) dropped, or (c) side-output to a late-data stream. Choose explicitly — the
default is usually (b), which is silent loss.

### 8.4 Failure modes of MERGE

| Failure | Cause | Symptom | Fix |
|---------|-------|---------|-----|
| Duplicate rows after merge | Non-unique source key | Row count grows each run | `row_number()` dedupe first |
| Rows silently not updated | `NULL` in join key | `NULL != NULL` | `COALESCE` keys or exclude nulls |
| Stale overwrite | No `updated_at` guard | Old data replaces new | Add monotonic guard |
| MERGE error | Target has duplicate keys | `MERGE` cardinality violation | Dedupe target; enforce `unique` test |
| Perf regression | Merge over huge target, no pruning | Hours per run | Partition pruning, `mergeSchema`, cluster keys |
| Deleting too much | `WHEN MATCHED THEN DELETE` too broad | Data vanishes | Explicit tombstone semantics |

### 8.5 Idempotency under concurrency

Two runs of the same interval **at the same time** are the hardest case:

- Orchestrator-level: `max_active_runs=1`, or a DB-level lock/lease on `(pipeline, interval)`.
- Sink-level: rely on the transactional commit — the second commit overwrites the first, which is fine
  if both computed the same thing (deterministic) and bad if they didn't.
- Guard with a **run guard table**:

```sql
-- Only one winner per (pipeline, interval)
INSERT INTO pipeline_run_guard (pipeline, interval_start, run_id, status, started_at)
VALUES (:pipeline, :interval_start, :run_id, 'running', CURRENT_TIMESTAMP);
-- Fails with a unique-constraint violation if another run holds the interval.
```

Combine a guard (mutual exclusion) with idempotent writes (harmless overlap). Together they cover both
the accidental and the deliberate duplicate run.

### 8.6 Performance: the cost of idempotency

| Technique | Extra cost | Mitigation |
|-----------|-----------|-----------|
| MERGE | Shuffle + rewrite of matched files | Partition pruning, `merge_update_columns`, compaction |
| Delete+insert interval | Two scans | Prune aggressively; clustered keys |
| Full-partition overwrite | Rewrites whole partition | Accept it — it *is* the unit of recompute |
| Dedupe on read | Every reader pays | Materialize a deduped table instead |
| Idempotency key check | An extra round trip per effect | Batch checks; unique index lookup is cheap |
| WAP (staging + swap) | 2× write | Only for high-stakes tables |

> **Senior note:** Delta/Iceberg `MERGE` on a large target can rewrite many small files. Regular
> `OPTIMIZE`/compaction and Z-order/clustering are not optional once you rely on MERGE in production —
> otherwise your idempotency mechanism becomes your performance problem.

---

## 9. Common mistakes and how to avoid them

1. **Retrying tasks that `INSERT`.** *Fix:* switch to MERGE or partition overwrite before enabling
   retries.
2. **`mode("overwrite")` without `replaceWhere`.** *Fix:* always scope the overwrite; whole-table
   overwrite should be an explicit, rare operation.
3. **Using `now()`/`CURRENT_DATE` inside the transform.** *Fix:* derive everything from the interval.
4. **Non-deterministic keys (`monotonically_increasing_id`, sequences).** *Fix:* hash natural keys.
5. **Unseeded randomness or unstable ordering.** *Fix:* seed and add deterministic tiebreakers.
6. **MERGE with a non-unique source key.** *Fix:* dedupe first; test uniqueness.
7. **Nullable merge keys.** *Fix:* `COALESCE` to a sentinel, or exclude `NULL` keys and route them to a
   DLQ.
8. **No `updated_at` guard on MERGE.** *Fix:* make updates monotonic.
9. **Assuming "at-least-once" only affects the broker.** *Fix:* every hop (extract, transform, load,
   side effect) has its own duplicate risk.
10. **"We'll dedupe downstream."** *Fix:* fix it at the source of truth; downstream dedupe is a tax
    every consumer pays forever.
11. **Never testing the second run.** *Fix:* make "run twice, compare checksums" a CI test.
12. **Ignoring late data.** *Fix:* lookback windows + recompute affected partitions.
13. **No reconciliation.** *Fix:* a scheduled source↔target row-count/checksum comparison catches
    idempotency bugs that tests miss.
14. **Confusing idempotency with "deduplication".** *Fix:* dedup is a *mechanism*; idempotency is the
    *property* you're buying.

---

## 10. Exercises / interview questions

1. Prove formally that `DELETE WHERE dt=X; INSERT … WHERE dt=X` is idempotent but
   `INSERT … WHERE dt=X` is not. What about `MERGE` when the source has two rows with the same key?
2. Your task is retried after a commit timeout — the commit actually succeeded. Walk through what
   happens for (a) `INSERT`, (b) `MERGE`, (c) `replaceWhere`. *(Hint: the ambiguous-commit problem.)*
3. A `MERGE` runs for 3 hours on a 500 GB target and rewrites everything. Give three fixes and their
   trade-offs. *(Hint: partition the target, prune the merge, `merge_update_columns`, compaction.)*
4. Why is `monotonically_increasing_id()` dangerous as a persisted key? Construct a concrete failing
   scenario. *(Hint: two runs of the same data.)*
5. Design idempotency for a pipeline that posts messages to Slack. *(Hint: deterministic hash key +
   a sent_events table with a unique constraint.)*
6. Explain why "exactly-once delivery" is often the wrong goal, and what to ask for instead.
7. Your incremental dbt model uses `where updated_at >= (select max(updated_at) from {{ this }})`.
   Enumerate every way this loses or duplicates data, and rewrite it. *(Hint: late data, ties,
   deletes, clock skew, partial commits.)*
8. How do you make a **streaming** job idempotent into a non-transactional sink (e.g., Elasticsearch)?
   *(Hint: deterministic doc IDs + upsert semantics.)*
9. You must reprocess the last 90 days after a logic fix — without changing today's numbers and
   without downtime. Sketch the approach. *(Hint: WAP, shadow table, blue/green swap.)*
10. What is the difference between "idempotent" and "commutative"? Give a pipeline operation that is
    commutative but not idempotent, and one that is idempotent but not commutative. *(Hint: `+= vs =`
    merge semantics.)*
11. Write the SQL for a merge that handles inserts, updates, and deletes from a CDC stream, and prove
    it is idempotent for a replayed batch.
12. How would you *detect* that a pipeline is not idempotent, in production, before anyone notices the
    bad numbers?

---

## References

- Apache Airflow — Best Practices (UPSERT vs INSERT, partition reads): https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html
- Databricks — Upsert into a Delta Lake table using MERGE: https://docs.databricks.com/aws/en/delta/merge
- Delta Lake — Table deletes, updates, and merges: https://docs.delta.io/latest/delta-update.html
- Delta Lake — `replaceWhere` (selective overwrite): https://docs.delta.io/latest/delta-batch.html#-selective-overwrite
- Apache Iceberg — Row-level deletes / MERGE: https://iceberg.apache.org/docs/latest/spark-writes/
- dbt — `unique_key` reference: https://docs.getdbt.com/reference/resource-configs/unique_key
- dbt — Incremental strategies: https://docs.getdbt.com/docs/build/incremental-strategy
- dbt — `generate_surrogate_key` (dbt-utils): https://github.com/dbt-labs/dbt-utils#generate_surrogate_key-source
- Snowflake — MERGE documentation: https://docs.snowflake.com/en/sql-reference/sql/merge
- BigQuery — MERGE (DML) reference: https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#merge_statement
- PostgreSQL — `INSERT … ON CONFLICT` (upsert): https://www.postgresql.org/docs/current/sql-insert.html
- Apache Spark — Structured Streaming exactly-once / idempotent sinks: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
- Stripe — Idempotent requests: https://docs.stripe.com/api/idempotent_requests
- AWS — Idempotent consumer pattern / SQS FIFO deduplication: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-queues-exactly-once-processing.html
- Martin Fowler — Idempotent receiver / transactional outbox: https://martinfowler.com/articles/patterns-of-distributed-systems/idempotent-receiver.html
