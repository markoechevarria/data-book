# Recoverability: Backfill, Replay, Atomic Swaps, and Disaster Recovery

> Fault tolerance ([02-fault-tolerance.md](./02-fault-tolerance.md)) keeps a *running* pipeline from
> falling over. Recoverability is the harder promise: after the failure, after the bad deploy, after
> the source bug — **can you put the data back the way it should have been?** A senior engineer designs
> for recovery *before* the first incident: immutable raw inputs, interval-scoped idempotent
> recomputation, lineage to compute blast radius, and an atomic publish step so consumers never see a
> half-fixed state.

---

## TL;DR

- **Recovery is a planned capability, not an improvisation.** If you cannot describe the recovery
  procedure for a pipeline, it has none.
- **The raw/landing zone is your recovery insurance.** Immutable, append-only, retained long enough to
  cover your worst-case reprocessing horizon.
- **Recovery = recompute a bounded set of intervals.** Everything must be a pure function of the
  interval, so recomputation is deterministic and idempotent. See [03-idempotency.md](./03-idempotency.md).
- **Backfill is a first-class operation**, not a code change. Airflow `--start-date/--end-date`,
  Dagster partition backfills, dbt `--full-refresh` on a selection.
- **Lineage computes the blast radius**: from the broken dataset, everything downstream is suspect and
  must be reprocessed in dependency order.
- **Write-Audit-Publish / blue-green swaps** let you rebuild a corrected version and publish it
  atomically, with instant rollback.
- **Recovery-point (RPO) and recovery-time (RTO) objectives** are the two numbers that define what you
  must build. Without them, "disaster recovery" is a wish.
- **Replaying is not re-running.** A replay reproduces the *original* inputs; a re-run re-executes
  against *current* inputs. Know which one you need.
- **Test recovery.** A backfill you've never run is a backfill that will fail when you need it.

---

## 1. Definitions and core vocabulary

| Term | Definition |
|------|-----------|
| **Recovery** | Restoring the system/data to a correct state after a failure. |
| **Backfill** | Running (or re-running) historical intervals. |
| **Reprocessing** | Recomputing data, usually after a logic or data fix. |
| **Replay** | Re-reading the original input stream/files and re-deriving outputs *as of a point in time*. |
| **Rerun** | Re-executing a task against current inputs (may differ from the original run). |
| **Idempotent replay** | Replay that produces the exact same output as the original run. |
| **Blast radius** | The set of downstream datasets/dashboards affected by a failure. |
| **Lineage-driven recovery** | Using lineage to enumerate and order what must be reprocessed. |
| **Write-Audit-Publish (WAP)** | Write to isolated staging → validate → atomically publish. |
| **Blue/green swap** | Maintain two versions; switch consumers atomically; keep the old for rollback. |
| **Shadow table** | A parallel build of a corrected dataset, used to test before publishing. |
| **Atomic swap** | A metadata-only change that repoints consumers from version A to version B. |
| **Time travel** | Query/branch/rollback to a previous table snapshot (Delta/Iceberg). |
| **RPO** | Recovery Point Objective — how much data loss is acceptable (time). |
| **RTO** | Recovery Time Objective — how long recovery may take. |
| **MTTR** | Mean time to recover. The operational outcome metric. |
| **DR (disaster recovery)** | Restoring after a region/cluster/platform-level failure. |
| **Failover** | Switching to a standby system/region. |
| **Point-in-time restore (PITR)** | Restoring a database/table to an arbitrary past moment. |
| **Compaction** | Rewriting many small files into fewer large ones (also a recovery aid). |
| **Reproducibility** | Same inputs + same code + same config → same outputs, indefinitely. |

---

## 2. What problem it solves / why it matters

### 2.1 The scenarios that require recovery

| Scenario | Frequency | Recovery need |
|----------|-----------|---------------|
| Task failed for one interval | daily | Re-run that interval |
| Bad transform deployed, ran for 3 days | monthly | Replay affected intervals after fix |
| Source data was wrong/unusual (e.g., double-load) | quarterly | Re-extract or reprocess from raw |
| Schema change broke the pipeline | quarterly | Fix + reprocess the affected window |
| Late-arriving data corrected historical numbers | weekly | Recompute affected partitions |
| Partial failure left a corrupt partition | occasionally | Rebuild + atomic swap |
| Cluster/region outage | rare | Failover + catch-up |
| Data was deleted/corrupted by a bad job | rare | Restore from backups/time travel |
| Audit asks for a number from 3 years ago | rare | Reprocess older data reproducibly |

### 2.2 Why it matters

Without recoverability:

- A one-line bug in a transform costs a **manual, error-prone, multi-day** repair — or is never fixed.
- Numbers change silently between reports, destroying trust ("the data team's numbers are always
  different").
- Audits and compliance filings cannot be reproduced.
- Every incident becomes a heroic, ad-hoc effort rather than a procedure.

With recoverability:

- Recovery is a **parameterized command** (`dagster asset backfill --partition-range ...`).
- Incident MTTR drops from days to minutes/hours.
- Reproducibility makes the platform *auditable*.

> **Senior note:** The single most important architectural decision for recoverability is **keeping an
> immutable raw zone**. Everything else (idempotent transforms, lineage, atomic swaps) is an amplifier
> of that decision. If you hardcode an ETL that discards raw data, no amount of engineering can bring
> it back.

---

## 3. How it works internally

### 3.1 The recoverability stack

```text
        ┌───────────────────────────────────────────────────────────────────┐
   L4   │  DISASTER RECOVERY   region failover · backups · PITR · IaC        │
        ├───────────────────────────────────────────────────────────────────┤
   L3   │  ATOMIC PUBLISH      WAP · blue/green · table-format snapshots     │
        ├───────────────────────────────────────────────────────────────────┤
   L2   │  TARGETED REPROCESS   backfill engine · interval identity · lineage │
        ├───────────────────────────────────────────────────────────────────┤
   L1   │  REPLAYABLE INPUTS    immutable raw zone · retained source data     │
        └───────────────────────────────────────────────────────────────────┘
```

Missing L1 makes L2 impossible. Missing L3 makes recovery visible to consumers (downtime). Missing L4
turns a regional outage into data loss.

### 3.2 Why "replay" ≠ "re-run"

```text
Original run (2026-01-15):
   input = raw/orders/dt=2026-01-15 (as landed at 02:00)  ──f──▶  output_v1

Re-run today:
   input = CURRENT raw/orders (which has been updated!)   ──f──▶  output_v2  ← differs!

Replay:
   read raw/orders as-of 2026-01-15 02:00 (time travel)    ──f──▶  output_v1  ← identical
```

If the raw zone is **append-only and immutable**, a re-run *is* a replay (nothing changed). If you
mutate raw data (overwrite, upsert, partial backfills), you need time travel / snapshots to reproduce
the original.

Guarantees to design for:

1. **Immutability:** raw partitions are written once, never modified.
2. **Determinism:** transforms depend only on their declared inputs.
3. **Isolation:** each interval is independent.

Then "re-run interval X" is a *safe, repeatable* operation, and backfills become routine.

### 3.3 Internals: the backfill engine

An orchestrator's backfill is essentially:

```text
for interval in [start, end]:                    # or a set of partitions
    if not already_done(interval) or force:
        create_run(pipeline, interval)           # with a distinct run_id
        # runs execute with data_interval_start/end = interval
        # the transform is a pure function of interval
        # writes are idempotent → overlapping runs are harmless
```

Key internals:

- **Interval-as-identity.** Airflow's `data_interval_start/end`; Dagster's partition key. The interval
  is not a timestamp of the run — it is the *identity of the work*.
- **Reverse-chronological vs chronological.** Backfilling newest-first gets consumers unblocked
  faster; oldest-first is correct for dependencies. Usually you backfill *within one layer* newest
  first, but *across layers* in dependency order.
- **Concurrency limits.** A backfill of 365 days × 50 tasks is 18,250 task instances; run it with
  bounded concurrency (pools) or it will saturate sources and the warehouse.
- **Dry runs.** Always have a `--dry-run` that reports what would run.

### 3.4 Internals: atomic publish

| Mechanism | Atomic? | Rollback? | Notes |
|-----------|---------|-----------|-------|
| Iceberg branch + `FAST FORWARD` | Yes | Yes (swap back) | Best for lakehouse tables |
| Delta `replaceWhere` | Yes (per commit) | Via time travel/`RESTORE` | Atomic partition swap |
| Rename table / swap view | Yes (metadata) | Yes | Blue/green classic |
| `ALTER TABLE … SWAP WITH` (Snowflake) | Yes | Yes | Zero-copy, instant |
| BigQuery table clone / view swap | Yes | Yes | Cheap metadata op |
| Overwrite in place | Risky | No | Consumers see mid-flight state |
| Drop + recreate table | Downtime | No | Loses grants/constraints |

**The pattern (blue/green / WAP):**

```text
1. Build v2 in a shadow location (table X_v2 / branch `audit` / schema `recovery`)
2. Run validations against v2 (row counts, checksums, tests, reconciliation)
3. Atomically repoint consumers from X_v1 to X_v2 (rename/swap/FAST FORWARD)
4. Keep X_v1 for N days for instant rollback
5. Clean up
```

### 3.5 Internals: lineage-driven blast radius

```text
                ┌──────────────┐
   raw.orders ─▶│ stg_orders   │─┬─▶ fct_orders ─┬─▶ mart_revenue ─▶ Dashboard A
                └──────────────┘ │               └─▶ mart_retention ─▶ Dashboard B
                                 └─▶ dim_customer ─▶ (feature store) ─▶ Model Z
```

Given that `raw.orders` was corrupted for `[2026-01-10, 2026-01-14]`, the recovery planner must:

1. Traverse lineage forward from `raw.orders`.
2. Filter to assets whose interval overlaps the corrupt window.
3. Topologically sort them (respect dependencies).
4. Generate a backfill plan for exactly those assets and intervals.
5. Execute with bounded concurrency; validate after each layer.

This is what OpenLineage + Marquez/DataHub enable: a machine-readable graph, so blast radius is
*computed*, not guessed.

---

## 4. How to implement it

### 4.1 Airflow: a parameterized backfill

```bash
# Re-run one logical interval
airflow dags backfill orders_daily \
  --start-date 2026-01-15 --end-date 2026-01-15 \
  --reset-dagruns \
  --max-active-runs 1

# A range, with concurrency bounded by a pool
airflow dags backfill orders_daily \
  --start-date 2026-01-01 --end-date 2026-01-31 \
  --max-active-runs 4
```

(Modern Airflow favors `airflow tasks` + re-running the DAG with a logical date, or `--reprocess-behavior`
/ `--run-backwards` flags depending on version; the semantics are the same: interval-scoped runs.)

```python
# The DAG must be backfill-safe by construction.
with DAG(
    "orders_daily",
    schedule="0 2 * * *",
    catchup=True,
    max_active_runs=1,
    default_args={"retries": 3},
) as dag:
    # Every read/write scoped to the interval; writes are MERGE or partition overwrite.
    ...
```

> **Senior note:** `catchup=True` + a parameter that can be overridden via `dag_run.conf` is the
> cleanest combination: the scheduler fills gaps automatically, and an operator can override the
> window explicitly for a targeted repair.

### 4.2 Dagster: partition-based backfill

```python
import dagster as dg

daily = dg.DailyPartitionsDefinition(start_date="2024-01-01")

@dg.asset(partitions_def=daily)
def raw_orders(context: dg.AssetExecutionContext): ...

@dg.asset(partitions_def=daily, deps=[raw_orders])
def fct_orders(context: dg.AssetExecutionContext): ...

# UI or CLI:
#   dagster asset backfill --select fct_orders --partition-range 2026-01-10...2026-01-14
#   dagster asset materialize --select fct_orders --partition 2026-01-15
```

Dagster records a **materialization event per partition**, so it knows exactly which partitions of
which asset are up to date — the recovery plan falls out of the model.

### 4.3 Replay from an immutable raw zone

```python
def replay_interval(interval_start: str, interval_end: str, version: str = "v2"):
    """Deterministic replay: same inputs -> same outputs."""
    raw = (spark.read
           .format("delta")                        # or Iceberg
           .option("versionAsOf", snapshot_for(interval_start))  # as-of the original run
           .load("s3://raw/orders"))

    out = transform_orders(raw, interval_start, interval_end, logic_version=version)

    # Publish to a shadow table, then atomically swap
    out.write.mode("overwrite").format("delta").save("s3://lake/_recovery/fct_orders_v2")

    # validate -> swap
    assert_rowcount("s3://lake/_recovery/fct_orders_v2", expected_min=1)
    swap_table("fct_orders", "fct_orders_v2")      # metadata-only, atomic
```

### 4.4 Iceberg WAP: write → audit → publish

```sql
-- 1. WRITE to an isolated branch
ALTER TABLE prod.fct_orders CREATE BRANCH repair_2026_01_15;
-- Spark writes into prod.fct_orders VERSION AS OF ... / with the branch:
--   spark.sql("INSERT INTO prod.fct_orders.branch_repair_2026_01_15 SELECT ...")

-- 2. AUDIT (run checks against the branch before anyone sees it)
SELECT COUNT(*) FROM prod.fct_orders.branch_repair_2026_01_15
 WHERE order_date = DATE '2026-01-15';    -- must equal the source count

-- 3. PUBLISH atomically (fast-forward main to the audited branch)
ALTER TABLE prod.fct_orders FAST FORWARD main TO repair_2026_01_15;

-- Rollback if needed: point main back at the previous snapshot
CALL prod.system.rollback_to_snapshot('prod.fct_orders', <previous_snapshot_id>);
```

### 4.5 Delta: time travel restore + partition repair

```sql
-- Inspect history
DESCRIBE HISTORY analytics.fct_orders;

-- Whole-table restore (use sparingly)
RESTORE TABLE analytics.fct_orders TO VERSION AS OF 42;

-- Targeted repair: recompute one partition atomically (preferred)
```

```python
(repaired_df.write
   .format("delta").mode("overwrite")
   .option("replaceWhere", "order_date = '2026-01-15'")
   .saveAsTable("analytics.fct_orders"))
```

### 4.6 Lineage-driven recovery plan (conceptual implementation)

```python
def recovery_plan(root_dataset: str, corrupt_intervals: list[tuple[str, str]]) -> list[dict]:
    """Return a topologically ordered list of (asset, interval) to reprocess."""
    graph = load_lineage_graph()                       # from OpenLineage/Marquez/DataHub
    downstream = graph.descendants(root_dataset)       # includes root
    affected = [a for a in downstream if intervals_overlap(a, corrupt_intervals)]

    plan = []
    for asset in topological_sort(affected):           # dependency order
        for lo, hi in merge_intervals(a_affected_intervals(asset)):
            plan.append({"asset": asset, "start": lo, "end": hi})
    return plan

# Execute with bounded concurrency, newest-first *within* a layer:
#   for layer in plan_by_layer(plan):
#       run_bounded(layer, concurrency=layer.max_concurrency)
#       validate_layer(layer)     # fail fast before propagating bad data further
```

### 4.7 Disaster recovery: backups, snapshots, failover

```text
RPO (data loss tolerance) ──▶ determines backup/snapshot frequency & replication
RTO (downtime tolerance) ──▶ determines whether you need hot standby or cold restore
```

| Layer | DR mechanism | Typical RPO/RTO |
|-------|--------------|-----------------|
| Source systems | Continuous replication, PITR backups | minutes / minutes |
| Raw zone (S3/GCS) | Cross-region replication, versioning | ~0 / minutes |
| Warehouse | Cross-region replication, time travel, failover groups | minutes / minutes–hours |
| Lakehouse tables | Iceberg/Delta metadata replication + object replication | ~0 / minutes |
| Orchestrator metadata | Managed DB backups / HA | minutes / minutes |
| Code/config | Git + IaC (Terraform/Pulumi) | 0 / minutes |
| Secrets | Managed secret store with replication | ~0 / minutes |
| **Recovery plan** | Documented + tested drills | — |

```hcl
# Terraform: cross-region replication for the raw zone (illustrative)
resource "aws_s3_bucket_replication_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id
  role   = aws_iam_role.replication.arn
  rule {
    id     = "raw-cross-region"
    status = "Enabled"
    destination { bucket = aws_s3_bucket.raw_replica.arn }
  }
}
```

**DR procedure (must be written and rehearsed):**

1. Declare the incident and freeze writes.
2. Promote the replica / fail over the warehouse.
3. Validate that the raw zone is complete up to the failure point.
4. Recompute affected intervals from raw (idempotent backfill).
5. Re-enable schedules; watch freshness catch up.
6. Postmortem: what was the actual RTO/RPO vs. the target? Update the plan.

---

## 5. Where to implement it (tools and when each fits)

| Capability | Tools | When |
|-----------|-------|------|
| Interval backfill | Airflow `backfill`/clear, Dagster backfills, Prefect deployments, dbt `--select`, Argo resubmit | Any historical recompute |
| Immutable raw zone | S3/GCS/ADLS + Parquet/Avro; Kafka with long retention/compaction | Always (recovery foundation) |
| Reproducible replay | Delta time travel, Iceberg snapshots/branches, Kafka offsets/retention | When raw is mutable or streaming |
| Atomic publish | Iceberg branches, Delta `replaceWhere`, Snowflake `SWAP WITH`, BigQuery view swap | Zero-downtime corrections |
| Lineage & blast radius | OpenLineage + Marquez, DataHub, OpenMetadata, Atlan | Multi-team platforms |
| Backups/PITR | Warehouse time travel, RDS/Aurora PITR, S3 versioning | Data loss protection |
| Cross-region DR | S3 CRR, warehouse replication/failover, managed Kafka MirrorMaker | Regional resilience |
| IaC | Terraform, Pulumi, CloudFormation, Helm/Kustomize | Reproducible environment recovery |
| Chaos/recovery drills | Game days, chaos tooling, restore drills | Prove the plan works |
| Data diff | `data-diff`, `dbt-audit-helper`, custom checksums | Verify recovery completeness |

**When each fits:**

- **Delta/Iceberg time travel** whenever your lakehouse tables need replayable history (almost always).
- **Iceberg branches** for WAP on high-stakes tables where consumers must never see partial data.
- **`replaceWhere`** for the common case: recompute a partition, atomically.
- **Table swap** when you rebuilt the whole table and want an instant cutover + rollback.
- **Cross-region replication** once the business impact of data loss exceeds the replication cost.

---

## 6. Patterns and anti-patterns

### 6.1 Patterns

| Pattern | Purpose |
|---------|---------|
| **Immutable raw / landing zone** | Enables replay and reprocessing |
| **Interval-scoped, idempotent transforms** | Makes backfill a routine operation |
| **Backfill as a first-class command** | No code changes to repair data |
| **Write-Audit-Publish** | Publish only validated data |
| **Blue/green table swap** | Zero-downtime cutover + instant rollback |
| **Shadow rebuild** | Test a corrected dataset before publishing |
| **Lineage-driven blast radius** | Enumerate and order recovery work |
| **Backfill newest-first within a layer** | Unblock consumers fastest |
| **Canary/shadow runs before a logic change** | Catch regressions before publishing |
| **Reconciliation with source of truth** | Proves recovery completeness |
| **Retained source extracts** (Bronze forever / raw forever) | Removes the need to re-query fragile sources |
| **DR drills / game days** | Convert a plan into a proven capability |
| **Versioned pipeline code + IaC** | Rebuild the platform from Git |

### 6.2 Anti-patterns

| Anti-pattern | Consequence |
|--------------|-------------|
| **No raw zone / transform-and-discard ETL** | No replay path, ever |
| **Mutating raw data in place** | Cannot reproduce historical results |
| **Non-idempotent backfill** | Repair creates duplicates |
| **Backfill by editing the WHERE clause** | Ad-hoc, error-prone, unreviewed |
| **Overwriting the target in place** | Consumers see half-repaired data |
| **No lineage** | Blast radius guessed; some downstream tables never fixed |
| **Forgetting downstream** | Fixed table feeds stale marts for weeks |
| **No validation before publish** | Bad repair goes live |
| **Drop-and-recreate tables** | Loses grants, constraints, history, and causes outages |
| **Backfilling everything because "we're not sure"** | Enormous cost, still not guaranteed correct |
| **One-shot DR plan never tested** | Fails when invoked |
| **No RPO/RTO defined** | Cannot size backups or justify cost |
| **No retention on raw** | Old intervals become unrecoverable when retention expires |
| **Manual recovery runbooks in someone's head** | Bus factor = 1 |

---

## 7. Real-world use cases

**Retail: a pricing bug ran for 12 days.** Recovery: the raw POS feed was retained for 2 years, so the
team backfilled 12 days of the affected marts, validated against the source counts, and swapped the
tables atomically. Consumers saw consistent numbers throughout; MTTR was 6 hours.

**Fintech: a CDC connector dropped the `deletes` flag for 3 days.** Recovery: re-read the WAL from the
last correct LSN, reprocessed the affected partitions, and ran a reconciliation that compared the
warehouse to the source row counts and checksums before publishing.

**Media: a streaming sessionization job had a watermark bug for a week.** Recovery: replayed the
retained Kafka topic (7-day retention) into a repair pipeline, recomputed the affected days, and used
Iceberg branches to validate before fast-forwarding `main`.

**Logistics: a region outage took down the warehouse for 4 hours.** Recovery: failed over to the
secondary region, and because the raw zone was cross-region replicated, catch-up backfill started
immediately with RPO ≈ 0 and RTO ≈ 40 minutes.

**Healthcare: an audit needed the exact numbers from 18 months ago.** Recovery: the platform was
reproducible, so the team checked out the code tag, ran the pipeline against the retained raw zone, and
reproduced the figures byte-for-byte.

**SaaS: a schema change broke 40 downstream models.** Recovery: lineage computed the blast radius, and
a single command backfilled the 40 models in dependency order for the affected window.

---

## 8. Senior-level deep dive

### 8.1 Designing for recovery: the checklist

| Requirement | Mechanism |
|-------------|-----------|
| Replayable inputs | Immutable, retained raw zone (Delta/Iceberg/Kafka retention) |
| Deterministic transforms | No `now()`, seeded randomness, stable ordering |
| Idempotent writes | MERGE / partition overwrite / WAP |
| Interval identity | `data_interval_start/end` or partition keys |
| Backfill capability | Orchestrator backfill + bounded concurrency |
| Blast radius | Lineage (OpenLineage) with column- and partition-aware edges |
| Zero-downtime repair | Atomic swap / branches / `replaceWhere` |
| Proof of correctness | Reconciliation + checksums + tests |
| DR | Replication, backups, PITR, IaC, tested runbooks |
| Observability of recovery | Track MTTR, backfill duration, recovery completeness |

> **Senior note:** If you can only do one thing, **retain the raw data and make the raw zone
> immutable**. It buys you the option to fix *any* downstream bug later, at the cost of storage. Most
> recovery failures are really "we didn't keep the inputs".

### 8.2 RPO/RTO → architecture

```text
                    RTO (how fast you must recover)
                < 15 min        < 4 h          < 24 h        < 1 week
 RPO   ~0    │  active-active │ hot standby  │ warm standby │ cold restore
 (loss) < 1h  │  multi-region  │ replication  │ replication  │ + replay from raw
        < 1d  │  replication   │ replication  │ backups      │ backups + replay
        days │  backups       │ backups      │ backups      │ backups
```

The cost of each row/column combination grows steeply. Pick the *cheapest cell that satisfies the
business requirement*, and document the decision.

### 8.3 The backfill decision tree

```text
What changed?
├─ Code/logic only, raw data untouched
│   └─ Re-run affected intervals from raw (deterministic replay). Cheapest.
├─ Raw data was wrong (source bug / duplicate load)
│   ├─ Re-extract from source (if the source can still serve the historical window)
│   └─ Else: correct the raw zone from a known-good backup, then recompute
├─ A downstream table was corrupted but inputs are fine
│   └─ Recompute only the affected downstream assets (lineage-driven)
└─ Everything is suspect (e.g., a shared library bug)
    └─ Recompute the whole affected time range across all dependent assets
```

Always ask: **is the source of truth still available?** If yes, prefer re-extraction into raw and
recompute. If no, you must have retained raw (or you cannot recover).

### 8.4 Failure modes of recovery itself

| Failure | Cause | Symptom | Mitigation |
|---------|-------|---------|-----------|
| Duplicate data after backfill | Non-idempotent writes | Row counts grow | Idempotent MERGE/overwrite; verify counts |
| Recovery itself times out | Full re-run of a huge window | Backfill never finishes | Interval-level granularity + concurrency |
| Sources overwhelmed | 365 days × N tasks at once | 429s, source incidents | Pools, rate limits, off-peak backfills |
| Missing downstream | No lineage | Stale marts persist | Lineage-driven plan; reconciliation across layers |
| Half-published repair | Overwrite in place | Consumers see mixed versions | WAP / atomic swap |
| Not actually reproducible | `now()` in logic; mutated raw | Numbers still differ after "replay" | Fix determinism; immutable raw |
| Cost blowout | Backfilling 3 years by mistake | Huge invoice | Dry-run first; bounded ranges; approval for large backfills |
| "Recovered" but wrong | No reconciliation | Silent correctness gap | Checksums + source comparison |
| DR fails at the worst time | Never tested | Extended outage | Game days; automated restore drills |
| Lost metadata | Orchestrator DB not backed up | Cannot tell what ran | Back up the orchestrator; use lineage as an independent record |

### 8.5 Performance and cost of recovery

- **Backfill throughput is bounded by the slowest resource** — usually the source API or the source
  database, not your compute.
- **Repartition the backfill**: process intervals independently and in parallel, but bound concurrency.
- **Compact afterwards.** Backfills often produce many small files (`OPTIMIZE`/`rewrite_data_files`
  post-backfill).
- **Reuse compute**: a backfill of 30 days can run in one Spark session reading 30 partitions — far
  cheaper than 30 separate runs, but with a larger blast radius if it fails. Prefer many small units
  with idempotent writes over one giant fragile job.
- **Cost of storage vs cost of recovery**: retaining 5 years of raw Parquet may cost less than one
  hour of the wrong answer. Model it.

### 8.6 A worked example: end-to-end repair procedure

```text
Incident: a currency-conversion bug in `stg_orders` understated EUR orders from 2026-01-10 to 2026-01-14.

T+0   Detected by finance reconciliation (EUR totals 3% low).
T+5m  Freeze writers to affected tables (disable the DAG / set a pause flag).
T+10m Computed blast radius via lineage: stg_orders → fct_orders → mart_revenue →
      mart_retention, plus 4 reverse-ETL syncs.
T+15m Created a shadow schema `recovery`, rebuilt stg_orders and fct_orders for the 5 days
      from the immutable raw zone using the fixed logic.
T+40m Validation: row counts match the source ledger; EUR totals within 0.1% of the
      processor settlement file; dbt tests pass on the shadow tables.
T+45m Atomic publish: swapped fct_orders and mart_revenue to the shadow versions
      (Snowflake SWAP WITH / Iceberg FAST FORWARD). Rolled back reverse-ETL syncs to
      re-push corrected segments.
T+50m Re-enabled schedules; backfill completed; dashboards refreshed.
T+1h  Reconciled warehouse vs. source for the 5 days: 100% match.
T+1d  Postmortem: added a currency-range test (a new monitor) and a unit test on the
      conversion logic. Registered the incident in the runbook.
```

Note how every stage relied on a property designed *before* the incident: immutable raw, interval
identity, idempotent writes, lineage, and an atomic swap.

---

## 9. Common mistakes and how to avoid them

1. **No raw retention.** *Fix:* retain raw immutable data for at least your worst-case reprocessing
   horizon (often 1–7 years for regulated domains).
2. **Mutating raw data.** *Fix:* append-only writes to the raw zone; never upsert it.
3. **Recovery requires a code change.** *Fix:* make backfill a parameterized command (interval range).
4. **Backfilling without bounding concurrency.** *Fix:* pools, rate limits, off-peak scheduling,
   dry-runs.
5. **No validation before publish.** *Fix:* WAP: validate the shadow, then swap.
6. **Publishing in place.** *Fix:* shadow table + atomic swap; keep the old version for rollback.
7. **Forgetting downstream assets.** *Fix:* lineage-driven plans; cross-layer reconciliation.
8. **Assuming "re-run" == "replay".** *Fix:* immutable raw + determinism (or time travel).
9. **No RPO/RTO.** *Fix:* define them per dataset; size backups/replication accordingly.
10. **DR plan never tested.** *Fix:* schedule drills; measure actual RTO/RPO against target.
11. **Orchestrator metadata not backed up.** *Fix:* back up the Airflow/Dagster DB; use lineage as a
    second record of what ran.
12. **No approval/guardrails for large backfills.** *Fix:* require a dry-run and an explicit window.
13. **Fixing forward instead of fixing back.** *Fix:* repair the affected intervals; never "start
    correct from today".
14. **Post-incident with no new test.** *Fix:* every incident adds at least one automated check.

---

## 10. Exercises / interview questions

1. You discover a transform bug that has affected 6 months of data. Write the step-by-step recovery
   plan, including ordering, validation, and publishing. *(Hint: raw zone → shadow → validate →
   swap → reconcile.)*
2. Your company deletes raw data after 30 days and a bug is found 60 days later. What are your options?
   What should have been the retention policy? *(Hint: source re-extraction, cost model, retention.)*
3. Explain the difference between backfill, replay, and rerun with a concrete example where they
   produce different results. *(Hint: mutated inputs.)*
4. Design the atomic-publish mechanism for a 5 TB table with a 2-hour rebuild time and consumers
   querying continuously. *(Hint: Iceberg branch + fast forward; Snowflake SWAP WITH.)*
5. How do you compute blast radius automatically? What metadata does each edge need?
6. Your backfill of 365 days would hammer a rate-limited API. How do you execute it safely?
   *(Hint: pools, throttling, off-peak, chunking, checkpointing the backfill itself.)*
7. Define RPO/RTO for a fraud-detection feature store. Justify the numbers and the architecture.
8. Walk through a DR drill for a full region failure. What do you test, and how do you measure success?
9. What makes a pipeline *reproducible*? List every source of non-determinism you can think of and the
   mitigation for each.
10. A "recovered" table has the right row count but a 2% different checksum. What do you do next?
    *(Hint: rounding, ordering, timezone, late data, non-deterministic keys.)*
11. How do you backfill a **streaming** pipeline? *(Hint: replay from offsets/retention; bounded
    replay; or reprocess from stored raw events.)*
12. Sketch the architecture for a platform where "any table can be rebuilt from raw at any time."
    What must be true of every pipeline?

---

## References

- Apache Airflow — Backfill & catchup: https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/backfill.html
- Apache Airflow — DAG runs, logical date & `data_interval_*`: https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dag-run.html
- Dagster — Partitions and backfills: https://docs.dagster.io/guides/build/partitions-and-backfills
- dbt — `--full-refresh` and rebuilding incremental models: https://docs.getdbt.com/docs/build/incremental-models#how-do-i-rebuild-an-incremental-model
- Apache Iceberg — Branching & tagging (WAP, rollback): https://iceberg.apache.org/docs/latest/branching/
- Apache Iceberg — Reliability / snapshots & time travel: https://iceberg.apache.org/docs/latest/reliability/
- Delta Lake — Time travel & `RESTORE`: https://docs.delta.io/latest/delta-batch.html#query-an-older-snapshot-of-a-table-time-travel
- Delta Lake — Selective overwrite (`replaceWhere`): https://docs.delta.io/latest/delta-batch.html#-selective-overwrite
- Snowflake — `SWAP WITH` / zero-copy cloning: https://docs.snowflake.com/en/sql-reference/sql/alter-table
- AWS Big Data Blog — Write-Audit-Publish with Iceberg branches: https://aws.amazon.com/blogs/big-data/build-write-audit-publish-pattern-with-apache-iceberg-branching-and-aws-glue-data-quality/
- OpenLineage — Lineage events for blast radius: https://openlineage.io/docs/
- Marquez — Lineage graph & recovery planning: https://marquezproject.ai/
- AWS — Disaster recovery strategies (RPO/RTO): https://docs.aws.amazon.com/whitepapers/latest/disaster-recovery-workloads-on-aws/disaster-recovery-options-in-the-cloud.html
- Google Cloud — DR planning guide: https://cloud.google.com/architecture/dr-scenarios-planning-guide
- Apache Kafka — Log retention & replay: https://kafka.apache.org/documentation/#compaction
