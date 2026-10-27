# ETL vs ELT, Pipeline Anatomy, and Orchestration Fundamentals

> A pipeline is not "a script that moves data" — it is a **contract**: a repeatable, scheduled,
> observable process that turns raw inputs into trustworthy outputs for a defined time window.
> Juniors learn operators and DAG syntax; seniors learn to reason about *where* transformation
> happens (ETL vs ELT), *when* it happens (batch vs micro-batch vs streaming), and *who guarantees
> correctness when a run fails and is retried* (orchestration semantics). This chapter builds the
> vocabulary and the mental model the other five files in this chapter depend on.

---

## TL;DR

- **ETL** transforms before landing in the target; **ELT** lands raw first and transforms inside the
  warehouse/lakehouse. ELT is the default for cloud warehouses because storage is cheap and compute
  is elastic; ETL still wins for **PII masking, heavy non-SQL transforms, and streaming enrichment**.
- **A pipeline has five stages**, not three: *extract → stage/land → transform → validate/publish →
  observe*. The two "extra" stages are where senior engineers add value.
- **Batch / micro-batch / streaming** is a *latency vs complexity* dial, not a maturity ladder. Most
  companies need batch + one streaming path, not a fully streaming platform.
- **A DAG encodes dependencies**, not time. Scheduling (cron, timetables, sensors, data-aware
  triggers) decides *when* the DAG runs; the DAG decides *what must finish before what*.
- **The unit of correctness is the time interval (partition), not the run.** Every task should be a
  function of `data_interval_start`/`data_interval_end`, never of `now()`.
- **Idempotency is the price of retries.** If the orchestrator can retry a task — and all of them
  can — then the task must produce the same result on every re-run. See
  [03-idempotency.md](./03-idempotency.md).
- **Orchestrators are not compute engines.** Airflow/Dagster/Prefect coordinate; Spark/dbt/the
  warehouse execute. Confusing the two is the most common architectural mistake in this space.
- **dbt is a transformation framework, not an orchestrator** — though it can be scheduled by one.
  Dagster and Airflow both model dbt as a native integration.

---

## 1. Definitions and core vocabulary

### 1.1 The three letters

| Term | Meaning | Where transform happens |
|------|---------|------------------------|
| **ETL** | Extract → **Transform** → Load | In a separate engine *before* the target |
| **ELT** | Extract → **Load** → Transform | Inside the target (warehouse/lakehouse) |
| **EtLT** | Extract → *light* transform → Load → final transform | Hybrid: cheap cleanup on the way in, heavy modeling after |

The historical reason ETL existed is that **warehouse compute was scarce and expensive**. You could
not afford to run `JOIN`-heavy logic inside a 2008-era appliance, so you transformed on a dedicated
server (Informatica, DataStage, Ab Initio) and loaded only clean, conformed data.

Cloud changed the economics:

- S3/GCS/ADLS object storage costs ~$0.02/GB/month → storing raw is nearly free.
- Snowflake/BigQuery/Databricks/Redshift compute scales elastically and bills per second.
- SQL is now the dominant transformation language and columnar engines are extremely good at it.

So the modern default flipped to **ELT**. But "default" ≠ "always". See §2.

### 1.2 Batch vs micro-batch vs streaming

| Mode | Latency | Unit of work | Typical engine | Cost shape |
|------|---------|--------------|----------------|-----------|
| **Batch** | hours–days | a partition (day/hour/file) | Spark, dbt, warehouse SQL | cheap per row, idle between runs |
| **Micro-batch** | seconds–minutes | a small partition or trigger interval | Spark Structured Streaming, Flink (in batch mode) | steady, higher than batch per row |
| **Streaming** | ms–seconds | an event (record) | Flink, Kafka Streams, Spark SS, Beam | always-on; you pay for idle |
| **Change Data Capture (CDC)** | seconds–minutes | a row-level change event | Debezium, Fivetran, Datastream | depends on target |

> **Senior note:** Micro-batch is *not* a "smaller batch". The distinguishing property is that the
> engine maintains **state across triggers** (watermarks, running aggregates, offsets). That state is
> exactly what makes recovery harder — see [02-fault-tolerance.md](./02-fault-tolerance.md).

### 1.3 Pipeline anatomy

```
┌──────────┐   ┌───────────┐   ┌────────────┐   ┌──────────────┐   ┌───────────┐
│ EXTRACT  │──▶│  STAGE /  │──▶│ TRANSFORM  │──▶│  VALIDATE /  │──▶│  OBSERVE  │
│ (source) │   │   LAND    │   │ (SQL/Spark)│   │   PUBLISH    │   │ (metrics) │
└──────────┘   └───────────┘   └────────────┘   └──────────────┘   └───────────┘
     ▲               ▲                ▲                ▲                ▲
  API/DB/Kafka   raw zone (immutable)   staging→marts   tests/tables    logs/traces
                    Parquet/Avro       bronze→gold     atomic swap      lineage
```

- **Extract** — pull from a source *for a bounded interval*. Sources: OLTP DB (JDBC/CDC), APIs
  (rate-limited, paginated), files (SFTP/S3), event streams (Kafka/Kinesis/Pub/Sub).
- **Stage / Land** — write the raw bytes durably and immutably. This is your **replayable source of
  truth**. Never mutate it. Partition by ingestion date AND source event date if possible.
- **Transform** — go from raw → conformed → business-level (bronze → silver → gold, or
  stage → intermediate → marts in dbt terms).
- **Validate / Publish** — run assertions, then atomically expose the result (swap a view, flip a
  branch in Iceberg, rename a partition).
- **Observe** — emit freshness, volume, schema, and quality signals. See
  [04-monitoring-observability.md](./04-monitoring-observability.md).

### 1.4 DAG vocabulary

| Term | Definition |
|------|-----------|
| **DAG** | Directed Acyclic Graph — tasks as nodes, dependencies as edges. No cycles. |
| **Task / op / asset** | A unit of work. In Airflow: `TaskInstance` of an `Operator`. In Dagster: an `op` or a materialization of an asset. |
| **Run** | One execution of the DAG (Airflow: `DagRun`; Dagster: a `Run`). |
| **Logical date / data interval** | The time window the run *represents* — distinct from wall-clock start time. |
| **Backfill** | Running historical intervals that were never run or need re-running. |
| **Catchup** | Whether the scheduler auto-creates runs for *past* intervals when a DAG is (re)enabled. |
| **Sensor** | A task that waits for a condition (file exists, partition ready, another DAG finished). |
| **Trigger rule** | Condition for running a task (`all_success`, `all_done`, `one_failed`, …). |
| **Idempotent task** | Running it N times yields the same state as running it once. |

---

## 2. What problem it solves / why it matters

### 2.1 Why ETL vs ELT is an *architecture* decision

The choice determines:

1. **Who can build transforms.** ELT → analysts write SQL/dbt. ETL → only engineers who can ship code.
2. **Whether raw data is recoverable.** ELT keeps raw → you can always re-derive. A classic ETL that
   discards raw data has **no replay path** — the worst possible position to be in.
3. **Where cost lands.** ETL pays for a separate compute cluster; ELT pays for warehouse compute
   (which can be *much* more expensive per CPU-hour — Snowflake credits vs. spot EC2).
4. **Compliance posture.** If PII must never land in the lake, you need *some* ETL (masking) before
   load, or a governed zone with column-level security.

### 2.2 Why orchestration matters

Without an orchestrator you have **cron + shell scripts**, and you immediately lose:

- **Retry with backoff** and per-task isolation.
- **Dependency resolution** (task B only if A succeeded *for the same interval*).
- **Backfill** as a first-class operation.
- **Observability** — per-task logs, durations, and status history.
- **Lineage** — what depends on what, who consumes this table.

> **Senior note:** The orchestrator's real product is **not scheduling**. It is *state*: a durable
> record of which interval of which task succeeded, so that retries, backfills, and partial failures
> are tractable. Every design decision should protect that state machine.

### 2.3 Why the time-window mindset matters

The single most important idea in this chapter:

```text
A task must be a pure function of its data interval:

    output(interval) = f(input(interval), config)

It must NOT be a function of wall-clock time or "the latest data available".
```

If you write `WHERE created_at >= CURRENT_DATE` you have made the task depend on *when it runs*, so a
retry at 00:05 tomorrow produces a different result than the run at 23:55 today. Airflow's own best
practices say this explicitly: *"Never read the latest available data in a task… use
`data_interval_start` as a partition."*

---

## 3. How it works internally (mechanisms, internals)

### 3.1 ETL internals

Classic ETL = **extract → transform in-memory/on-disk → load**, usually with a metadata-driven
engine (Informatica, SSIS, Talend) or hand-written Spark. Key internals:

- **Extraction is often the bottleneck**, not transformation. JDBC reads from an OLTP database
  compete with production traffic → you need *incremental* extraction, read replicas, or CDC.
- **Transformation is row-wise or set-wise.** Columnar engines (Spark, warehouse) prefer set-wise;
  row-oriented ETL tools push work down or spill to disk.
- **Load is the transaction boundary.** If load is a full-partition overwrite, recovery is trivial
  (re-run). If it is a row-by-row insert, recovery is a nightmare (partial rows inserted).

### 3.2 ELT internals

```
Source ──▶ Loader (Fivetran/Airbyte/ADF/Debezium) ──▶ RAW zone ──▶ dbt/Spark SQL ──▶ MARTS
   (schema-on-read; source-shaped; append-only; "as landed" columns + load metadata)
```

Key internals:

- **Loaders keep load metadata**: `_loaded_at`, `_extracted_at`, `_source_file`, `_batch_id`. Do not
  delete these — they are your deduplication and lineage anchors.
- **dbt models are compiled to SQL** and executed by the warehouse. There is no dbt "runtime": the
  warehouse does the work. `{{ ref() }}` builds a dependency graph that dbt turns into an execution
  order.
- **Materializations** are the crux: `view`, `table`, `incremental`, `ephemeral`, `materialized_view`,
  `snapshot`. Each trades storage, freshness, and cost differently.

### 3.3 Orchestrator internals

Airflow (3.x) core loop:

```text
Scheduler ── parses DAG files → serializes to DB (DagModel) ── creates DagRuns for intervals
          └─▶ Scheduler decides which TaskInstances are runnable (deps met, pool slots free)
              └─▶ Executor queues them (Local/Celery/Kubernetes)
                  └─▶ Workers run task.execute() ──▶ write state back to metadata DB
```

Critical internals to know:

- **DAG parsing is Python execution.** Expensive top-level code runs on *every* parse (default
  `min_file_process_interval` ~30s). Dynamic DAG generation multiplies that cost.
- **The metadata DB is a scheduler bottleneck.** Thousands of tasks/seconds of heartbeats will
  saturate Postgres. This is why Airflow 3 separated the DAG processor and rewrote the scheduler's
  critical path.
- **Deferrable operators / sensors** release the worker slot while waiting — essential for
  long-running "wait for file in S3" patterns.
- **Dagster inverts the model**: you declare **assets** (tables, files) and their upstream
  dependencies; runs materialize assets. Backfills and partitions become first-class.

---

## 4. How to implement it (step-by-step with code)

### 4.1 Minimal, correct Airflow DAG (idempotent, partition-scoped)

```python
# dags/orders_daily.py — Airflow 3 style (TaskFlow API)
from __future__ import annotations

import pendulum
from airflow.sdk import DAG, task
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

with DAG(
    dag_id="orders_daily",
    schedule="0 2 * * *",           # 02:00 UTC daily
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=True,                   # backfill missing intervals on enable
    max_active_runs=1,              # serialize runs: never overlap intervals
    default_args={
        "retries": 3,
        "retry_delay": pendulum.duration(minutes=5),
        "retry_exponential_backoff": True,
        "max_retry_delay": pendulum.duration(minutes=30),
    },
    tags=["orders", "daily"],
) as dag:

    start = EmptyOperator(task_id="start")

    @task
    def extract(interval_start: str, interval_end: str) -> str:
        """Extract is a pure function of the interval; writes to the raw zone."""
        # e.g. copy orders WHERE updated_at >= interval_start AND < interval_end
        # to s3://raw/orders/dt=<date>/batch=<run_id>.parquet  (append-only!)
        return f"s3://raw/orders/dt={interval_start[:10]}/"

    extract_sql = SQLExecuteQueryOperator(
        task_id="transform_orders",
        conn_id="warehouse",
        sql="""
            -- Idempotent transform: DELETE+INSERT the whole partition, or MERGE
            delete from analytics.stg_orders
             where order_date = date '{{ data_interval_start | ds }}';

            insert into analytics.stg_orders
            select ...
              from raw.orders
             where order_date = date '{{ data_interval_start | ds }}';
        """,
    )

    check = SQLExecuteQueryOperator(
        task_id="check_rowcount",
        conn_id="warehouse",
        sql="""
            select case when count(*) = 0
                        then 1/0  -- forces task failure
                        else 1 end
              from analytics.stg_orders
             where order_date = date '{{ data_interval_start | ds }}';
        """,
    )

    end = EmptyOperator(task_id="end")

    start >> extract(
        "{{ data_interval_start | ds }}", "{{ data_interval_end | ds }}"
    ) >> extract_sql >> check >> end
```

Why this is "correct by construction":

- `max_active_runs=1` prevents overlapping runs from racing on the same partition.
- Every read/write is scoped to `data_interval_start` (Jinja templated at execution time).
- The transform is a **partition-scoped delete+insert** → safe to re-run (idempotent).
- Retries are configured at the DAG level; the check task fails loudly on empty output.

### 4.2 The same pipeline as ELT + dbt

```sql
-- models/staging/stg_orders.sql  (materialized as incremental, unique_key = order_id)
{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
) }}

select
    order_id,
    customer_id,
    order_total,
    created_at,
    updated_at
from {{ source('raw', 'orders') }}
{% if is_incremental() %}
  -- only rows created/updated since the last successful run
  where updated_at >= (select coalesce(max(updated_at), '1900-01-01') from {{ this }})
{% endif %}
```

```yaml
# models/staging/_sources.yml
sources:
  - name: raw
    database: RAW
    schema: ORDERS
    tables:
      - name: orders
        loaded_at_field: _loaded_at
        freshness:
          warn_after: {count: 6, period: hour}
          error_after: {count: 24, period: hour}
```

```bash
dbt build --select stg_orders+          # run + test the model and everything downstream
dbt build --select stg_orders --full-refresh   # rebuild from scratch (logic changed)
```

### 4.3 Scheduling patterns (cron + offsets + timezones)

| Pattern | Cron | Notes |
|---------|------|-------|
| Daily at 02:00 UTC, data for **yesterday** | `0 2 * * *` | With `data_interval_start`, the run at 02:00 on day D covers day D-1. |
| Hourly | `0 * * * *` | Set `max_active_runs=1`; use `depends_on_past` sparingly (it serializes badly). |
| Wait for a file, not a clock | `schedule=None` + S3 sensor / `@asset_sensor` | Data-driven; avoids useless runs. |
| After upstream DAG completes | Airflow **Asset/Dataset** scheduling or Dagster **asset dependency** | Preferred over `ExternalTaskSensor` (no cross-DAG polling). |

> **Senior note — the timezone trap:** "Daily" means different things at DST boundaries. Always run
> orchestrators in UTC and convert only at the presentation layer. Airflow cron is evaluated in the
> DAG's timezone; a `America/New_York` DAG will have a 23- or 25-hour interval twice a year.

### 4.4 Airflow vs Dagster vs Prefect in code

```python
# Dagster: assets are the unit; partitions and backfills fall out of the model
import dagster as dg

daily = dg.DailyPartitionsDefinition(start_date="2024-01-01")

@dg.asset(partitions_def=daily)
def raw_orders(context: dg.AssetExecutionContext) -> None:
    day = context.partition_key
    # extract for `day` -> raw zone

@dg.asset(partitions_def=daily, deps=[raw_orders])
def stg_orders(context: dg.AssetExecutionContext) -> None:
    day = context.partition_key
    # transform for `day`; idempotent by construction

# Backfill a range:
#   dagster asset backfill --select stg_orders --partition-range 2024-01-01...2024-01-31
```

```python
# Prefect: flows + tasks; retries and caching are decorators
from prefect import flow, task

@task(retries=3, retry_delay_seconds=[10, 60, 300], cache_key_fn=lambda *_: "fixed")
def extract(day: str) -> str: ...

@flow
def orders_daily(day: str):
    path = extract(day)
    # ...
```

---

## 5. Where to implement it (tools and when each fits)

### 5.1 Orchestrators

| Tool | Model | Best when | Watch out for |
|------|-------|-----------|---------------|
| **Apache Airflow** | Task-centric DAGs | Huge ecosystem, many sources/sinks, enterprise standard | Metadata DB scaling, DAG parse cost, no native data-aware model until Assets |
| **Dagster** | Asset-centric, typed I/O | You care about *data assets*, partitions, backfills, local dev (dagit) | Smaller ecosystem; opinionated |
| **Prefect** | Pythonic flows | Small teams that want dynamic, code-first workflows | Less "declarative scheduling"; state is in Prefect Cloud/Server |
| **dbt Cloud / dbt Core** | Models + lineage | SQL transformations; scheduling of `dbt build` | Not a general orchestrator (no arbitrary tasks / sensors) |
| **Argo Workflows** | Kubernetes-native DAG | Already all-in on K8s; container-per-step | No data-aware scheduling; YAML-heavy |
| **Mage / Kestra / Flyte** | Mixed | Niche or ML-heavy | Evaluate maturity carefully |

### 5.2 Ingestion / extraction tools

| Category | Tools | When |
|----------|-------|------|
| Managed ELT connectors | Fivetran, Airbyte (OSS/Cloud), Hevo, ADF | Fast path to many SaaS sources; buy over build |
| CDC | Debezium, AWS DMS, Datastream, Fivetran HVR | Low-latency replication from OLTP; avoids `SELECT *` load |
| Batch move | DistCp, `aws s3 sync`, Snowpipe, `COPY INTO` | Bulk file movement |
| Custom | Python/Boto3, JDBC, `requests` with pagination | Bespoke APIs and edge cases |

### 5.3 Compute for transformation

- **Warehouse SQL** (Snowflake, BigQuery, Redshift, Databricks SQL) — ELT default.
- **dbt** — SQL modeling framework with tests, docs, lineage.
- **Spark** — Python/Scala transforms, ML feature engineering, very large or messy data.
- **Flink / Beam** — true streaming and unified batch/stream semantics.

---

## 6. Patterns and anti-patterns

### 6.1 Patterns

| Pattern | Problem it solves |
|---------|-------------------|
| **Immutable raw / landing zone** | Enables replay, reprocessing, and audit without touching sources |
| **Idempotent partition overwrite** (`DELETE … WHERE dt=X; INSERT …`) | Safe retries and backfills |
| **Write-Audit-Publish (WAP)** | Consumers never see unvalidated data (see [05-recoverability.md](./05-recoverability.md)) |
| **Medallion (bronze/silver/gold)** | Clear contracts between raw, conformed, and business layers |
| **Fan-out / fan-in** | Parallelize independent extracts, then fan in to a single transform |
| **Blue/green table swap** | Zero-downtime, atomic publish of a rebuilt table |
| **Data-aware scheduling** | Replace clock-driven runs with "run when upstream data is ready" |
| **Contract-first ingestion** | Schema is agreed up front; drift is an incident, not a surprise |
| **Config-driven DAG generation** | One factory + a YAML per table, instead of 400 copy-pasted DAGs |

### 6.2 Anti-patterns

| Anti-pattern | Why it hurts |
|--------------|-------------|
| **`now()` inside a transform** | Non-deterministic; retries/backfills produce different data |
| **Reading "latest" data** (`WHERE ts > (SELECT max(ts) FROM src)`) | Missed or duplicated rows under concurrency; not replayable |
| **One giant DAG for the whole company** | One failure blocks everything; unreadable; slow to parse |
| **Too many tiny tasks** | Scheduling overhead, metadata DB pressure, no real isolation |
| **Business logic in the orchestrator (Python operator doing 30 min of pandas)** | Orchestrator workers become a compute cluster; no scaling story |
| **No raw retention** | You cannot reprocess when a transform bug is found |
| **Silent partial success** (task returns 0 rows, "succeeds") | Data quietly disappears; only freshness/volume checks catch it |
| **Backfilling by changing code to "last 30 days"** | Non-reusable, error-prone; backfills must be a first-class operation |
| **Cross-DAG implicit dependencies** (a task *hoping* another DAG finished) | Race conditions; breaks under backfill |
| **Testing only in production** | The orchestrator is a production system; DAGs need unit + integration tests |

---

## 7. Real-world use cases and industry applications

| Industry / domain | Pipeline shape | Notes |
|-------------------|----------------|-------|
| **E-commerce** | CDC from Postgres orders, nightly dbt marts, hourly event aggregates | Late-arriving orders require reprocessing recent partitions |
| **Fintech / banking** | Strict ETL for PII, immutable audit log, reconciliation pipelines | Regulatory: every number must be traceable to a source event |
| **Ad-tech / marketing** | Streaming click events → sessionization → attribution | High volume, late data, heavy dedup — see streaming chapter |
| **IoT / manufacturing** | Streaming sensor data → windowed aggregates → alerting | Backpressure and out-of-order data dominate; see [01-scalable-pipelines.md](./01-scalable-pipelines.md) |
| **Gaming** | Event ingestion → near-real-time dashboards + daily economy reports | Mixed batch + streaming; replay of player sessions is common |
| **Healthcare** | HL7/FHIR ingestion, de-identification, clinical marts | Compliance boundaries force ETL+masking before landing |
| **SaaS product analytics** | Product events → warehouse → reverse-ETL back into tools | Freshness SLOs drive business metrics directly |
| **ML platforms** | Feature pipelines → training sets → monitoring | Requires point-in-time correctness; pipelines are "training data contracts" |

---

## 8. Senior-level deep dive

### 8.1 Choosing ETL vs ELT vs EtLT — a decision framework

```text
Does raw data contain regulated PII that must not persist in a general lake?
├─ YES → ETL or EtLT: mask/tokenize before landing; keep a governed raw zone with column security
└─ NO  → Is the transform expressible in SQL at scale?
         ├─ YES → ELT (load raw, transform with dbt/warehouse SQL)      ← 80% of cases
         └─ NO  → EtLT: land raw, then run Spark/custom compute, then load modeled output
```

Decision dimensions to weigh explicitly:

| Dimension | Favors ETL | Favors ELT |
|-----------|-----------|-----------|
| Warehouse compute cost | high/constrained | elastic/cheap |
| Team SQL skill | low | high |
| Raw data sensitivity | high | low |
| Need to replay from raw | low (or unavoidable) | high |
| Non-SQL transforms | heavy | light |
| Governance maturity | strong tooling | weak |
| Time-to-first-value | slow | fast |

> **Senior note:** The best modern answer is almost always **EtLT**: land an *immutable, access-
> controlled* raw zone (which is ELT), then do *cheap* masking/typing on the way in and heavy
> modeling after. You get replayability *and* compliance.

### 8.2 Orchestration architecture decisions

| Decision | Options | Trade-off |
|----------|---------|-----------|
| Where does compute run? | Orchestrator worker vs. external cluster | Isolation & scalability vs. convenience |
| Push vs. pull extraction | Source pushes to landing (events) vs. pipeline pulls | Push = lower latency, needs source cooperation |
| Task granularity | 1 task per table vs. 1 task per pipeline | Retry cost vs. orchestration overhead |
| Scheduling model | Time-based vs. data-aware (Assets/Sensors) | Predictability vs. efficiency |
| State store | Orchestrator DB vs. dedicated state table | Central visibility vs. self-contained pipelines |
| Trigger semantics | `catchup=True` vs `False` | Automatic backfill vs. cost blowups |

**Task granularity heuristic:** aim for tasks that take **1–15 minutes**. Below that, orchestration
overhead dominates; above that, a mid-task failure wastes work and you lose retry precision.

### 8.3 Failure modes specific to orchestration

| Failure | Symptom | Root cause | Mitigation |
|---------|---------|-----------|-----------|
| Duplicate runs | Two runs writing the same partition | `catchup` + manual trigger, or `max_active_runs > 1` | Idempotent writes; `max_active_runs=1` |
| Zombie tasks | Task "running" forever, slot held | Worker crash, no heartbeat | `execution_timeout`, zombie detection, pools |
| DAG parse storm | Scheduler CPU pegged, runs delayed | Top-level heavy code across many DAGs | Lazy imports, DAG bundles, split files |
| Metadata DB saturation | Slow scheduling, timeouts | Too many tasks/heartbeats | Tune scheduler, prune old runs, reduce task count |
| Silent skipped data | "Success" but stale data | Missing upstream partition, filter dropped rows | Freshness + volume monitors, `not_null`/`unique` tests |
| Retry storms | Thundering herd on the source | Tight `retry_delay`, many parallel tasks | Exponential backoff + jitter, concurrency limits |

### 8.4 Debugging a broken pipeline — a procedure

1. **Identify the interval.** Which `data_interval_start` is wrong? Compare against the freshness
   dashboard.
2. **Check the run state machine.** Airflow Grid view / Dagster run log: which task failed, and how
   many times did it retry?
3. **Reproduce the interval locally.** `airflow tasks test dag_id task_id <logical_date>` or
   `dbt build --select model --defer` against a dev schema.
4. **Inspect inputs for that interval only.** Was the upstream partition present? Complete? Was the
   extract truncated by a pagination bug?
5. **Verify output count and distribution.** Row counts and null rates vs. the previous partition —
   the fastest signal of a filter bug.
6. **Fix → re-run that interval only.** Never "fix forward" by re-running today; re-run the broken
   interval, then let catchup fill forward if required.
7. **Record it.** Postmortem with a regression test (a dbt test, an assertion task, an alert rule).

### 8.5 Performance considerations

- **Push down.** Always push filters/aggregations into the source and the warehouse; avoid pulling
  millions of rows into the orchestrator.
- **Parallelize the fan-out.** `max_active_tasks` + pools let you run 20 extracts concurrently
  without saturating a source.
- **Partition pruning.** Every scan should be partition-pruned; a transform that reads the whole raw
  table each run will not survive growth.
- **Incremental by default.** Full-refresh should be an exception you trigger deliberately.
- **Data interval alignment.** Align pipeline intervals with source partition boundaries (e.g., S3
  date prefixes, warehouse partition columns) to avoid partial reads.

---

## 9. Common mistakes and how to avoid them

1. **Using `now()` / `CURRENT_DATE` in transforms.** *Fix:* templated `data_interval_start/end`; pass
   interval as a parameter everywhere.
2. **Assuming the orchestrator runs tasks in order.** Dependencies define order — not the order you
   wrote the code. *Fix:* declare explicit edges.
3. **Mixing scheduling concerns into transforms.** *Fix:* the task should not know whether it is a
   backfill or a scheduled run — only its interval.
4. **No raw zone.** *Fix:* land immutable raw data before any transform; it is your cheapest insurance.
5. **Letting catchup run wild on a new DAG.** *Fix:* `catchup=False` by default for exploratory
   pipelines; enable deliberately with a bounded `start_date`.
6. **Monolithic DAGs.** *Fix:* one DAG per domain/table-group; use Asset/Dataset dependencies.
7. **Ignoring timezones/DST.** *Fix:* UTC everywhere; test a DST boundary.
8. **Treating dbt as an orchestrator.** *Fix:* let Airflow/Dagster trigger `dbt build`, or use dbt
   Cloud's scheduler with proper alerting — but put sensors and cross-system deps in the orchestrator.
9. **Empty-result success.** *Fix:* assert minimum row counts and freshness; make zero rows a failure
   for a required partition.
10. **No local dev or CI.** *Fix:* run DAGs with `python my_dag.py`, unit-test operators, lint with
    `ruff check --select AIR`, and test in a staging environment.
11. **Secrets in DAG code.** *Fix:* connections / secrets backends / env injection.
12. **Backfill = "change the filter and hope".** *Fix:* make backfill a parameterized, repeatable
    operation (Airflow's `--start-date/--end-date`, Dagster partition backfills).

---

## 10. Exercises / interview questions

1. You inherit a DAG whose transform uses `WHERE created_at >= CURRENT_DATE - 1`. Describe precisely
   what happens on: (a) a retry 3 hours later, (b) a backfill of last month, (c) a DST switch day.
   *(Hint: enumerate the three failure modes — duplicate, missing, shifted intervals.)*
2. Design the pipeline for a source that only offers full snapshots (a 50 GB CSV nightly). How do you
   make the *load* incremental and idempotent? *(Hint: think checksums/merge keys/immutable landing +
   set-difference transforms.)*
3. When would you choose ETL over ELT for a new pipeline in 2026? Give two concrete scenarios and the
   specific requirement that forces each. *(Hint: PII persistence rules; non-SQL transforms.)*
4. Your DAG must run only after another team's DAG produces data, but their schedule drifts. Compare:
   `ExternalTaskSensor`, Airflow Assets/Datasets, a file sensor, and a shared state table. Which do
   you pick and why? *(Hint: backfill and race semantics.)*
5. Batch, micro-batch, streaming: for a fraud-detection use case, pick one and justify with latency,
   cost, and correctness arguments. *(Hint: what is the cost of a 5-minute delay?)*
6. Your `catchup=True` DAG is enabled with `start_date` three years ago and an API-based extract.
   What goes wrong in the first hour? How do you remediate *without* disabling catchup permanently?
   *(Hint: pools, concurrency, and a bounded backfill.)*
7. Explain why "the orchestrator is not a compute engine" matters architecturally. What breaks when
   you ignore it at 100 DAGs? At 1,000? *(Hint: worker resources, retries, autoscaling.)*
8. Write the DAG-level configuration that prevents two runs from writing the same partition, and
   explain the residual race that remains if someone manually triggers a run.
9. What does "the unit of correctness is the interval, not the run" mean for testing? Sketch the test
   strategy for one model. *(Hint: fixture intervals, golden datasets, `dbt test` scoping.)*
10. Given a pipeline that must be **replayable for 18 months**, sketch the storage layout (raw zone,
    zones, table formats, retention) and the cost/compliance trade-offs.

---

## References

- Apache Airflow — Best Practices: https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html
- Apache Airflow — Core Concepts (DAGs, Tasks, Scheduling): https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/index.html
- Apache Airflow — `data_interval_start` / templating reference: https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html
- Apache Airflow — Assets / data-aware scheduling: https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/asset-scheduling.html
- Astronomer — DAG writing best practices: https://www.astronomer.io/docs/airflow/dag-best-practices
- Dagster — Software-defined assets: https://docs.dagster.io/guides/build/assets
- Dagster — Partitioning and backfills: https://docs.dagster.io/guides/build/partitions-and-backfills
- Prefect — Workflows & retries: https://docs.prefect.io/
- dbt — Incremental models: https://docs.getdbt.com/docs/build/incremental-models
- dbt — Materializations: https://docs.getdbt.com/docs/build/materializations
- dbt — Source freshness: https://docs.getdbt.com/docs/build/sources#source-data-freshness
- Databricks — ETL vs ELT: https://www.databricks.com/discover/etl/vs-elt
- Snowflake — Data pipelines / Snowpipe: https://docs.snowflake.com/en/user-guide/data-load-snowpipe-intro
- Google Cloud — Dataflow/Airflow pipelines: https://cloud.google.com/dataflow/docs
- AWS — Modern data architecture (ELT patterns): https://docs.aws.amazon.com/prescriptive-guidance/latest/modern-data-centric-use-cases/welcome.html
