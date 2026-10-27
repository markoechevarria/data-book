# Data Validation

> Data validation is the last line of defense between a broken upstream system and a
> decision made on bad numbers. A junior engineer writes "IF NULL" checks. A senior
> engineer designs a **validation architecture**: what gets checked, at which boundary,
> with what severity, what happens to the rows that fail, and how the checks themselves are
> tested, versioned, and owned. This chapter is about building that architecture with
> constraints, schema contracts, and frameworks like Great Expectations, Soda, and dbt
> tests — and about the operational consequence of failing a check: quarantine, dead-letter,
> or fail-the-pipeline.

## TL;DR

- **Validation is a set of assertions about data**, evaluated at a **boundary** (ingestion,
  pre-write, post-write, pre-publish). Where you put the boundary matters more than which
  tool you use.
- The canonical **data quality dimensions** are completeness, accuracy, consistency,
  timeliness, uniqueness, validity (+ integrity, and often "reasonableness"/distribution).
  Every check you write should map to one of them.
- **Not all failures are equal.** Classify severity: `block` (fail the pipeline / reject the
  batch), `warn` (emit metric + alert, keep going), `quarantine` (route offending rows to a
  side table / dead-letter queue), `report` (dashboard only). A single binary pass/fail on a
  100M-row batch is almost always the wrong model.
- **Schema contracts** (dbt contracts, Avro/Protobuf + Schema Registry, Delta/Iceberg
  enforcement, ODCS data contracts) catch structural breakage *before* row-level checks run.
  Structural tests are cheap and deterministic; statistical tests are expensive and fuzzy.
  Run them in that order.
- **Great Expectations** models data as `Batch → Expectation Suite → Checkpoint → Validation
  Result → Data Docs`. **Soda** compiles YAML **SodaCL** checks. **dbt tests** are just
  `SELECT` statements that return failing rows. Pick by where your transform lives, not by
  feature checklist.
- **Quarantine / dead-letter** is the enterprise-grade answer to "the pipeline must not
  stop and we must not lose data". It converts a hard failure into a soft failure + a
  replayable backlog.
- **Test the tests in CI.** Validation code is code: lint it, unit-test it against golden
  fixtures, and run it against a sample of production on every PR. A check that silently
  stopped running is worse than no check, because it creates false confidence.

## 1. Definitions and core vocabulary

| Term | Meaning |
|------|---------|
| **Validation** | Evaluating assertions against data and producing a pass/fail/quantity result. |
| **Expectation / Check / Test** | A single assertion, e.g. "column `email` is never null". |
| **Expectation Suite / Check suite** | A versioned collection of assertions for a dataset. |
| **Checkpoint / Scan** | A runnable unit that binds data + suite + actions (and produces results). |
| **Assertion result** | The observed outcome: `success`, `success_ratio`, `unexpected_count`, `observed_value`. |
| **Threshold / Tolerance** | The level at which a check fails (e.g. nulls < 0.1%, freshness < 2h). |
| **Contract** | A formal, versioned agreement on schema + semantics + quality + SLA between a producer and consumers. |
| **Schema contract / enforcement** | Rejecting writes whose schema does not match the declared one. |
| **Quarantine** | Isolating rows that failed validation into a separate dataset for triage. |
| **Dead-letter queue (DLQ)** | A durable channel (usually Kafka) holding messages that could not be processed/validated. |
| **Data quality dimension** | A named axis of quality used to structure checks (completeness, etc.). |
| **Freshness** | How recent the data is relative to expected cadence. |
| **Profiling** | Measuring statistical properties of data (min/max/null%/distribution) to derive checks. |

### 1.1 The data quality dimensions (the taxonomy you attach to every check)

| Dimension | Question | Typical check | Framework primitive |
|-----------|----------|---------------|---------------------|
| **Completeness** | Are required values present? | `null_count(col)=0`, `row_count` in expected band | GX `expect_column_values_to_not_be_null`, Soda `missing_count`, dbt `not_null` |
| **Accuracy** | Do values reflect reality? | Range/format checks, cross-system reconciliation | GX `expect_column_values_to_be_between`, custom SQL |
| **Consistency** | Do values agree across systems/rows? | FK integrity, totals reconcile, SCD coherence | dbt `relationships`, `expression_is_true` |
| **Timeliness** | Is it fresh enough? | `max(updated_at)` vs now | Soda `freshness`, dbt source freshness, GX `expect_column_max_to_be_between` |
| **Uniqueness** | Are keys unique? | duplicate_count = 0 | GX `expect_column_values_to_be_unique`, dbt `unique` |
| **Validity** | Do values conform to a rule/format/range? | Enums, regex, ranges | GX `expect_column_values_to_match_regex`, dbt `accepted_values` |
| **Integrity** | Are relationships intact? | referential integrity, checksums | dbt `relationships` |
| **Reasonableness / Distribution** | Is the *shape* plausible? | null% stable, mean within 3σ of 7-day baseline | Elementary `anomaly` tests, MC monitors |

> **Senior note.** Accuracy can almost never be checked from inside a single dataset — you
> have no ground truth. Real accuracy checks are **reconciliation** and **provenance**: sum
> of the source table vs sum of the target table, event count in Kafka vs count in the
> warehouse, or a lineage comparison against the system of record. Everything else marketed
> as "accuracy" is really validity or completeness.

## 2. What problem it solves / why it matters

A data platform fails in four characteristic ways:

1. **Structural breakage** — an upstream team renames `user_id` to `uid`; your pipeline
   silently writes NULLs into a join key and the dashboard shows "-".
2. **Semantic breakage** — the unit of `amount` changes from cents to dollars; nothing is
   null, nothing fails, and the revenue number is off by 100×.
3. **Timeliness / availability breakage** — the pipeline is green but ran at 03:00 for a
   06:00 dashboard and the data is 27 hours stale.
4. **Silent partial failure** — 0.4% of rows corrupt; no alert; the error compounds across
   three joins into a 6% error in the published KPI.

Validation addresses all four, but **only if you choose the right boundary and severity**.
The economics are simple:

- Cost to fix a defect at ingestion: **1×**.
- Cost to fix it after it reaches the warehouse: **10×** (backfills, downstream recomputes).
- Cost to fix it after a decision was made on it: **100×** (a bad price, a bad payout, a
  regulatory filing).

This is the data analogue of Boehm's cost-of-change curve, and it is the reason "shift
validation left" is the single highest-leverage quality decision a data team makes.

## 3. How it works internally (mechanisms, internals)

### 3.1 The anatomy of a validation run

```
        ┌────────────┐   ┌──────────────┐   ┌───────────────┐   ┌───────────────┐
        │   DATA     │──▶│   ENGINE     │──▶│  ASSERTIONS   │──▶│   RESULTS +   │
        │ (batch/df/ │   │ (SQL pushdown│   │ (suite/checks)│   │  ACTIONS      │
        │  stream)   │   │  or local)   │   │               │   │ (alert, block,│
        └────────────┘   └──────────────┘   └───────────────┘   │  quarantine)  │
                                                                └───────────────┘
                                                                        │
                                                              ┌─────────▼─────────┐
                                                              │ results store +   │
                                                              │ observability UI  │
                                                              └───────────────────┘
```

Two execution models dominate:

- **Pushdown (Soda Core, dbt, GX-SQL):** the framework compiles each check into SQL
  (`SELECT count(*) FROM t WHERE col IS NULL`) and runs it **inside** the warehouse. Cost =
  warehouse compute; scale = warehouse scale; the data never leaves the warehouse. This is
  the correct default for anything ≥ a few million rows.
- **Local / in-memory (GX with pandas/Spark, Deequ):** the framework reads the data (or a
  sample) into an engine and computes metrics. Flexible (can validate a DataFrame before it
  is written), but you pay for moving data and you must manage sampling.

### 3.2 What an expectation actually computes (GX internals)

Great Expectations decomposes every "column map" expectation into **metrics** computed from
the batch, then compares them to configured bounds. For
`expect_column_values_to_not_be_null`:

```
metric               = column.null_count
element_count        = column_values.nonnull.count + null_count
unexpected_count     = null_count
success_ratio        = (element_count - unexpected_count) / element_count
success              = success_ratio >= mostly           # default mostly = 1.0
```

The **`mostly` parameter** is the crucial knob: `mostly=0.99` turns a hard assertion into a
tolerance-based one. GX also computes `partial_unexpected_counts` and
`unexpected_index_list` (a sample of offending rows) so that a failure is *actionable*, not
just red.

> **Senior note.** The single most common production mistake is `mostly=1.0` (the default)
> on a column that legitimately has 0.2% nulls. The team then either disables the check or
> grows deaf to it. Set tolerances from **observed baselines** (profile first), not from
> ideals.

### 3.3 Checkpoints, actions, and why validation is a *pipeline stage*

A **checkpoint** binds `data asset + suite + runtime config + actions` and is what actually
runs in production (not the suite alone). An action is a side effect on the validation
result: write to Data Docs, send a Slack alert, update a metric store, **fail the Airflow
task**, or trigger a callback that quarantines rows. This is what makes validation
composable: the same suite can be run with a `warn-only` action set in staging and a
`fail-hard` action set in prod.

### 3.4 Structural vs statistical checks

```
   CHEAP, DETERMINISTIC                EXPENSIVE, PROBABILISTIC
   ──────────────────────             ─────────────────────────
   schema equality                    row_count in band
   types / nullability  ────────▶     null% within baseline
   PK/FK constraints                  distribution drift
   enums, regex, ranges               freshness anomaly
   ──────────────────────             ─────────────────────────
   Run FIRST. If they fail,         Run SECOND. Slower, and need
   don't bother computing stats.    historical baselines to be meaningful.
```

## 4. How to implement it (step-by-step, with code/YAML/SQL examples)

### 4.1 Step 0 — Profile first, then assert

Never hand-write thresholds. Profile the data, derive candidates, then promote the important
ones to hard checks:

```python
# Great Expectations 1.x — profile a dataset to bootstrap a suite
import great_expectations as gx
import pandas as pd

context = gx.get_context()

# 1. Data source -> asset -> batch definition
ds = context.data_sources.add_pandas("pandas_ds")
asset = ds.add_dataframe_asset(name="orders")
batch_def = asset.add_batch_definition_whole_dataframe("orders_batch")

df = pd.read_parquet("s3://curated/orders/dt=2025-01-01/part.parquet")
batch = batch_def.get_batch(batch_parameters={"dataframe": df})

# 2. Profile -> candidate expectations you then curate by hand
from great_expectations.expectations.expectation_configuration import ExpectationConfiguration
# (GX ships a Data Assistant / Onboarding Data Assistant for this in the UI/CLI.)
```

The output of profiling is a **draft** suite. A human then removes volatile checks (e.g. a
min/max that throws every Black Friday) and keeps the durable ones.

### 4.2 dbt — data tests as SQL, plus contracts

dbt tests are the lowest-friction, highest-leverage validation for warehouse-transformed
data. A test is any `SELECT` that returns **failing rows**; zero rows = pass.

```yaml
# models/marts/orders/schema.yml
version: 2

models:
  - name: fct_orders
    description: "One row per completed order."
    config:
      contract:
        enforced: true          # <-- schema contract: dbt REFUSES to build on schema drift
    columns:
      - name: order_id
        data_type: string
        description: "PK"
        tests:
          - unique
          - not_null

      - name: customer_id
        data_type: string
        tests:
          - not_null
          - relationships:                       # referential integrity (integrity dimension)
              to: ref('dim_customer')
              field: customer_id

      - name: status
        data_type: string
        tests:
          - accepted_values:
              values: ['placed', 'shipped', 'delivered', 'returned', 'cancelled']

      - name: order_total
        data_type: numeric
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: "order_total >= 0"

      - name: order_date
        data_type: date
        tests:
          - dbt_utils.not_null_proportion:
              at_least: 0.999
```

A **singular test** for a business rule that doesn't fit a generic test:

```sql
-- tests/fct_orders_total_matches_lines.sql  (one test = one file = one SELECT)
-- Consistency: the order header total must equal the sum of its line items.
select
    o.order_id,
    o.order_total      as header_total,
    l.line_total       as lines_total
from {{ ref('fct_orders') }}                o
join (
    select order_id, sum(line_amount) as line_total
    from {{ ref('fct_order_lines') }} group by 1
) l using (order_id)
where abs(o.order_total - l.line_total) > 0.01
```

**Source freshness** (timeliness dimension) is declared, not coded:

```yaml
# models/staging/_sources.yml
sources:
  - name: raw_orders
    database: raw
    schema: app
    tables:
      - name: orders
        loaded_at_field: _ingested_at
        freshness:
          warn_after:  { count: 6,  period: hour }
          error_after: { count: 24, period: hour }
```

`dbt source freshness` then fails/warns based on `max(_ingested_at)`.

> **Senior note.** dbt tests are **post-transform, warehouse-side**. They cannot stop a bad
> file from being *written*; they fail after the model is materialized (unless the model is
> ephemeral/incremental with `--select`). If you need pre-write gating, that is a contract or
> an ingestion-time validator, not a dbt test.

### 4.3 Great Expectations — a full suite + checkpoint

```python
# gx_setup.py  (GX 1.x style; concepts identical in 0.18.x)
import great_expectations as gx
from great_expectations.checkpoint import UpdateDataDocsAction
from great_expectations.checkpoint.actions import SlackNotificationAction

context = gx.get_context()

# --- suite ---
suite = context.suites.add(gx.ExpectationSuite(name="orders_suite"))

# completeness
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(column="order_id"))
# uniqueness
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeUnique(column="order_id"))
# validity (enum + regex + range)
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeInSet(
        column="status",
        value_set=["placed", "shipped", "delivered", "returned", "cancelled"]))
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToMatchRegex(
        column="email", regex=r"^[^@\s]+@[^@\s]+\.[^@\s]+$", mostly=0.999))
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeBetween(
        column="order_total", min_value=0, max_value=1_000_000, mostly=0.9999))
# timeliness (batch-scoped)
suite.add_expectation(
    gx.expectations.ExpectColumnMaxToBeBetween(
        column="order_ts", min_value="2025-01-01"))
# completeness at table level
suite.add_expectation(
    gx.expectations.ExpectTableRowCountToBeBetween(min_value=100_000))

# --- validation definition binds data + suite ---
validation_def = context.validation_definitions.add(
    gx.ValidationDefinition(
        name="validate_orders",
        data=context.data_sources.get("warehouse").get_asset("orders")
              .get_batch_definition("daily_batch"),
        suite=suite,
    )
)

# --- checkpoint binds validation + actions ---
context.checkpoints.add(gx.Checkpoint(
    name="orders_checkpoint",
    validation_definitions=[validation_def],
    actions=[
        UpdateDataDocsAction(name="update_docs"),
        SlackNotificationAction(
            name="slack", slack_webhook_url="${SLACK_WEBHOOK}",
            notify_on="failure",
            renderer={"module_name": "great_expectations.render.renderer",
                      "class_name": "SlackRenderer"}),
    ],
))

result = context.checkpoints.get("orders_checkpoint").run()
assert result.success, "Data validation failed"   # <- makes the Airflow task fail
```

**Quarantine action** (route failing rows aside instead of failing the batch):

```python
def quarantine_failing_rows(validation_result, batch, engine):
    """Custom GX action: write unexpected rows to a side table and continue."""
    for r in validation_result.results:
        if not r.success and "unexpected_index_query" in r.result:
            sql = r.result["unexpected_index_query"]   # a SELECT you can wrap
            engine.execute(f"""
                INSERT INTO quarantine.orders_rejects
                    (order_id, rule, detected_at, payload)
                SELECT *, '{r.expectation_config.type}', current_timestamp, to_json(t)
                FROM ({sql}) t
            """)
```

### 4.4 Soda — SodaCL checks (YAML) + data contracts

Soda compiles YAML into SQL. It is the fastest path from "we have no checks" to "we have
checks in CI".

```yaml
# checks/orders.yml
checks for fct_orders:
  # completeness
  - missing_count(order_id) = 0
  - missing_percent(customer_id) < 1%
  # uniqueness
  - duplicate_count(order_id) = 0
  # validity
  - invalid_percent(email) < 0.5%:
      valid format: email
  - invalid_count(status) = 0:
      valid values: [placed, shipped, delivered, returned, cancelled]
  - invalid_percent(order_total) = 0%:
      valid min: 0
  # timeliness
  - freshness(order_ts) < 6h
  # volume (with anomaly detection against recent history)
  - row_count > 100000
  - change for row_count > -20%   # Soda Cloud-backed; compare to prior runs
  # schema contract
  - schema:
      fail:
        when required column missing: [order_id, customer_id, order_total]
        when wrong column type:
          order_total: numeric
```

Run it:

```bash
soda scan -d warehouse -c configuration.yml checks/orders.yml
# exit code != 0 -> fail the CI step / Airflow task
```

A Soda **data contract** (schema enforcement across the stack):

```yaml
# contract for fct_orders
dataset: fct_orders
columns:
  - name: order_id
    data_type: string
    checks:
      - missing_count = 0
      - duplicate_count = 0
  - name: status
    data_type: string
    checks:
      - invalid_count = 0:
          valid values: [placed, shipped, delivered, returned, cancelled]
```

### 4.5 Schema contracts (catch drift before row checks)

A **schema contract** fails fast when structure changes. Three mechanisms:

| Mechanism | Where | Behaviour on drift |
|-----------|-------|--------------------|
| dbt `contract.enforced: true` | Transformation | Build **fails**; dbt won't create a table whose schema differs from the declared `columns` |
| Avro/Protobuf + Schema Registry | Streaming / ingestion | Producer's `register` is **rejected** if incompatible |
| Delta `overwriteSchema=false` / Iceberg writes | Lakehouse | Write **fails** on incompatible schema |
| ODCS data contract + `datacontract-cli test` | Anywhere | CI step fails on schema/quality violation |

Minimal ODCS contract (Linux Foundation / Bitol):

```yaml
apiVersion: v3.0.0
kind: DataContract
id: fct_orders
version: 1.2.0
status: active
servers:
  - server: prod-warehouse
    type: snowflake
    environment: production
schema:
  - name: fct_orders
    physicalName: analytics.marts.fct_orders
    properties:
      - name: order_id
        logicalType: string
        required: true
        unique: true
      - name: order_total
        logicalType: number
        required: true
        quality:
          - type: library
            rule: greaterThanOrEqual
            value: 0
slaProperties:
  - property: freshness
    value: 6
    unit: hour
```

### 4.6 Quarantine and dead-letter patterns

**Pattern A — quarantine table (batch / ELT).** Split a batch into `good` and `rejected`
with one SQL pass; load both; alert on reject count.

```sql
-- One pass: write good rows to the target, bad rows to quarantine.
WITH src AS (SELECT * FROM stage.orders_raw WHERE dt = current_date),
flagged AS (
    SELECT *,
        (order_id IS NOT NULL AND order_total >= 0
         AND status IN ('placed','shipped','delivered','returned','cancelled'))
         AS is_valid
    FROM src
)
INSERT INTO quarantine.orders_rejects
SELECT *, current_timestamp AS _rejected_at,
       concat_ws(',',
         CASE WHEN order_id IS NULL THEN 'null_pk' END,
         CASE WHEN order_total < 0 THEN 'negative_total' END,
         CASE WHEN status NOT IN ('placed','shipped','delivered','returned','cancelled')
              THEN 'bad_status' END) AS _rules_failed
FROM flagged WHERE NOT is_valid;

INSERT INTO curated.fct_orders
SELECT * FROM flagged WHERE is_valid;
```

**Pattern B — dead-letter queue (streaming / Kafka).** Never block the consumer; route
undeserializable or invalid messages to a DLQ topic with metadata.

```
Kafka topic: orders.raw ──▶ Consumer
                              │
             deserialize + validate
                 │            │
             valid│            │invalid  (deserialize error OR rule failure)
                 ▼            ▼
        sink: warehouse   orders.raw.dlq  (original bytes + headers:
                                          error_class, error_msg, rule,
                                          source_topic, source_offset, ts)
```

Implementation sketch (Kafka + Avro, Confluent serde):

```python
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationError

sr = SchemaRegistryClient({"url": "http://schema-registry:8081"})
deser = AvroDeserializer(sr, schema_str)

def handle(msg):
    try:
        record = deser(msg.value(), None)          # raises on bad bytes / wrong schema
        errors = validate_rules(record)            # your rule set
        if errors:
            raise ValueError(",".join(errors))
        warehouse.write(record)
    except (SerializationError, ValueError) as e:
        dlq_producer.produce(
            "orders.raw.dlq",
            key=msg.key(),
            value=msg.value(),                      # keep ORIGINAL bytes
            headers={
                "error_class": type(e).__name__,
                "error_msg":   str(e)[:512],
                "source_topic": msg.topic(),
                "source_offset": str(msg.offset()),
                "failed_at":   now_iso(),
            },
        )
```

> **Senior note.** A DLQ without an **owner, a replay tool, and an alert on DLQ depth** is
> just a graveyard. The rule: every DLQ message must be *either* replayed after a fix
> *or* explicitly acknowledged and deleted by a human, and the DLQ *depth* is an SLI you
> alert on. Otherwise you have silently dropped data with extra steps.

### 4.7 Testing validation in CI

Treat validation code as production code. Multiple layers:

```yaml
# .github/workflows/data-quality.yml
name: data-quality
on: [pull_request]

jobs:
  dq:
    runs-on: ubuntu-latest
    services:
      postgres: { image: postgres:16, env: { POSTGRES_PASSWORD: pg }, ports: ['5432:5432'] }
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: '3.11' }
      - run: pip install -r requirements.txt

      # 1. Lint the check definitions (broken YAML = silently skipped checks)
      - run: yamllint checks/ && sqlfluff lint models/

      # 2. Unit-test the check code against golden fixtures
      - run: pytest tests/unit/test_quality_rules.py

      # 3. Build dbt and run ALL tests in a throwaway env
      - run: dbt build --target ci --full-refresh
        env: { DBT_PROFILES_DIR: ./ci }

      # 4. Run Soda scans against the built models
      - run: soda scan -d ci_warehouse -c ci/configuration.yml checks/

      # 5. Enforce the data contract (schema + quality rules)
      - run: datacontract test contracts/fct_orders.contract.yaml --server ci

      # 6. Validate the CHANGE, not just the state: alert if a new expectation
      #    regresses on a production sample
      - run: python scripts/validate_on_sample.py --dataset fct_orders --rows 100000
```

Golden-fixture unit test:

```python
# tests/unit/test_quality_rules.py
import pandas as pd, great_expectations as gx

def test_orders_rules_accept_good_rows():
    df = pd.DataFrame({"order_id": ["1"], "order_total": [10.0], "status": ["placed"]})
    result = run_suite(df)          # helper that builds batch + suite
    assert result.success

def test_orders_rules_reject_negative_total():
    df = pd.DataFrame({"order_id": ["1"], "order_total": [-5.0], "status": ["placed"]})
    result = run_suite(df)
    assert not result.success
    # and assert it fails on the RIGHT expectation (not a coincidental null)
    assert {"expect_column_values_to_be_between"} <= {
        r.expectation_config.type for r in result.results if not r.success}
```

**The meta-rule:** the CI above tests the *validation*, and the "validate on production
sample" step tests that the validation still matches reality. A check that has never failed
and has never alerted is untested until proven otherwise.

## 5. Where to implement it (tools/platforms and when each fits)

| Tool | Execution | Strength | Best fit | Weak spot |
|------|-----------|----------|----------|-----------|
| **dbt tests** | Warehouse SQL | Zero new infra; lives with the model | Data already in the warehouse; transformation tests | No pre-ingest gating; no ML/anomaly by default |
| **Great Expectations** | SQL pushdown or local (pandas/Spark) | Richest expectation library; Data Docs; reusable suites | Heterogeneous sources; DataFrame-before-write; Python pipelines | Heavier setup; suite maintenance |
| **Soda (+ Soda Cloud)** | SQL pushdown | Fast authoring (SodaCL YAML); data contracts; anomaly checks | Pragmatic org starting DQ in CI | Cloud for advanced/anomaly features |
| **Amazon Deequ** | Spark | Scalable profiling + constraint verification for huge tables | Spark-native lakehouse | Spark-only; Scala-first |
| **Elementary** | dbt-native | Anomaly detection, freshness/volume/schema monitors as dbt tests | Teams already on dbt | dbt-centric |
| **TensorFlow Data Validation** | TFX / Beam | Schema inference + drift detection for ML | ML feature pipelines | ML-centric |
| **Frictionless / Pandera** | pandas/polars | Lightweight DataFrame schema+validation | Python ETL, unit tests | Not warehouse-scale |
| **ODCS + datacontract-cli** | CI | Product-oriented contract; cross-tool | Cross-team contracts, governance | Young ecosystem |

**Decision guide:**

- Data lives in the warehouse and you transform with dbt → **start with dbt tests +
  source freshness**. Add Elementary for anomaly detection.
- Mixed sources (APIs, files, CDC, Kafka) and you need pre-write gating → **Great
  Expectations** or **Soda** at the ingestion boundary.
- Huge Spark tables and you want profiling → **Deequ**.
- Cross-team producer/consumer agreement → **contracts** (dbt contract / Avro registry /
  ODCS) *before* row-level checks.

## 6. Patterns and anti-patterns

### Good patterns

| Pattern | Why |
|---------|-----|
| **Profile → curate → assert** | Thresholds come from data, not ideals. |
| **Layered severity** (`block`/`warn`/`quarantine`/`report`) | Not every defect should stop the pipeline. |
| **Structural before statistical** | Cheap deterministic checks gate expensive fuzzy ones. |
| **Quarantine + DLQ with replay** | Convert hard failures into recoverable backlogs. |
| **Contracts at the producer boundary** | Cheapest place to catch drift; enforces ownership. |
| **Tests-as-code in CI** | Validation regressions are caught pre-merge. |
| **Reconciliation checks** | The only real "accuracy" check available. |
| **Sampling for expensive checks** | Validate 1M rows statistically instead of 1B always. |
| **Fail with actionable context** | `unexpected_index_list`, sample values, rule name. |

### Anti-patterns

| Anti-pattern | Consequence | Fix |
|--------------|-------------|-----|
| `not_null` on every column by default | Noise, alert fatigue, checks disabled | Profile; keep only meaningful columns |
| Binary pass/fail on a whole batch | One bad row kills 100M good rows | Tolerances + quarantine |
| Checks that never run (broken cron, wrong dataset) | False confidence — the worst outcome | Meta-monitoring: alert if a check *didn't* run |
| Alert on the same Slack channel for everything | Deaf engineers | Routing by severity + ownership |
| Validation only in prod, never in CI | Errors caught at 2am, not at PR | Shift left |
| Hardcoding thresholds | Rot as data grows | Derive from baselines |
| DLQ with no consumer/replay | Silent data loss | Owner + replay tool + depth alert |
| Duplicating the same rule in 4 tools | Divergence, double maintenance | One source of truth (contract), generate checks |

## 7. Real-world use cases and industry applications

- **Financial services / payments.** Reconciliation is the core validation: ledger totals
  must match; every transaction appears exactly once; regulatory reports reconcile to the
  source of truth. Accuracy = cross-system reconciliation (see the classic "sum the source,
  sum the target" control).
- **E-commerce.** Order/payment/inventory consistency: an order marked `delivered` must have
  a shipment; inventory must never go negative; revenue KPIs must reconcile to the payment
  processor. Quarantine tables capture bad rows from flaky webhooks.
- **Ad-tech / real-time bidding.** Streaming DLQs for malformed bid events; freshness SLAs in
  the seconds; volume anomaly detection to catch a broken bidder before it burns budget.
- **Healthcare / life sciences.** Validity is regulated: coded values must be in the standard
  vocabulary (SNOMED/LOINC); ranges must be physiologically plausible; audit trails required.
- **IoT / telemetry.** Dead-letter for out-of-order/duplicate sensor reads; range plausibility
  (`temp > -50 AND temp < 150`) to drop sensor faults; volume anomalies detect a dead fleet.
- **ML / feature stores.** Distribution validation (train/serve skew), schema contracts for
  feature tables, completeness checks that block a model retrain on bad features.

## 8. Senior-level deep dive

### 8.1 Trade-offs

| Decision | Option A | Option B | What it depends on |
|----------|----------|----------|--------------------|
| Where to validate | Pre-ingest (source boundary) | Post-transform (warehouse) | Can you reject the source? Own the upstream? |
| Severity | Hard fail | Warn + quarantine | Cost of stopping the pipeline vs shipping bad data |
| Granularity | Row-level quarantine | Batch-level fail | Is the defect systemic or sparse? |
| Freshness of checks | Real-time (streaming) | Daily (batch) | Decision latency of consumers |
| Pushdown vs local | Warehouse SQL | In-memory | Data size, data residency, warehouse cost |
| Build vs buy | OSS (GX/Soda/dbt) | SaaS (Monte Carlo/Soda Cloud) | Team size, need for anomaly ML, TCO |
| Central vs federated | One DQ platform | Team-owned checks | Org size, data mesh maturity |

### 8.2 Performance considerations

- **Pushdown is almost always cheaper than pulling data.** A `count(*)`/`min`/`max` is often
  served from metadata or a zone map (Parquet min/max, Iceberg manifests, Snowflake
  clustering) and can be sub-second on billions of rows.
- **Beware full-column scans for distribution checks.** `approx_percentile` / quantile
  sketches (t-digest) and `count(distinct)` (HyperLogLog) keep checks sublinear.
- **Sample, but sample *correctly*.** Random samples miss rare-but-critical defects (a 0.001%
  corruption). For those, use a targeted predicate (`WHERE _source = 'partnerX'`) or validate
  all rows but only on the key columns.
- **Run checks after the load, not interleaved**, and let the warehouse cache/warm. On
  multi-tenant warehouses, DQ scans can starve ETL — put them on a separate warehouse/size.
- **Batch your metadata writes.** Persisting every expectation result for every run of
  thousands of tables explodes your metadata store; aggregate per-run.

### 8.3 Failure modes and debugging

| Failure mode | Symptom | Diagnosis |
|--------------|---------|-----------|
| **False positive** (alarm on good data) | Check fires during a promo/holiday | Threshold too tight; use seasonality-aware baselines |
| **False negative** (silent bad data) | Nobody alerted, numbers wrong | Check missing, or `mostly` too lax, or check not running |
| **Check didn't run** | Green dashboards, stale results | Meta-monitor: `last_run` per check; alert on staleness |
| **Suite drift** | Checks reference dropped columns | CI contract test; regenerate suites from schema |
| **Quarantine bloat** | Reject table grows unbounded | Alerting on reject *rate*, retention policy on rejects |
| **DLQ overload** | Consumer lag spikes, DLQ floods | Upstream schema change; check DLQ depth + error class histogram |
| **Non-determinism** | Same data passes/fails | Time-dependent checks (`now()`) without nominal time; pin the batch time |

Debugging a red check, in order:
1. Is it the check or the data? Re-run the check's SQL manually.
2. **When** did it start? Correlate with deploy/upstream run times (lineage helps — see
   chapter `03-data-lineage.md`).
3. **How big** is the blast radius? Row counts, affected partitions, downstream models.
4. Is it a **true** defect (fix data) or a **stale** check (fix threshold)? Never widen a
   threshold to make a real defect disappear.

### 8.4 Org and process — who owns data quality?

The senior answer is: **quality is owned at the producer, enabled by the platform, and
enforced by contracts.**

```
   PRODUCERS                 PLATFORM / CENTRAL            CONSUMERS
   ─────────                 ────────────────              ─────────
   own schema + SLA          provide tools (GX/dbt/Soda)     define acceptance
   publish contract  ──────▶ run meta-monitoring      ─────▶  tests + alerting
   fix at source             publish DQ metrics              report defects back
                             (data contracts as code)        (feedback loop)
```

- **Data contract** is the artifact that makes ownership explicit: producer commits to
  schema, semantics, and SLA; breaking it is a **breaking change** with a deprecation window.
- **Central team** owns the *platform* (runners, results store, alert routing, meta-monitoring),
  not every rule.
- **On-call:** DQ alerts route to the **owning team** of the dataset, not to a generic
  `#data-alerts`. A dataset without an owner is itself a finding.
- **Metrics:** track DQ **coverage** (% of critical datasets with a suite), **pass rate**,
  **MTTD/MTTR** for data incidents, and **false-alarm rate**. These make quality a managed
  system, not a heroic effort.

## 9. Common mistakes and how to avoid them

1. **Validation without tolerance.** → Set `mostly`/thresholds from profiling.
2. **Only structural checks OR only statistical checks.** → Layer them; structural first.
3. **No quarantine/DLQ.** → Fail-hard loses data; add a side channel + replay.
4. **Checks that can silently stop running.** → Meta-monitor `last_run` per check.
5. **Duplicated rules across tools.** → Single source of truth (contract), generate checks.
6. **Alerting everything at the same severity.** → Route by severity and owner; only `page`
   on `block`.
7. **No CI.** → Lint + unit-test the checks; run on a prod sample per PR.
8. **Confusing validity with accuracy.** → Add reconciliation checks; you can't self-assess
   accuracy from one table.
9. **Ignoring timeliness in tests.** → Add freshness as a first-class check.
10. **"Green dashboard = good data."** → Green means the checks you wrote passed; coverage
    and meta-monitoring tell you whether that means anything.

## 10. Exercises / interview questions

1. *Design:* You ingest a partner CSV nightly into the warehouse. The partner occasionally
   renames columns and sends 0.1% malformed rows. Design the validation at *two* boundaries
   and specify the severity and the fate of bad rows. *(Hint: contract at ingest + tests
   post-load; quarantine not fail.)*
2. *Internals:* Why does Great Expectations report both `unexpected_count` and
   `success_ratio`, and how does `mostly` interact with a checkpoint's `success`? *(Hint:
   element_count, tolerance, and what "success" means for the whole run.)*
3. *Trade-off:* Warehouse pushdown vs pulling data into Python for validation. When is each
   correct, and what breaks at 10B rows? *(Hint: zone maps, sampling, ML-based checks.)*
4. *Severity:* Give a concrete example where a `warn` is right and `block` is wrong, and one
   where the reverse is true.
5. *Quarantine:* Design the schema of a `rejects` table and a replay procedure. What columns
   are mandatory and why?
6. *CI:* How do you test that a validation suite itself is correct? *(Hint: golden fixtures,
   prove it fails on the right rule, meta-monitoring.)*
7. *Reconciliation:* You have no ground truth for "accuracy". Name three reconciliation
   checks you can still build in an e-commerce pipeline.
8. *Debugging:* A `not_null` check on `customer_id` fails in prod but passes in CI. Enumerate
   five root causes. *(Hint: source change, join change, sampling, timezones, contracts.)*
9. *Anti-pattern:* Why is `mostly=1.0` on a legitimately-sparse column dangerous beyond the
   false alarm itself? *(Hint: alert fatigue → disabled checks → silent failures.)*
10. *Org:* Who should own a data quality alert in a data mesh? What artifact makes that
    ownership enforceable? *(Hint: the contract.)*

## References

- Great Expectations — home & docs: https://docs.greatexpectations.io/docs/home/
- Great Expectations — expectation gallery: https://greatexpectations.io/expectations/
- Great Expectations — validation workflow / checkpoints: https://docs.greatexpectations.io/docs/0.18/oss/guides/validation/validate_data_overview/
- Soda — SodaCL quick start: https://docs.soda.io/soda-documentation/soda-v3/soda-cl-overview/quick-start-sodacl
- Soda — metrics and checks reference: https://docs.soda.io/soda-documentation/soda-v3/sodacl-reference/metrics-and-checks
- Soda Core (open source, data contracts): https://github.com/sodadata/soda-core
- dbt — data tests: https://docs.getdbt.com/docs/build/data-tests
- dbt — writing custom generic tests: https://docs.getdbt.com/best-practices/writing-custom-generic-tests
- dbt — model contracts: https://docs.getdbt.com/docs/build/model-contracts
- dbt — source freshness: https://docs.getdbt.com/docs/build/source-freshness
- Amazon Deequ (constraint verification for Spark): https://github.com/awslabs/deequ
- TensorFlow Data Validation: https://www.tensorflow.org/tfx/data_validation/get_started
- Frictionless Framework: https://framework.frictionlessdata.io/
- IBM — data quality dimensions: https://www.ibm.com/think/topics/data-quality-dimensions
- Open Data Contract Standard (Bitol / Linux Foundation): https://bitol-io.github.io/open-data-contract-standard/
- datacontract-cli: https://github.com/datacontract/datacontract-cli
- Apache Airflow (orchestrating validation tasks): https://airflow.apache.org/docs/
- Confluent — dead-letter queues for Kafka Connect: https://docs.confluent.io/platform/current/connect/concepts.html
