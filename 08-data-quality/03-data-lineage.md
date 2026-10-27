# Data Lineage

> Data lineage is the answer to two questions every senior engineer gets asked when something
> breaks at 3am: *"where did this number come from?"* and *"what else breaks if I change it?"*
> At table level it's a dependency graph; at column level it's the thing that makes impact
> analysis, debugging, and regulatory provenance actually possible. This chapter explains how
> lineage is captured (statically and at runtime), what OpenLineage/Marquez and dbt give you,
> and how to turn a lineage graph into a usable operational tool rather than a pretty picture.

## TL;DR

- Lineage comes in **two axes**: *table-level vs column-level* (granularity) and *technical vs
  business* (vocabulary). You need all four cells for different jobs, and they are captured by
  different mechanisms.
- **Three capture mechanisms:** *static* (parse SQL/notebooks/DAGs — precise for columns, blind
  at runtime), *runtime* (instrument the engine to emit events — accurate for what actually
  ran, often coarse for columns), and *logs/catalog* (query history, DDL audit, manual
  annotations — cheap, stale, incomplete).
- **OpenLineage is the wire standard**; **Marquez is a reference implementation** (server +
  UI + API). An `OpenLineage` event says "Job X-run Y read datasets {…} and wrote {…}", with
  **facets** for schema, SQL, and **column-level lineage**.
- **dbt gives you table-level lineage for free** from `ref()`/`source()` via `manifest.json`;
  column-level lineage in dbt is derived by parsing compiled SQL or via the semantic layer.
- **Impact analysis** = descendants of an artifact. **Root-cause analysis** = ancestors of a
  bad artifact. Both are graph traversals; distinguish the direction or you'll answer the
  wrong question.
- **Provenance** (which source and which pipeline produced this row/value) is the
  governance/compliance use case: GDPR erasure, audit, and "explain the number".
- The hard parts are: `SELECT *`, aggregations/`GROUP BY` (ambiguous column mapping),
  dynamic SQL, and **keeping the graph fresh** — stale lineage is worse than none because
  people trust it.

## 1. Definitions and core vocabulary

| Term | Meaning |
|------|---------|
| **Lineage** | The record of how data moves and transforms across systems, jobs, and columns. |
| **Provenance** | The origin and history of a specific data item (often for audit/regulatory). |
| **Technical lineage** | Physical artifacts: tables, columns, files, jobs, tasks. |
| **Business lineage** | Business concepts: "Revenue", "Active Customer", mapped to technical artifacts. |
| **Table-level lineage** | Which tables feed which tables. |
| **Column-level (attribute) lineage** | Which columns feed which columns, with transformation semantics. |
| **Upstream / ancestors** | Artifacts that feed the artifact in question (root cause). |
| **Downstream / descendants** | Artifacts the artifact feeds (impact analysis). |
| **Job / Task / Run** | A process, its definition, and one execution instance. |
| **Dataset** | An abstract table/file/topic, namespaced by physical location. |
| **Facet** | A piece of metadata attached to a job/run/dataset (schema, SQL, column lineage…). |
| **DAG** | Directed acyclic graph of dependencies (lineage graph, workflow graph). |
| **Impact analysis** | Determining what is affected by a change/failure (downstream traversal). |
| **Root-cause analysis** | Determining what caused a defect (upstream traversal). |

### 1.1 The 2×2 (granularity × vocabulary)

```
                     TECHNICAL                         BUSINESS
               ┌───────────────────────────┬───────────────────────────┐
   TABLE       │ orders → fct_orders       │ "Revenue" ← "Order       │
   LEVEL       │ (DAG of tables/jobs)      │  Amount" + "Currency"     │
               ├───────────────────────────┼───────────────────────────┤
   COLUMN      │ orders.total_cents →      │ "Gross Margin %" ←        │
   LEVEL       │ fct_orders.total_cents    │  "Revenue" & "COGS"       │
               └───────────────────────────┴───────────────────────────┘
                 captured from SQL &         captured by glossary +
                 runtime events              metric definitions + owners
```

- **Table-level technical** → dependency orchestration, "what's stale", incident routing.
- **Column-level technical** → schema-change impact, PII/PII-flow, precise debugging.
- **Business lineage** → "this number comes from those business rules", glossary and metric
  definitions, regulatory narratives.
- **Column-level business** → metric-level trust ("Gross Margin is defined from Revenue and
  COGS"), attribution of a KPI to upstream data.

> **Senior note.** Most "lineage" products are strong in one cell and weak in the others. When
> evaluating a tool, ask it to answer *four* concrete questions: (1) which tables feed this
> table? (2) which column of which source feeds column `x`? (3) which KPIs does this column
> feed? (4) which PII columns flow into this dataset? A tool that answers one and markets all
> four is common.

## 2. What problem it solves / why it matters

| Problem | Without lineage | With lineage |
|---------|-----------------|--------------|
| **Debugging** ("why is this number wrong?") | Manual archaeology across repos, tickets, Slack | Traverse upstream; see the last failing/changed node |
| **Impact analysis** ("can I drop this column?") | Fear; nobody changes anything | List downstream consumers, notify owners |
| **Schema/deprecation** | Breaking changes surprise consumers | Automated consumer list for a deprecation window |
| **Compliance / GDPR** | Can't prove where data came from or where it went | Provenance: trace PII across the graph, execute erasure |
| **Trust** | "I think this comes from the CRM…" | Auditable "explain this number" |
| **Cost/ownership** | Unknown criticality | Criticality-ranked graph; who owns what |
| **Migration** | Rebuild blind | Coverage: what must be migrated, in what order |

The killer stat: in most organizations, the **time to answer "why is this number wrong?"** is
dominated by *finding the relevant pipelines*, not by *fixing* them. Lineage compresses the
first phase from hours to seconds.

## 3. How it works internally (mechanisms, internals)

### 3.1 Capture mechanisms

```
 ┌───────────────────────────── STATIC (design-time) ─────────────────────────────┐
 │ Parse SQL / Python / notebooks / dbt projects / Airflow DAGs from source.       │
 │ Precise for columns. Knows intent. Blind to what actually ran and to dynamic SQL│
 └────────────────────────────────────────────────────────────────────────────────┘
 ┌──────────────────────────── RUNTIME (execution-time) ──────────────────────────┐
 │ Instrument the engine (Spark/Flink/Airflow/dbt) to emit events as jobs run.     │
 │ Accurate to reality, catches dynamic SQL. Needs the job to actually run.         │
 └────────────────────────────────────────────────────────────────────────────────┘
 ┌──────────────────────────── LOGS / CATALOG (passive) ──────────────────────────┐
 │ Query history, DDL audit, platform logs, manual annotations.                    │
 │ Cheap, broad, but stale, incomplete, and often table-level only.                 │
 └────────────────────────────────────────────────────────────────────────────────┘
```

A mature platform usually **combines all three**: static for column precision at design time,
runtime for truth, logs for coverage and for systems you can't instrument.

### 3.2 How SQL lineage is actually derived (the parser)

Column-level lineage from SQL requires resolving the query into an **AST**, then mapping
**output columns → input columns** through projections, joins, CTEs, unions, and
aggregations:

```sql
WITH o AS (SELECT order_id, customer_id, total_cents FROM raw.orders),
     c AS (SELECT customer_id, country FROM raw.customers)
SELECT o.order_id                          AS order_id,
       c.country                           AS customer_country,
       o.total_cents * 0.01                AS total_usd,
       sum(o.total_cents) OVER (PARTITION BY c.country) AS country_total
FROM o JOIN c USING (customer_id)
```

Column lineage the parser should emit:

| Output column | Input columns | Transformation |
|---------------|---------------|----------------|
| `order_id` | `raw.orders.order_id` | `IDENTITY` (DIRECT) |
| `customer_country` | `raw.customers.country` | `IDENTITY` (DIRECT) |
| `total_usd` | `raw.orders.total_cents` | `INDIRECT` (arithmetic) |
| `country_total` | `raw.orders.total_cents`, `raw.customers.country` | `INDIRECT` (window/aggregate) |

Hard cases (where naive parsers break):

- `SELECT *` → you must resolve `*` against the schema at that point (needs the catalog).
- `GROUP BY` / aggregates → the output is a function of *many* input rows; lineage is
  column-set-level, not row-level.
- Dynamic SQL / templating (`{{ }}`, `EXECUTE IMMEDIATE`) → must be compiled first (rendered),
  then parsed.
- UDFs → opaque unless the UDF body is itself parsed.

### 3.3 OpenLineage's object model (the standard)

OpenLineage models lineage as **events**, not as a graph you edit. Three event types:

- `RunEvent` — "job run reached state S at time T" (`START`, `RUNNING`, `COMPLETE`, `FAIL`,
  `ABORT`). Runtime lineage.
- `JobEvent` — static job metadata (source location, declared inputs/outputs). Design-time.
- `DatasetEvent` — dataset metadata (schema, ownership, docs). Design-time.

Datasets and jobs are augmented by **facets** (atomic metadata blocks keyed by name). Key
facets:

| Facet | Attached to | Carries |
|-------|-------------|---------|
| `schema` | Dataset | Column names/types |
| `dataSource` | Dataset | Physical instance URI |
| `lifecycleStateChange` | Dataset | create/drop/alter/overwrite/rename/truncate |
| `version` | Dataset | e.g. Iceberg snapshot ID |
| `columnLineage` | Dataset (output) | Which input fields feed each output field |
| `sql` | Run | The SQL that ran |
| `sourceCodeLocation` | Job | Repo + git SHA |
| `ownership` | Job/Dataset | Owners |
| `dataQualityMetrics` / `dataQualityAssertions` | Input dataset | Row counts, test results (ties into chapter 01) |
| `outputStatistics` | Output dataset | Written row count / bytes |

### 3.4 A real OpenLineage event (START, with schema + inputs/outputs)

```json
{
  "eventType": "START",
  "eventTime": "2025-01-15T03:00:05.123Z",
  "producer": "https://github.com/OpenLineage/OpenLineage/tree/1.20.0/integration/spark",
  "schemaURL": "https://openlineage.io/spec/2-0-0/OpenLineage.json#/$defs/RunEvent",
  "run": {
    "runId": "3b452093-782c-4ef2-9c0c-aafe2aa6f34d",
    "facets": {
      "nominalTime": {
        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.20.0/integration/airflow",
        "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/NominalTimeRunFacet.json",
        "nominalStartTime": "2025-01-15T03:00:00Z"
      }
    }
  },
  "job": {
    "namespace": "airflow",
    "name": "daily_orders.build_fct_orders",
    "facets": {
      "sourceCodeLocation": {
        "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.20.0/integration/airflow",
        "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/SourceCodeLocationJobFacet.json",
        "type": "git",
        "url": "git@github.com:acme/data-platform.git",
        "path": "dags/orders.py",
        "branch": "main",
        "version": "a1b2c3d"
      }
    }
  },
  "inputs": [
    {
      "namespace": "postgres://prod-crm",
      "name": "public.orders",
      "facets": {
        "schema": {
          "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.20.0/integration/spark",
          "_schemaURL": "https://openlineage.io/spec/facets/1-1-1/SchemaDatasetFacet.json",
          "fields": [
            { "name": "order_id", "type": "string" },
            { "name": "total_cents", "type": "long" }
          ]
        }
      }
    }
  ],
  "outputs": [
    {
      "namespace": "snowflake://acme",
      "name": "ANALYTICS.MARTS.FCT_ORDERS",
      "facets": {
        "columnLineage": {
          "_producer": "https://github.com/OpenLineage/OpenLineage/tree/1.20.0/integration/spark",
          "_schemaURL": "https://openlineage.io/spec/facets/1-1-0/ColumnLineageDatasetFacet.json",
          "fields": {
            "ORDER_ID": {
              "inputFields": [
                { "namespace": "postgres://prod-crm", "name": "public.orders",
                  "field": "order_id",
                  "transformations": [{ "type": "DIRECT", "subtype": "IDENTITY",
                                        "description": "", "masking": false }] }
              ]
            },
            "TOTAL_USD": {
              "inputFields": [
                { "namespace": "postgres://prod-crm", "name": "public.orders",
                  "field": "total_cents",
                  "transformations": [{ "type": "INDIRECT", "subtype": "IDENTITY",
                                        "description": "divide by 100", "masking": false }] }
              ]
            }
          }
        }
      }
    }
  ]
}
```

The `columnLineage` facet is where the four cells of §1.1 become machine-readable.
`transformations[].type` is `DIRECT` (the output value derives from this input value) or
`INDIRECT` (it contributes, e.g. a filter/join/aggregate). `masking: true` marks PII
transformation.

### 3.5 How Marquez consumes it

```
Spark / Airflow / dbt / Flink ──emit HTTP POST /api/v1/lineage──▶ Marquez API
                                                                      │
                                          ┌───────────────────────────┼─────────────────────┐
                                          ▼                           ▼                     ▼
                                   namespace/job/dataset        LINEAGE GRAPH EDGES    column_lineage
                                   registry + versions          (who feeds whom)       (field → field)
```

Marquez exposes REST endpoints (`/api/v1/lineage`, `/api/v1/namespaces`, `/api/v1/jobs`,
`/api/v1/datasets/{ns}/{name}`, `/api/v1/lineage?nodeId=...&depth=...`) and a UI rendering the
graph. It is the reference **backend**; OpenLineage is the **format**. You can point the same
events at DataHub, OpenMetadata, or a custom store.

### 3.6 dbt lineage

dbt's lineage is derived from the DAG of `ref()` and `source()` calls. `dbt build` writes
`target/manifest.json` with every node, its `depends_on.nodes`, and columns:

```jsonc
// target/manifest.json (abridged)
{
  "nodes": {
    "model.analytics.fct_orders": {
      "unique_id": "model.analytics.fct_orders",
      "resource_type": "model",
      "depends_on": { "nodes": ["model.analytics.stg_orders",
                                "model.analytics.dim_customer"] },
      "columns": {
        "order_id": { "name": "order_id", "data_type": "string" },
        "total_usd": { "name": "total_usd", "data_type": "numeric" }
      },
      "compiled_code": "select ... from ..."   // used to derive COLUMN lineage by parsing
    }
  },
  "sources": {
    "source.analytics.raw_orders": {
      "unique_id": "source.analytics.raw_orders",
      "columns": { "order_id": { "name": "order_id" } }
    }
  }
}
```

- **Table-level** lineage = traverse `depends_on.nodes`.
- **Column-level** lineage = parse `compiled_code` (dbt now exposes this; tools like
  `dbt-column-lineage`, Elementary, or catalog integrations compute it), or via the semantic
  layer (`ref()` in metrics).

You can emit OpenLineage from dbt (`dbt-ol`) so that dbt lineage lands in the same backend as
your Spark/Airflow lineage — this is how you get **one** graph instead of three.

## 4. How to implement it (step-by-step, with examples)

### 4.1 Step 1 — Instrument the engines (runtime)

```bash
# Spark: attach the OpenLineage Spark listener
spark-submit \
  --packages io.openlineage:openlineage-spark_2.12:1.20.0 \
  --conf spark.extraListeners=io.openlineage.spark.agent.OpenLineageSparkListener \
  --conf spark.openlineage.transport.type=http \
  --conf spark.openlineage.transport.url=http://marquez:5000 \
  --conf spark.openlineage.namespace=prod-spark \
  myjob.py
```

```ini
# Airflow: OpenLineage provider, set in airflow.cfg or env
[openlineage]
transport = {"type": "http", "url": "http://marquez:5000", "endpoint": "api/v1/lineage"}
namespace = prod-airflow
```

```bash
# dbt: wrap the run so dbt emits OpenLineage events
dbt-ol run --project-dir ./dbt --target prod
```

### 4.2 Step 2 — Emit a custom OpenLineage event (Python client)

```python
from openlineage.client import OpenLineageClient
from openlineage.client.event_v2 import (
    RunEvent, RunState, Run, Job, Dataset,
)
from openlineage.client.uuid import generate_new_uuid

client = OpenLineageClient(url="http://marquez:5000")
run_id = generate_new_uuid()

def emit(state, outputs):
    client.emit(RunEvent(
        eventType=state,
        eventTime=__import__("datetime").datetime.utcnow().isoformat() + "Z",
        run=Run(runId=str(run_id)),
        job=Job(namespace="custom-etl", name="ship_orders"),
        inputs=[Dataset(namespace="postgres://prod-crm", name="public.orders")],
        outputs=[Dataset(namespace="s3://lake", name="curated/shipments")],
        producer="https://acme.com/etl",
    ))

emit(RunState.START, [])
...  # do the work
emit(RunState.COMPLETE, [])
```

### 4.3 Step 3 — Answer the two core questions (graph traversal)

```python
# Pseudocode over a lineage store (Marquez-style or a graph DB)
def descendants(artifact, depth=None):     # IMPACT ANALYSIS
    frontier, seen = [artifact], set()
    while frontier:
        node = frontier.pop()
        for child in EDGES.get(node, []):
            if child not in seen:
                seen.add(child); frontier.append(child)
    return seen

def ancestors(artifact, depth=None):       # ROOT-CAUSE
    return descendants(artifact) if EDGES_ARE_INVERTED else _walk_up(artifact)
```

Practical queries:

```sql
-- Impact analysis: who consumes column total_cents of fct_orders?
SELECT DISTINCT downstream_dataset, downstream_column
FROM column_lineage
WHERE upstream_dataset = 'analytics.marts.fct_orders'
  AND upstream_column  = 'total_cents';

-- Root cause: which upstream jobs ran (or failed) between the last good and first bad output?
SELECT job, run_state, event_time
FROM run_events
WHERE dataset IN (SELECT upstream FROM column_lineage WHERE downstream = 'kpi.revenue')
  AND event_time BETWEEN :last_good AND :first_bad
ORDER BY event_time;
```

### 4.4 Step 4 — Business lineage: bind metrics to technical artifacts

```yaml
# metrics/revenue.yml  (dbt metric / semantic layer)
metrics:
  - name: revenue
    label: "Revenue"
    type: derived
    type_params:
      expr: "sum(total_usd)"
    model: ref('fct_orders')
    dimensions: [country, order_date]
    meta:
      owner: "finance-analytics"
      business_definition: "Gross merchandise value, USD, tax-exclusive"
```

The metric's `model` + `expr` is the bridge from **business** ("Revenue") to **technical**
(`fct_orders.total_usd` → `raw.orders.total_cents`). A catalog (DataHub/OpenMetadata) then
renders business lineage by stitching glossary terms → metrics → columns.

### 4.5 Step 5 — Make lineage operational (the part most teams skip)

1. **Freshness of the graph:** alert if a dataset's lineage hasn't been updated since its
   expected run. Stale lineage is a trap.
2. **Criticality + ownership:** each dataset/node has an owner; alerts route to them.
3. **Deprecation workflow:** "drop column X" → auto-generate the consumer list → open tickets →
   wait the policy window → drop. Lineage *enables the policy*.
4. **PR-time impact:** a schema change PR queries lineage and comments the downstream impact.
5. **PII flow:** tag PII columns; query the graph for downstream reach; drive masking/erasure.

```yaml
# PR-time impact comment (conceptual GitHub Action)
- name: lineage impact
  run: |
    python scripts/impact.py --dataset analytics.marts.fct_orders \
      --change "drop column legacy_flag" --comment-on-pr
```

## 5. Where to implement it (tools/platforms and when each fits)

| Tool | Type | Strength | When to choose |
|------|------|----------|----------------|
| **OpenLineage** | Standard/format + integrations | Vendor-neutral event spec; Spark/Airflow/Flink/dbt integrations | You want one graph across engines |
| **Marquez** | Open-source backend | Reference server, UI, REST API for OpenLineage | Self-hosted OSS lineage backend |
| **DataHub** | Metadata platform | Lineage + catalog + governance + PII | You want one control plane for metadata |
| **OpenMetadata** | Metadata platform | Lineage + catalog + quality + glossary | Similar OSS-neutral choice |
| **dbt** | Transform tool | Table-level lineage free from `manifest.json`; column via parsed SQL/semantic layer | You transform in dbt |
| **Apache Atlas** | Governance (Hadoop-era) | Hive/HBase lineage, classification | Legacy Hadoop estates |
| **Spline** | Spark lineage | Deep Spark execution lineage | Spark-only shops |
| **Unity Catalog / Snowflake ACCESS_HISTORY / BigQuery Lineage** | Cloud-native | Column lineage from query logs | Cloud-warehouse-native and low effort |
| **Collibra / Atlan / Alation** | Commercial catalogs | Business lineage, glossary, stewardship | Regulated enterprise, business users |

**Decision guide:**

- **OSS, engine-agnostic, engineering-first** → OpenLineage + Marquez (or DataHub/OpenMetadata).
- **Cloud warehouse, minimal effort** → start with the warehouse's native lineage
  (Snowflake/BigQuery/Databricks) and export to your catalog.
- **Governance/regulated, business users** → a commercial catalog, fed by OpenLineage events.

## 6. Patterns and anti-patterns

### Patterns

| Pattern | Why |
|---------|-----|
| **Emit at runtime, augment statically** | Runtime is truth; static/design adds column precision and future deps |
| **Standardize on OpenLineage** | Decouples emitters from the backend; swap backends freely |
| **Lineage-driven deprecation** | Automatic consumer lists make breaking changes safe |
| **PR-time impact analysis** | Catch breakage at review, not in prod |
| **PII tags + lineage queries** | Regulatory reach and erasure |
| **Criticality-ranked graph** | Prioritize what to protect/monitor |
| **Idempotent, representable events** | Run/complete pairs let the backend reconcile |

### Anti-patterns

| Anti-pattern | Consequence |
|--------------|-------------|
| **Stale lineage** | People trust a wrong graph; worse than none |
| **Table-level only, marketed as column-level** | Can't answer "which column breaks" |
| **`SELECT *` everywhere** | Column lineage degrades to "everything feeds everything" |
| **Lineage captured only from one engine** | Graph has islands; cross-engine questions fail |
| **No provenance/identifiers** | Can't explain or legally trace a value |
| **Lineage as a static diagram** | Rots immediately; nobody updates hand-drawn maps |
| **Ignoring UDFs/dynamic SQL** | Silent blind spots exactly where complexity is high |
| **No ownership attached** | You find the impact but not who to call |

## 7. Real-world use cases and industry applications

- **Incident response.** A KPI is wrong. Traverse **upstream** from the KPI, find the upstream
  job that failed or the schema change that landed, and the blast radius **downstream**. This
  is the single most common use; it's why lineage and observability ship together (chapter 04).
- **Safe deprecation.** "We're dropping `customer_id` from `orders`." Lineage produces the
  exact list of 47 downstream models, 12 dashboards, and 3 ML features, and drives the
  deprecation ticket.
- **Regulatory / GDPR.** Article 30 records of processing, DSARs ("delete this customer" →
  find every dataset the customer's PII reached), and "explain this number" for a regulator.
  Provenance is the artifact.
- **Finance / audit.** SOX-style controls: who changed the pipeline, what data it touched,
  what reports it produced. Lineage + `sourceCodeLocation` (git SHA) is the audit trail.
- **Data migration.** Move a warehouse: lineage gives coverage (what must move), order
  (topological), and a checklist for validated parity.
- **Cost attribution / ownership.** Which datasets feed which marts → charge-back and
  ownership assignment.
- **ML.** Feature lineage: which source columns trained a model, for reproducibility and for
  "why did the model change" investigations.

## 8. Senior-level deep dive

### 8.1 Trade-offs

| Decision | Option A | Option B | Depends on |
|----------|----------|----------|-----------|
| Capture | Static (parse) | Runtime (emit) | Need for column precision vs truth |
| Backend | Marquez/OSS | Commercial catalog | Budget, governance, business users |
| Granularity | Table-level | Column-level | Compliance (PII), schema-change safety |
| Storage | Graph DB | RDBMS/OLAP | Query patterns, scale |
| Scope | One engine | Multi-engine | Platform heterogeneity |
| Freshness | Real-time events | Nightly batch | Incident SLAs |
| Build vs buy | Instrument + OSS | Vendor | Team size, time-to-value |

### 8.2 Architecture decisions & scale

- **Event volume.** A large Spark estate can emit millions of events/day. Store raw events in a
  queue/log, aggregate into the graph, and keep the hot graph in a queryable store. Backends
  (Marquez/DataHub) do this; your job is to **size it** and set **retention**.
- **Namespacing is the join key.** Datasets must be named consistently across engines
  (`postgres://host/db/schema/table`) or the graph breaks into islands. Naming contract first,
  instrument second.
- **Idempotency.** Events can be re-delivered. The backend must upsert by `runId` + event type,
  not blindly append.
- **Column-level cost.** Computing column lineage for every query is expensive; many stacks
  compute it **sampled** or **on demand** for critical datasets, and table-level for the rest.
  Know which mode you're in.
- **Graph vs lineage *of lineage*.** Meta-lineage (how the graph itself is built) matters when
  debugging the lineage system.

### 8.3 Failure modes and debugging

| Failure mode | Symptom | Diagnosis |
|--------------|---------|-----------|
| **Stale graph** | Missing recent edges; wrong impact set | Compare `max(event_time)` per dataset to its schedule |
| **Islands** | Cross-engine impact analysis finds nothing | Inconsistent namespaces; missing emitters |
| **Spurious edges** | `SELECT *` / unparsed SQL links everything to everything | Improve parsing; replace `*`; resolve via catalog |
| **Missing column lineage** | Facet absent on datasets | Emitter version, dynamic SQL, or UDF opacity |
| **Double counting** | Same run recorded twice | Non-idempotent emit; dedupe by `runId` |
| **Wrong direction** | "Impact" shows upstream | Confused traversal; fix API/query |
| **Lineage says X→Y but data flows Y→X** | Homonym/alias datasets | Namespace normalization |
| **Unbounded growth** | Storage cost explodes | Retention + aggregation policy |

### 8.4 Org and process

- **Lineage is infrastructure, not a project.** It must be instrumented by default in the
  platform (DAG runner, compute engine, transform tool), not maintained by hand.
- **Ownership:** platform team owns the emitters and the backend; **dataset owners** keep
  metadata (schema, owners, PII tags) accurate; governance owns the business glossary.
- **Processes lineage enables:** change review (PR impact), deprecation windows, incident
  triage, access reviews, DSARs. If none of these *use* the graph, it will decay.
- **Success metric:** *time to answer* a lineage question (root cause, impact) — not the
  number of nodes in the graph.

## 9. Common mistakes and how to avoid them

1. **Capturing lineage but never *using* it.** → Wire it into PR reviews, deprecation, and
   on-call; otherwise it rots.
2. **Table-level only.** → If you have PII or schema-change risk, you need column level for at
   least the critical datasets.
3. **`SELECT *`.** → It destroys column lineage; enumerate columns or resolve via catalog.
4. **Inconsistent dataset naming.** → Define a naming contract before instrumenting.
5. **Relying on one emitter.** → Cross-engine questions fail; instrument Airflow + Spark +
   dbt + the warehouse.
6. **No freshness monitoring of the graph.** → Stale lineage is worse than none.
7. **Confusing impact with root cause.** → Always state the direction; get the traversal right.
8. **Hand-maintained diagrams.** → They're wrong within a week; generate from the graph.
9. **No ownership binding.** → You find the problem, not the person.
10. **Ignoring business lineage.** → Executives ask in business terms; translate via metrics and
    a glossary.

## 10. Exercises / interview questions

1. *Granularity:* Give a concrete question only column-level lineage can answer and one only
   table-level can. *(Hint: schema change vs orchestration.)*
2. *Mechanisms:* Why can't static parsing alone be trusted for lineage, and why can't runtime
   alone be complete? *(Hint: dynamic SQL vs never-run jobs.)*
3. *OpenLineage:* Name the three event types and the facet that carries column-level lineage.
   *(Hint: RunEvent/JobEvent/DatasetEvent; columnLineage.)*
4. *Traversal:* A KPI is wrong. Do you traverse up or down first, and why? What do you query?
5. *`SELECT *`:* Explain precisely how `SELECT *` degrades column lineage and one way to fix it.
6. *dbt:* Where does dbt store its DAG, and how would you derive column lineage from it?
7. *Scale:* Estimate the event volume for 500 Spark jobs each running hourly with 10
   datasets, and propose a storage/retention design.
8. *Provenance:* How does lineage support a GDPR erasure request end-to-end?
9. *Failure mode:* Your impact analysis misses a downstream dashboard. Enumerate five reasons.
   *(Hint: islands, stale, `*`, business-only, un-instrumented BI.)*
10. *Org:* Which processes must consume lineage for it to survive? Pick three and justify.

## References

- OpenLineage — object model: https://openlineage.io/docs/spec/object-model/
- OpenLineage — spec (JSON schema, facets): https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.md
- OpenLineage — column-level lineage: https://openlineage.io/blog/column-lineage/
- OpenLineage — integrations: https://openlineage.io/docs/integrations/
- Marquez — project site: https://marquezproject.ai/
- Marquez — column-level lineage announcement/demo: https://marquezproject.ai/blog/column-lineage-demo/
- dbt — project/manifest & lineage: https://docs.getdbt.com/reference/artifacts/manifest-json
- dbt — `ref` and DAG: https://docs.getdbt.com/reference/dbt-jinja-functions/ref
- DataHub — lineage: https://docs.datahub.com/docs/features/feature-guides/lineage
- OpenMetadata — lineage: https://docs.open-metadata.org/latest/features/lineage
- Apache Atlas: https://atlas.apache.org/
- Spline (Spark lineage): https://absaoss.github.io/spline/
- Google Cloud — Data Lineage in Dataplex Universal Catalog: https://cloud.google.com/dataplex/docs/data-lineage
- Snowflake — ACCESS_HISTORY view: https://docs.snowflake.com/en/sql-reference/account-usage/access_history
