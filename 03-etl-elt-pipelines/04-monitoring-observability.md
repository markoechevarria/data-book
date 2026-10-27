# Monitoring and Observability for Data Pipelines

> Traditional monitoring tells you *that* something is broken ("the task failed"). Observability tells
> you *what* is broken, *why*, *who* is affected, and *what to do* — including the failure mode that
> kills data platforms: the pipeline that reports **success** while producing wrong, stale, or missing
> data. A senior engineer designs three layers: infrastructure (did the job run?), pipeline (did it
> produce the right shape?), and data (is the data correct, fresh, and complete?). Only the third
> layer protects the business.

---

## TL;DR

- **Three layers:** infra metrics (CPU, memory, task status), pipeline metrics (runtime, lag,
  retries, row counts), and **data observability** (freshness, volume, schema, distribution, lineage).
- **The most dangerous incident is a green pipeline with bad data.** Alert on data properties, not
  just on task exit codes.
- **SLOs make "good" measurable.** Define freshness/completeness/accuracy objectives with explicit
  error budgets; alert on burn rate, not on single noisy events.
- **Freshness is the highest-value metric** for most pipelines: `now() - max(event_time)`. It directly
  maps to consumer trust.
- **Alert on symptoms (SLO violations), page on user impact, and track everything else in dashboards.**
  Alert fatigue destroys on-call effectiveness faster than any outage.
- **Lineage turns a single failure into a blast-radius answer**: "these 47 downstream tables and 12
  dashboards are affected."
- **The five pillars of data observability:** freshness, volume, schema, distribution/quality, lineage.
- **Instrument at the boundary, not deep in the code**: standardize a small set of metrics, emitted by
  every pipeline, using OpenTelemetry conventions.
- **Every alert must have a runbook**; an alert with no action is noise.

---

## 1. Definitions and core vocabulary

| Term | Definition |
|------|-----------|
| **Monitoring** | Watching known signals against thresholds. |
| **Observability** | Being able to answer *arbitrary new questions* about system state from its outputs. |
| **Metric** | Numeric time series (counters, gauges, histograms). Cheap, aggregated. |
| **Log** | Structured event records. Rich, expensive at scale. |
| **Trace** | A correlated path of a request/job across components (span tree). |
| **Span** | One unit of work inside a trace, with parent/child relationships. |
| **SLI** | Service Level Indicator — the *measured* signal (e.g., % of runs on time). |
| **SLO** | Service Level Objective — the internal target for an SLI (e.g., 99% on time). |
| **SLA** | Service Level Agreement — an SLO with contractual/financial consequences. |
| **Error budget** | `1 − SLO`. How much failure you can afford in a period. |
| **Burn rate** | How fast you're consuming the error budget; multi-window burn-rate alerts are best practice. |
| **Data freshness** | How old the newest available data is, vs. when it should have arrived. |
| **Data volume** | Row/byte counts per batch/partition; anomalies signal upstream gaps. |
| **Data schema** | Column names/types/nullability; drift breaks consumers. |
| **Data distribution** | Statistical shape (null rate, min/max, cardinality) — catches silent corruption. |
| **Data lineage** | The graph of how datasets derive from other datasets. |
| **Blast radius** | All assets affected by a given failure (computed via lineage). |
| **MTTD / MTTR** | Mean time to detect / to resolve. The two numbers on-call is judged by. |
| **Data downtime** | Time during which data is missing, wrong, or stale. |

---

## 2. What problem it solves / why it matters

### 2.1 The four questions on-call must answer at 3 a.m.

1. **Is it broken?** (detection)
2. **What is broken, and since when?** (localization)
3. **Who is affected?** (impact/blast radius)
4. **What do I do?** (runbook, rollback, backfill)

A pipeline without observability answers none of them.

### 2.2 Why the traditional approach fails

| Traditional | Why it fails for data |
|-------------|----------------------|
| Alert on task failure | Misses silent data errors (empty, stale, wrong, duplicated) |
| Watch CPU/memory | The pipeline can be "healthy" and producing garbage |
| Manual spot checks | Does not scale past ~20 tables; no history |
| Dashboards nobody owns | Rot; drift from reality |
| Alerts on every error | Fatigue; real incidents get ignored |
| "The data team will notice" | Nobody notices until an executive does |

### 2.3 The business cost model

Data downtime costs: a revenue report that is wrong for a day, a model retrained on stale features, an
invoice sent with bad numbers, a compliance filing with a gap. The cost grows with *detection time*,
not with the *number* of failures — which is why MTTD is the primary metric to improve.

---

## 3. How it works internally

### 3.1 The three layers

```text
┌────────────────────────────────────────────────────────────────────────────┐
│ L3  DATA OBSERVABILITY   freshness · volume · schema · distribution · lineage│  → "is the DATA right?"
├────────────────────────────────────────────────────────────────────────────┤
│ L2  PIPELINE / JOB       runtime · retries · lag · rows in/out · queue depth│  → "did it do its job?"
├────────────────────────────────────────────────────────────────────────────┤
│ L1  INFRASTRUCTURE       CPU · memory · disk · network · scheduler health   │  → "is the platform up?"
└────────────────────────────────────────────────────────────────────────────┘
```

A senior engineer treats L3 as the *primary* alerting surface for data pipelines, L2 as the diagnostic
surface, and L1 as the "everything is on fire" surface.

### 3.2 Metrics, logs, traces — what each is good for

| Signal | Cost | Best for | Cardinality limit |
|--------|------|----------|-------------------|
| **Metric** | Low | Rates, latencies, counters, alerting | Keep labels bounded (no `user_id`!) |
| **Log** | High | Root cause, error detail, audit | Sampled/indexed |
| **Trace** | Medium | Cross-component latency, distributed pipelines | Sampled |

> **Senior note:** Metrics with unbounded label cardinality (e.g., `task_id` on a DAG with 10,000
> tasks, or `table_name` across 50,000 tables) will melt your metrics backend. Aggregate, or use a
> different store (a metadata/observability service) for high-cardinality dimensions.

### 3.3 Internals: how job-level telemetry is produced

- **Airflow:** the metadata DB is the source of truth for `DagRun`/`TaskInstance` states and
  durations; `airflow.metrics`/StatsD emits scheduler and task metrics; the REST API and the UI read
  from the DB. Airflow 3 also emits OpenTelemetry traces so a DAG run can be correlated with the
  systems it touches.
- **Spark:** the Spark UI / REST API (event logs) expose stages, tasks, shuffle, spill, and GC.
  Streaming queries expose `inputRowsPerSecond`, `processedRowsPerSecond`, and `batchDuration`.
- **Flink:** metrics via the metric registry (Prometheus reporter) — checkpoint duration/size,
  `numRecordsInPerSecond`, `currentInputWatermark`, backpressure indicators.
- **Kafka:** broker and consumer metrics, plus **consumer lag** (`kafka_consumergroup_lag`) which is
  *the* health metric for streaming pipelines.
- **dbt:** run results, test failures, source freshness, artifacts (`manifest.json`, `run_results.json`,
  `sources.json`) — the machine-readable output that observability tools consume.
- **OpenLineage:** a standard event schema (`START`/`COMPLETE`/`FAIL`/`ABORT` run events plus
  dataset/job facets) emitted by Airflow, Spark, dbt, etc., to build lineage and run history
  independently of any single tool.

### 3.4 SLO mechanics and burn-rate alerting

```text
SLI  = fraction of partitions delivered before their deadline this month
SLO  = 99.5%                 (error budget = 0.5% ≈ 3.6 hours/month)
Alert = multi-window burn rate:
        - fast: 14.4× burn over 1h AND 5m  → page (budget gone in ~2 days)
        - slow: 1×   burn over 3d AND 6h   → ticket
```

Multi-window burn-rate alerting (the Google SRE approach) is the right pattern: it pages on *real*
fast-moving problems and creates tickets for slow drift, without alerting on every blip.

### 3.5 Freshness internals

```text
expected_arrival  = last_successful_run_end + max_acceptable_delay
actual_freshness  = now() - max(event_time)          -- event-time freshness
lag               = expected_arrival - last_success  -- schedule freshness

alert if actual_freshness > threshold  OR  lag > threshold
```

Two distinct freshness notions that people conflate:

- **Schedule freshness** — did the pipeline run on time? (System health.)
- **Data freshness** — how old is the newest row? (Consumer impact.)

A pipeline can be perfectly on schedule and still produce stale data (e.g., the source stopped
publishing). Monitor **both**.

---

## 4. How to implement it

### 4.1 Emit pipeline metrics from Airflow

```python
import time, logging
from airflow.sdk import task
from statsd import StatsClient

statsd = StatsClient(host="graphite", port=8125, prefix="pipeline")

log = logging.getLogger(__name__)

@task(on_failure_callback=lambda ctx: statsd.incr("task.failure", tags=[f"pipeline:{ctx['dag'].dag_id}"]))
def transform(day: str, **context) -> dict:
    started = time.time()
    rows_in, rows_out = run_transform(day)
    duration = time.time() - started

    statsd.timing("job.duration_ms", duration * 1000,
                  tags=[f"pipeline:orders", f"task:transform"])
    statsd.gauge("job.rows_in", rows_in,  tags=[f"pipeline:orders"])
    statsd.gauge("job.rows_out", rows_out, tags=[f"pipeline:orders"])

    if rows_out == 0:
        raise ValueError(f"zero rows for {day} — refusing to publish")

    log.info("transform_complete", extra={
        "pipeline": "orders", "interval": day,
        "rows_in": rows_in, "rows_out": rows_out, "duration_s": round(duration, 2),
    })
    return {"rows_in": rows_in, "rows_out": rows_out}
```

### 4.2 Metrics + traces with OpenTelemetry (vendor-neutral)

```python
from opentelemetry import trace, metrics
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

tracer = trace.get_tracer("data-pipelines")
meter  = metrics.get_meter("data-pipelines")

rows_processed = meter.create_counter("pipeline.rows.processed")
run_duration   = meter.create_histogram("pipeline.run.duration_seconds")

def run_pipeline(day: str):
    with tracer.start_as_current_span("pipeline.run") as span:
        span.set_attribute("pipeline.name", "orders")
        span.set_attribute("data.interval.start", day)
        with tracer.start_as_current_span("extract"):
            n = extract(day)
        with tracer.start_as_current_span("transform"):
            m = transform(day)
        rows_processed.add(m, {"pipeline": "orders", "stage": "transform"})
        run_duration.record(time.time() - t0, {"pipeline": "orders"})
        return m
```

Standard attribute names make your telemetry portable across backends (Prometheus, Datadog, Grafana,
Honeycomb) and let you correlate pipeline runs with the services they touch.

### 4.3 Data-freshness and volume monitors

```sql
-- Freshness + volume, run hourly, one row per table
SELECT
  'analytics.fct_orders'                                 AS table_name,
  MAX(order_date)                                        AS max_partition,
  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(_ingested_at), MINUTE) AS freshness_minutes,
  COUNT(*)                                               AS rows_last_partition
FROM analytics.fct_orders
WHERE order_date = (SELECT MAX(order_date) FROM analytics.fct_orders);
```

```python
# Anomaly-aware volume check: compare to the trailing distribution, not a fixed threshold
def check_volume(table: str, partition: str, observed: int):
    hist = recent_row_counts(table, partitions=previous_n_days(28))
    mean, sd = statistics.mean(hist), statistics.stdev(hist)
    z = (observed - mean) / (sd or 1)
    if observed == 0:
        alert(f"{table} partition {partition} is EMPTY", severity="page")
    elif abs(z) > 4:
        alert(f"{table} partition {partition} volume anomaly: {observed} vs mean {mean:.0f} (z={z:.1f})",
              severity="ticket")
```

Fixed thresholds break as data grows. **Statistical/ML-based anomaly detection** (or at least
seasonal baselines) is what makes volume monitoring survive growth.

### 4.4 dbt tests + source freshness as code

```yaml
# models/schema.yml
models:
  - name: fct_orders
    description: "One row per order line, daily grain."
    columns:
      - name: order_id
        tests: [unique, not_null]
      - name: customer_id
        tests:
          - not_null
          - relationships:
              to: ref('dim_customer')
              field: customer_id
      - name: order_total
        tests:
          - not_null
          - dbt_utils.accepted_range: {min_value: 0}
    tests:
      - dbt_utils.recency:            # freshness as a test
          datepart: day
          field: order_date
          interval: 1
      - dbt_utils.expression_is_true:
          expression: "order_total >= 0"
```

```yaml
# models/sources.yml — freshness for raw sources
sources:
  - name: raw
    freshness:
      warn_after: {count: 6, period: hour}
      error_after: {count: 12, period: hour}
    loaded_at_field: _loaded_at
    tables:
      - name: orders
```

```bash
dbt source freshness     # fails the job if sources are stale
dbt test --select fct_orders
dbt build                # run + test + snapshot in correct order
```

### 4.5 Alert routing (severity, ownership, runbooks)

```yaml
# alert-rules.yml (Prometheus style, abbreviated)
groups:
  - name: data-pipelines
    rules:
      - alert: PipelineRunLate
        expr: |
          time() - pipeline_last_success_timestamp_seconds{pipeline="orders"} > 3 * 3600
        for: 10m
        labels: {severity: page, team: data-platform, runbook: "runbooks/orders-late.md"}
        annotations:
          summary: "orders pipeline is 3h late"
      - alert: ConsumerLagGrowing
        expr: deriv(kafka_consumergroup_lag{topic="orders"}[15m]) > 0 and kafka_consumergroup_lag{topic="orders"} > 100000
        for: 20m
        labels: {severity: page, runbook: "runbooks/kafka-lag.md"}
      - alert: FreshnessSLOBreach
        expr: data_freshness_minutes{table="analytics.fct_orders"} > 120
        for: 15m
        labels: {severity: ticket}
```

Rules of good alerting:

1. **Symptom-based** (user-visible impact), not cause-based.
2. **Actionable** — every alert links to a runbook with a fix.
3. **Owned** — a team, not a person.
4. **Severity-tiered** — page / ticket / dashboard-only.
5. **Tuned** — measure your false positive rate and delete noisy alerts.

### 4.6 A pipeline health dashboard (what to put on it)

| Panel | Question it answers |
|-------|---------------------|
| Run success rate (7d rolling) | Are runs reliable? |
| Freshness per critical table | Is data current? |
| End-to-end latency (source event → dashboard) | How fast is the platform? |
| Retries & failures by pipeline | Where is the fragility? |
| Consumer lag (streams) | Are we falling behind? |
| Row counts vs. expected (with bands) | Is volume normal? |
| Test failures by severity | Quality trend |
| MTTR / MTTD trend | Are we getting better? |
| Cost per pipeline | Is it affordable? |
| Top-N longest tasks | Where is the time going? |

Keep **one "golden signals" dashboard per domain** with 6–8 panels; deep dives go elsewhere. A
dashboard nobody reads is a liability — the golden dashboard must be the first thing on-call opens.

---

## 5. Where to implement it (tools and when each fits)

| Category | Tools | When to use |
|----------|-------|-------------|
| Metrics + alerting | Prometheus + Alertmanager, Grafana, Datadog, CloudWatch, New Relic, Dynatrace | Platform-wide infra + pipeline metrics |
| Logs | Loki, Elasticsearch/OpenSearch, CloudWatch Logs, Splunk, Datadog Logs | Root-cause investigation |
| Traces | Jaeger, Tempo, Honeycomb, Datadog APM, OpenTelemetry Collector | Cross-system correlation |
| Pipeline-native UI | Airflow UI/REST, Dagster UI + `dagster.yaml`, Prefect UI/API | Job-level truth & manual ops |
| dbt artifacts | `run_results.json`, `manifest.json`, Elementary | Self-hosted dbt observability |
| Data observability | Monte Carlo, Bigeye, Soda, Great Expectations, Elementary, Anomalo | Freshness/volume/schema/lineage at scale |
| Lineage | OpenLineage, Marquez, DataHub, OpenMetadata, Atlan, Collibra | Blast radius + column-level lineage |
| Data quality tests | Great Expectations, Soda Core, dbt tests, Deequ, Glue Data Quality | Rule-based validation |
| Incident management | PagerDuty, Opsgenie, Grafana OnCall, FireHydrant | Paging + postmortems |
| Status/communication | Statuspage, Slack bots, Teams | Consumer-facing comms |

**When each fits:**

- **Build** infra + pipeline metrics (you need them specifically for your platform).
- **Buy (or adopt OSS)** data observability for cross-table freshness/volume/lineage — the cost of
  building and maintaining it is high and it's not your differentiator.
- **Use OpenLineage** to keep lineage portable and vendor-neutral.

---

## 6. Patterns and anti-patterns

### 6.1 Patterns

| Pattern | Purpose |
|---------|---------|
| **Three-layer monitoring** | Infra, pipeline, data — each answers a different question |
| **Freshness + volume + schema monitors** | The minimum viable data observability |
| **SLO with error budget** | Makes "healthy" quantitative and reviewable |
| **Multi-window burn-rate alerts** | Pages on real problems; tickets for drift |
| **Runbook per alert** | Turns paging into a procedure |
| **Lineage-driven blast radius** | Answers "who is affected?" instantly |
| **Shift-left quality (tests in CI)** | Catch issues before they reach prod |
| **Canary / shadow runs** | Validate a new pipeline version without consumers seeing it |
| **Anomaly detection with seasonality** | Volume/freshness that survives growth |
| **Structured logs with consistent keys** | Enables querying across pipelines |
| **Correlation IDs / run IDs everywhere** | Ties logs, traces, and runs together |
| **Consumer-lag-based autoscaling** | Scales consumers on the signal that matters |
| **Data contracts** | Producers commit to a schema/SLA; violation is an incident |

### 6.2 Anti-patterns

| Anti-pattern | Consequence |
|--------------|-------------|
| **Only alerting on task failure** | Silent data corruption goes unnoticed |
| **Alert on every retry/error** | Alert fatigue; real incidents ignored |
| **Thresholds hard-coded to a fixed number** | Breaks as data grows |
| **Unbounded metric cardinality** | Metrics backend melts / bill explodes |
| **Dashboards with 40 panels** | Nobody finds anything |
| **Logging everything at INFO** | Cost + noise; hard to find the signal |
| **No correlation ID** | Cannot tie a Spark job to the DAG run that launched it |
| **"Freshness" = last run time only** | Misses stale-source cases |
| **No owner on a dashboard/alert** | Rot; nobody acts |
| **Alerts without runbooks** | On-call improvises at 3 a.m. |
| **Monitoring only in prod** | No baseline; no trend; no early warning |
| **Ignoring cost as a signal** | Surprise invoice is an incident too |
| **Manual spot checks instead of automated tests** | Does not scale |

---

## 7. Real-world use cases

**E-commerce revenue dashboard.** The #1 SLO is freshness of `fct_orders` at 07:00 local time.
Observability: freshness monitor with a 30-minute deadline, volume band per weekday (weekends differ),
schema contract with the checkout team, lineage to 9 dashboards, and a page to the on-call when the
burn rate exceeds 14×.

**Fintech ledger.** Accuracy SLO: daily reconciliation must match to the cent. Observability: the
reconciliation job *is* the monitor; a mismatch pages immediately with the diff and the affected
transaction IDs. Distribution checks catch "all values are null" classes of bugs.

**Streaming IoT.** The key metric is consumer lag and watermark lag. Observability: lag-based
autoscaling, backpressure alerts, and a dead-letter-rate monitor. A rising DLQ rate is the earliest
indicator of a schema change in the field.

**SaaS product analytics.** Reverse ETL pushes segments to the CRM. Freshness SLO drives marketing
campaigns. A freshness breach is a business incident, not just an engineering one — so the alert goes
to a shared channel with business stakeholders.

**Marketplace supply/demand marts.** Seasonal volume makes fixed thresholds useless; the team uses
weekday-adjusted baselines and alerts on z-score > 4.

**Healthcare claims processing.** Regulatory completeness: every submitted claim must appear in the
processed table within 24 hours. Observability includes a count reconciliation against the intake
ledger and an audit trail of every rejection.

---

## 8. Senior-level deep dive

### 8.1 Choosing SLIs: a decision framework

Ask: **what does the consumer care about?**

| Consumer | Primary SLI | Secondary | Typical SLO |
|----------|-------------|-----------|-------------|
| BI dashboard user | Freshness (dashboard ready by 07:00) | Completeness | 99% of days ready by deadline |
| ML training pipeline | Point-in-time correctness | Freshness | 100% feature/time consistency |
| Operational app (reverse ETL) | Freshness (< 15 min lag) | Availability | 99.9% within 15 min |
| Finance/reporting | Accuracy (reconciliation) | Freshness | 100% reconciled |
| Data scientist (ad-hoc) | Availability of tables | Freshness | 99% of tables queryable at 09:00 |
| Regulator | Completeness + auditability | Freshness | 100% complete, 7-year retention |

> **Senior note:** Freshness SLOs must be **per-consumer**. The same table can have a 07:00 SLO for
> the executive dashboard and a "best effort" expectation for ad-hoc analysis. One number for all
> consumers guarantees either over-alerting or under-protection.

### 8.2 Alerting strategy: what pages vs. what doesn't

| Condition | Severity | Why |
|-----------|----------|-----|
| Critical table violates freshness SLO | **Page** | User-visible, time-sensitive |
| Reconciliation mismatch | **Page** | Financial/regulatory |
| Stream lag growing for 20 min, no recovery | **Page** | Will get worse, cannot self-heal |
| Single task retry succeeded | Dashboard only | Self-healed |
| DLQ rate 0.001% | Ticket | Investigate next business day |
| DLQ rate > 5% | **Page** | Likely a systemic break |
| Test failure on a non-critical model | Ticket | Quality drift |
| Volume anomaly z > 4 | Ticket (or page if critical) | Could be an upstream gap |
| Cost anomaly | Ticket | Budget concern |
| Schema drift | Page or ticket | Contract violation |

The rule: **page only for "user-visible impact, now, and no self-healing".** Everything else is a
ticket or a dashboard.

### 8.3 Failure modes in the observability system itself

| Failure | Symptom | Mitigation |
|---------|---------|-----------|
| **Alert storm** | Hundreds of pages from one root cause | Dependency-aware alert suppression; aggregate by pipeline |
| **Metric cardinality explosion** | Backends OOM / huge bill | Bound labels; drop/aggregate high-cardinality dimensions |
| **Stale monitor** | Monitor itself stopped running | Watchdog: monitor the monitors, heartbeat alerts |
| **False negative from fixed thresholds** | No alert while data is broken | Statistical/seasonal baselines + tests |
| **Noisy freshness monitor on a variable source** | Constant alerts | Per-source deadlines derived from source SLA |
| **Lineage gaps** | Blast radius incomplete | Emit OpenLineage from all engines (Airflow/Spark/dbt) |
| **Sampled traces miss failures** | Cannot debug rare errors | Tail-based sampling (keep error/slow traces) |
| **Log volume cost** | Bills exceed pipeline costs | Structured events at boundaries; sample verbose logs |
| **Alerts to a dead channel** | Nobody sees the page | Test the paging path quarterly |

### 8.4 Debugging a data incident — a procedure

1. **Confirm and scope.** Is it real? Which interval(s), which tables, which consumers?
2. **Check the pipeline's own SLOs.** Run success, freshness, volume — where is the break?
3. **Read the lineage graph.** What upstream inputs changed? What downstream is affected?
4. **Compare to the previous period.** Row counts, null rates, distinct counts — what changed?
5. **Look at the boundary that failed.** Extract? Load? Transform? Publication? Dependencies?
6. **Check for recent changes.** Deploys, schema changes, source changes, code changes.
7. **Communicate.** Status update to consumers with impact and ETA (even if ETA is "unknown").
8. **Fix, then backfill.** Use the idempotent re-run path from
   [05-recoverability.md](./05-recoverability.md).
9. **Postmortem + a new monitor.** Every incident must produce at least one new automated check, or it
   will happen again undetected.

### 8.5 Performance considerations

- **Instrument at boundaries, not in loops.** Emitting a metric per record will destroy throughput.
- **Use counters/histograms, not per-event metrics.** Push cardinality down.
- **Sampling:** trace 1–10% of runs, but **always keep failures and slow runs** (tail-based sampling).
- **Async export:** buffered, non-blocking exporters so telemetry never slows the pipeline.
- **Retention tiers:** high-resolution metrics for 15 days, downsampled for 1–2 years.
- **Cost of observability** should be a tracked line item; typically aim for a small single-digit
  percentage of platform cost.

---

## 9. Common mistakes and how to avoid them

1. **Monitoring infra but not data.** *Fix:* add freshness/volume/schema monitors to every critical
   table.
2. **Alerting on everything.** *Fix:* severity tiers + SLOs; delete alerts with no action.
3. **No owner.** *Fix:* every alert/table has a team and a runbook link.
4. **Fixed thresholds.** *Fix:* baselines, seasonality, z-scores, or ML anomaly detection.
5. **High-cardinality metric labels.** *Fix:* bound labels; store high-cardinality dimensions in a
   metadata store, not the metrics backend.
6. **No lineage.** *Fix:* emit OpenLineage from Airflow/Spark/dbt; store in a catalog.
7. **"Success" means "done".** *Fix:* treat zero rows, null spikes, and schema drift as failures.
8. **No correlation between orchestrator and engine.** *Fix:* propagate a run/attempt ID into Spark,
   warehouse queries, and logs.
9. **Dashboards with no narrative.** *Fix:* one golden signals dashboard per domain; deep dives
   separate.
10. **No incident process.** *Fix:* defined severities, comms channels, and blameless postmortems.
11. **Monitoring only in production.** *Fix:* same instrumentation in staging for baselines.
12. **No monitoring of the monitors.** *Fix:* heartbeats/watchdog alerts on the observability stack.
13. **Ignoring cost signals.** *Fix:* alert on cost anomalies like any other SLO.
14. **Not measuring MTTD/MTTR.** *Fix:* track them; they are the outcomes that improve reliability.

---

## 10. Exercises / interview questions

1. Design the observability stack for a 300-table warehouse with a 5-person team. What do you build,
   buy, and ignore? *(Hint: OpenLineage + dbt artifacts + Prometheus/Grafana + a bought data
   observability tool for anomaly detection.)*
2. Your freshness SLO is "ready by 07:00". Write the SLI, the SLO, and the burn-rate alert rule.
   *(Hint: fraction of days ready by deadline; multi-window burn rate.)*
3. A pipeline "succeeds" but `fct_orders` has 0 rows for last night. Which monitor should have caught
   it, and why didn't "task success" catch it? *(Hint: volume monitor; task exit code ≠ data quality.)*
4. Distinguish schedule freshness from data freshness with an example where one is fine and the other
   is broken.
5. Your volume alert fires every Monday because weekends are low. How do you fix it without disabling
   the alert? *(Hint: weekday-seasonal baselines.)*
6. How do you compute blast radius? What metadata do you need, and where does it come from?
7. Compare metrics, logs, and traces for debugging a 4-hour Spark job that occasionally hangs. Which
   do you reach for first, and why?
8. What is the difference between an alerting rule and a data quality test? When do you use each?
9. You have 40 alerts/day, 38 of which are false positives. Walk through your remediation plan.
10. How would you monitor a streaming pipeline's *correctness* (not just its liveness)?
11. What metric would you use to detect duplicate rows introduced by a non-idempotent backfill?
    *(Hint: key uniqueness / count vs distinct count trends.)*
12. Design a data contract for a producer team and the monitors that enforce it day-to-day.

---

## References

- Google SRE Book — Monitoring distributed systems: https://sre.google/sre-book/monitoring-distributed-systems/
- Google SRE Workbook — Alerting on SLOs (multi-window burn rate): https://sre.google/workbook/alerting-on-slos/
- OpenTelemetry — Metrics/Traces/Logs specification: https://opentelemetry.io/docs/concepts/signals/
- OpenLineage — Open standard for data lineage & job events: https://openlineage.io/docs/
- Marquez — OpenLineage reference implementation: https://marquezproject.ai/
- Apache Airflow — Metrics & logging: https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/logging-monitoring/metrics.html
- Apache Airflow — OpenTelemetry tracing: https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/logging-monitoring/traces.html
- Apache Spark — Monitoring & instrumentation (Streaming metrics): https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#monitoring-streaming-queries
- Apache Flink — Metrics & backpressure monitoring: https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/metrics/
- Kafka — Monitoring (consumer lag, JMX metrics): https://kafka.apache.org/documentation/#monitoring
- dbt — Source freshness: https://docs.getdbt.com/docs/build/sources#source-data-freshness
- dbt — Build and test (data tests): https://docs.getdbt.com/docs/build/data-tests
- Great Expectations — Checkpoints & expectations: https://docs.greatexpectations.io/docs/
- Soda — SodaCL checks & monitors: https://docs.soda.io/
- Monte Carlo — Data observability pillars: https://www.montecarlodata.com/blog-what-is-data-observability/
- Elementary — dbt-native observability: https://docs.elementary-data.com/
- Prometheus — Alerting & alertmanager: https://prometheus.io/docs/alerting/latest/overview/
- Grafana — Dashboards best practices: https://grafana.com/docs/grafana/latest/dashboards/
