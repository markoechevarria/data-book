# Scalable Pipelines: Scaling, Partitioning, Parallelism, Backpressure, and Cost

> Scalability in data pipelines is not "add more machines". It is a set of *structural* decisions made
> before the first row flows: how you partition the work, how you bound concurrency, how you absorb
> bursts without dropping data, and how you keep cost proportional to value. A senior engineer knows
> that most pipelines that "don't scale" actually have a **partitioning or fan-in problem**, and that
> the cheapest performance optimization is usually not running the job at all.

---

## TL;DR

- **Scale out (horizontal) for throughput; scale up (vertical) for a single irreducible unit of work.**
  Data pipelines are almost always horizontal-scaling problems — except for trickle-in "one giant
  query/join" cases.
- **Partitioning is the primary scalability lever.** Partition by time first (aligned to your SLAs),
  then by a high-cardinality key that spreads load evenly. Skew kills more pipelines than volume.
- **Parallelism must be bounded.** Unbounded fan-out saturates sources (API rate limits, OLTP
  replicas), warehouses (concurrency limits), and the orchestrator (metadata DB).
- **Backpressure is how a pipeline says "slow down" instead of silently losing or accumulating data.**
  Design for it explicitly in streaming and API-based ingestion.
- **Incremental beats batch beats full-refresh** for cost, but each introduces correctness risks
  (late data, deletes, schema drift) that must be handled deliberately.
- **Cost-aware design = partition pruning + incremental + right-sized compute + avoiding idle.**
  Serverless/auto-scaling engines shift the problem from capacity planning to *query efficiency*.
- **Measure first.** A pipeline with no metrics cannot be scaled — you will optimize the wrong thing.

---

## 1. Definitions and core vocabulary

| Term | Definition |
|------|-----------|
| **Vertical scaling** | Bigger machine (more CPU/RAM/IO). Simple, capped, and expensive. |
| **Horizontal scaling** | More machines/workers. Used by Spark/Kafka/Flink/warehouse clusters. |
| **Partitioning** | Splitting data into independent chunks so work can be parallelized and pruned. |
| **Shuffle** | Redistributing data across workers (by key) — the expensive part of distributed compute. |
| **Fan-out / fan-in** | One-to-many (parallel extracts) then many-to-one (single join/aggregate). |
| **Concurrency** | How many tasks/queries run at once. Must be bounded (pools, `max_active_tasks`). |
| **Throughput vs latency** | Rows/second vs. time-to-first-result. Optimizing one often hurts the other. |
| **Backpressure** | A mechanism that slows producers when consumers are saturated, instead of buffering forever. |
| **Consumer lag** | How far behind real-time a consumer is (Kafka: offset distance × messages). |
| **Incremental load** | Only processing new/changed data since the last successful run. |
| **Watermark** | A signal that all events before time T have arrived, enabling final results. |
| **Skew** | Uneven partition sizes causing a few workers to do most of the work. |
| **Cardinality** | Number of distinct values — drives partition count, memory, and shuffle cost. |
| **Selectivity** | Fraction of rows a filter keeps — drives pushdown effectiveness. |
| **Cost per row / cost per pipeline run** | Unit economics that must be tracked like latency. |

---

## 2. What problem it solves / why it matters

A pipeline that works on 10 GB and dies on 10 TB usually fails for one of these reasons:

1. **No partitioning / no pruning** → every run scans everything, cost grows linearly forever.
2. **A single hot key** → one worker gets 90% of the data (skew) while others idle.
3. **Unbounded parallelism** → the source or warehouse melts; the pipeline thrashes.
4. **Full-refresh assumptions** → runtime grows with total history, not with new data.
5. **Buffering without backpressure** → memory grows until OOM, or the queue grows until disk fills.
6. **Fan-in at the end** → 200 parallel tasks finish in seconds and then all wait on one slow join.
7. **Cost blindness** → the pipeline "works" and quietly costs $40k/month.

Scalability matters because it converts a *temporary* solution into a *platform*. At senior level you
are not asked "does it work?" but "does it still work at 10×, 100×, and does the cost curve stay
linear or better?".

---

## 3. How it works internally

### 3.1 Horizontal scaling: what actually happens

```text
                 ┌─────────────┐
   input ───────▶│  PARTITION  │──────▶ [worker 1] ─┐
                 │  (split)    │──────▶ [worker 2] ─┼──▶ SHUFFLE ──▶ [reduce] ──▶ output
                 │             │──────▶ [worker N] ─┘   (by key)
                 └─────────────┘
```

Three phases everywhere (MapReduce's ghost):

1. **Split** — break input into partitions (files, Kafka partitions, table partitions, query splits).
2. **Map / process** — process each partition independently (narrow transformations: filter, project,
   `map`, `flatMap`). No data movement → perfectly scalable.
3. **Reduce / shuffle** — group by key (wide transformations: `groupBy`, `join`, `distinct`, `sort`).
   Data moves across the network → the bottleneck, and where skew appears.

> **Senior note:** "How many shuffles does this pipeline have, and on what keys?" is the single best
> question to ask about pipeline scalability. A 12-shuffle pipeline on a 2 TB table is a design smell.

### 3.2 Partitioning internals

**Time partitioning (the default for analytics):**

```
s3://lake/events/dt=2026-01-01/hour=00/part-00000.parquet
s3://lake/events/dt=2026-01-01/hour=01/part-00000.parquet
```

**Hash partitioning (for joins and even distribution):**

```python
df = df.repartition(200, "customer_id")   # 200 buckets by hash(customer_id)
```

**Range partitioning** (for ordered scans / range queries) and **hidden partitioning** (Iceberg:
partition by `days(event_ts)` without the user writing the transform in every query).

Internals that matter:

- **File count is a first-class concern.** 200,000 tiny files → metadata overhead dominates and
  planning takes minutes. Compact to 128 MB – 1 GB files.
- **Columnar + statistics.** Parquet/ORC store min/max per row group; engines use them for partition
  pruning and predicate pushdown. Good clustering makes scans 10–100× cheaper.
- **Iceberg/Delta/Hudi** add a metadata layer over files: snapshots, manifests, and **partition specs
  that can evolve**. Hidden partitioning means queries don't need to know the partition layout.

### 3.3 Parallelism internals

| Layer | Knob | What it controls |
|-------|------|------------------|
| Orchestrator | `max_active_tasks`, pools, `max_active_runs` | How many tasks run concurrently |
| Spark | `spark.sql.shuffle.partitions`, `spark.default.parallelism`, executor count/cores | How many tasks per shuffle and cluster shape |
| Warehouse | Warehouse size, multi-cluster, concurrency scaling, slots | How many queries run without queuing |
| Kafka | Partition count, consumer count (≤ partitions), `max.poll.records` | Consumer parallelism |
| Flink | Parallelism, slots, operator chaining | Operator-level parallelism |
| API ingestion | Rate limits, concurrency caps, page size | How fast you can ethically pull |

> **Senior note:** Kafka consumer parallelism is *capped by partition count*. You cannot add a 7th
> consumer to a 6-partition topic and get more throughput — a classic interview trap.

### 3.4 Backpressure internals

Backpressure exists when a downstream stage is slower than an upstream one. Three strategies:

1. **Blocking / pull-based** — the consumer asks for the next batch only when ready. Kafka's
   `poll()` model and Flink's credit-based flow control work this way: the slowest operator throttles
   the source. This is the *healthy* default.
2. **Bounded buffers + load shedding** — when buffers fill, drop or sample, or apply a priority
   policy. Legitimate when the data is lossy-tolerant (metrics, telemetry).
3. **Unbounded buffering** — the anti-pattern. Memory/disk grows until OOM or disk-full. Symptom:
   latency looks great right up until the process dies.

Discriminators to monitor: **consumer lag** (Kafka), **queue depth** (SQS/RabbitMQ), **input queue
length / watermark lag** (Flink/Spark SS), **producer buffer** (`buffer.memory`, `linger.ms`).

---

## 4. How to implement it

### 4.1 Incremental load with a high-watermark (and why it is subtly wrong)

```python
# Naive high-watermark incremental extract — fast, but loses late-arriving rows.
last = spark.sql("select max(updated_at) as m from raw.orders").first()["m"]
new = spark.read.jdbc(
    url=jdbc, table="public.orders",
    predicates=[f"updated_at > '{last}' and updated_at <= '{now}'"],
)
new.write.mode("append").partitionBy("dt").parquet("s3://raw/orders")
```

Problems: (a) rows updated *after* the read but with an earlier timestamp are missed; (b) `now()`
makes it non-deterministic; (c) clock skew across sources.

**Better: overlap window + MERGE (idempotent).**

```python
LOOKBACK = "3 days"   # re-read a trailing window to catch late arrivals
lo = add_days(last, -LOOKBACK)

new = spark.read.jdbc(url=jdbc, table="public.orders",
                      predicates=[f"updated_at >= '{lo}' and updated_at < '{now}'"])
# land to a staging table (append-only), then MERGE into the target
new.createOrReplaceTempView("orders_delta")
spark.sql("""
    MERGE INTO raw.orders AS t
    USING (SELECT * FROM (
             SELECT *, row_number() OVER (PARTITION BY order_id ORDER BY updated_at DESC) rn
             FROM orders_delta) WHERE rn = 1
           ) AS s
    ON t.order_id = s.order_id
    WHEN MATCHED AND s.updated_at >= t.updated_at THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")
```

### 4.2 Partitioning and avoiding skew in Spark

```python
# Detect skew before you fix it
df.groupBy("partition_col").count().orderBy(col("count").desc()).show(20)

# Salting to break up a hot key (e.g., one customer_id = 60% of rows)
import pyspark.sql.functions as F
SALT = 32
salted = df.withColumn("salt", (F.rand() * SALT).cast("int"))

# Join two salted sides (both must be salted consistently) or explode the small side
agg = salted.groupBy("customer_id", "salt").agg(F.sum("amount")) \
            .groupBy("customer_id").agg(F.sum("sum(amount)").alias("total"))

# Or: broadcast the small dimension instead of shuffling the big fact
big.join(F.broadcast(dim), "customer_id")
```

```python
# Right-size shuffle partitions: target ~100-200 MB per task
spark.conf.set("spark.sql.shuffle.partitions", "auto")   # with AQE enabled
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# Control file count on write
(df.repartition(200, "dt")            # or coalesce() to *reduce* without a shuffle
   .write.mode("overwrite")
   .partitionBy("dt")
   .option("maxRecordsPerFile", 5_000_000)
   .parquet("s3://lake/fact_orders"))
```

### 4.3 Bounding parallelism in Airflow (pools + concurrency)

```python
with DAG(
    "ingest_100_sources",
    max_active_tasks=8,        # at most 8 tasks of this DAG at once
    max_active_runs=2,
    schedule="0 * * * *",
) as dag:
    for src in sources:
        ingest.override(pool="source_api_pool")(src)   # global cap across all DAGs
```

```ini
# airflow.cfg / environment
AIRFLOW__CORE__DEFAULT_POOL_TASK_SLOT_COUNT=4
```

Create a pool `source_api_pool` with N slots in the UI/CLI to protect a rate-limited API globally —
not just per DAG. This is how you make 500 DAGs share a 20-requests/second budget.

### 4.4 Backpressure in a Kafka consumer

```python
# Manual control of how much is pulled -> natural backpressure
consumer = KafkaConsumer(
    "orders",
    enable_auto_commit=False,
    max_poll_records=500,              # cap batch size
    fetch_max_bytes=50 * 1024 * 1024,  # cap fetch size
    max_poll_interval_ms=300_000,      # time budget between polls
)

while running:
    records = consumer.poll(timeout_ms=1000)
    batch = process(records)           # bounded work per poll
    producer.send(...); producer.flush()   # apply backpressure to the producer
    consumer.commit()                  # commit only after the sink is durable
```

For Spark Structured Streaming, bound the *rate* explicitly:

```python
(spark.readStream.format("kafka").option("subscribe", "orders").load()
    .writeStream
    .option("maxOffsetsPerTrigger", 100_000)     # backpressure: cap records per micro-batch
    .option("checkpointLocation", "s3://chk/orders")
    .trigger(processingTime="1 minute")
    .foreachBatch(upsert_to_delta)
    .start())
```

### 4.5 Cost-aware pipeline design

```sql
-- BAD: scans all history every run
select count(*) from events where event_ts >= current_date - 1;

-- GOOD: partition pruning (Iceberg hidden partitioning / Delta partition column)
select count(*) from events where dt = current_date - 1;

-- GOOD: cluster / liquid clustering on the column you filter by
ALTER TABLE events CLUSTER BY (customer_id, dt);     -- Snowflake / BigQuery-style
```

```python
# Databricks: liquid clustering + predictive optimization instead of manual partitions
spark.sql("ALTER TABLE fact_orders CLUSTER BY (order_date, region)")
```

Cost checklist:
1. **Prune** — every scan must touch only the needed partitions.
2. **Incremental** — process deltas, not history.
3. **Right-size** — auto-scaling serverless clusters > fixed over-provisioned clusters.
4. **Compact** — fewer, larger files; avoid the small-file tax.
5. **Cache / materialize** — reuse expensive intermediate results.
6. **Idle avoidance** — always-on streaming clusters cost 24/7; batch spot instances cost cents.

---

## 5. Where to implement it (tools and when each fits)

| Capability | Tools | When to use |
|-----------|-------|-------------|
| Batch distributed compute | Spark, Databricks, EMR, Dataproc, Flink (batch), Snowpark | Large, messy, or Python-heavy transforms |
| Warehouse scaling | Snowflake warehouses/multi-cluster, BigQuery slots/reservations, Redshift RA3/serverless | SQL-first ELT at scale |
| Streaming | Kafka + Flink/Spark SS, Kinesis + Managed Flink, Pub/Sub + Dataflow | Sub-minute latency needs |
| CDC | Debezium, DMS, Datastream, Fivetran | Incremental replication without full scans |
| Orchestration/concurrency | Airflow pools, Dagster concurrency limits, Prefect concurrency | Global rate limiting, bounded fan-out |
| Table formats | Iceberg, Delta Lake, Hudi | Hidden partitioning, compaction, time travel |
| Cost control | Cloud budgets, warehouse `resource monitors`, Spark adaptive query execution | Guardrails against runaway spend |

**When each fits — quick rules:**

- Warehouse ELT when the transform is SQL and data is < a few TB per run.
- Spark when the transform is complex, the data is huge, or you need ML/feature logic.
- Streaming when the *business value* of low latency exceeds the 24/7 cost.
- Iceberg/Delta when you need ACID, schema/partition evolution, and time travel on object storage.

---

## 6. Patterns and anti-patterns

### 6.1 Patterns

| Pattern | Effect |
|---------|--------|
| **Time-partition + compaction** | Pruning + manageable file counts |
| **Salting hot keys** | Breaks skew in joins/aggregations |
| **Broadcast small dimensions** | Eliminates a shuffle entirely |
| **Bounded fan-out with pools** | Protects sources and warehouses |
| **Rate-limited streaming (`maxOffsetsPerTrigger`)** | Converts "crash" into "stay behind" |
| **Two-phase fan-in** | Aggregate in stages so no single task is the bottleneck |
| **Adaptive query execution / AQE** | Auto-corrects partition sizes and skew at runtime |
| **Delta/Medallion layering** | Each layer is smaller and cheaper than the last |
| **Workload isolation (separate warehouses/queues)** | Big jobs don't starve dashboards |
| **Cost tagging per pipeline** | Makes cost a measurable, attributable SLO |

### 6.2 Anti-patterns

| Anti-pattern | Symptom | Fix |
|--------------|---------|-----|
| **No partition pruning** | Cost grows with history | Partition/cluster on the filter column |
| **Full refresh "for safety"** | Hours of runtime, huge cost | Incremental + periodic full-refresh |
| **One giant text file** | Single-threaded read | Split source / parallel reads |
| **200,000 tiny files** | Planning takes longer than scanning | Compact, `maxRecordsPerFile` |
| **Unbounded `for` loop of tasks** | Orchestrator and source melt | Pools + `max_active_tasks` |
| **Hot-key shuffle** | One task runs for hours | Salt, broadcast, or pre-aggregate |
| **Ignoring consumer lag** | Data arrives hours late | Backpressure + autoscaling consumers |
| **Over-partitioning** | Directory explosion, metadata overhead | Partition coarse first (day, not second) |
| **Cross-join / cartesian** | Exponential blowup | Always join on keys; audit the plan |
| **Same cluster for 2-minute and 2-hour jobs** | Worst of both (cost and latency) | Workload isolation |
| **No cost visibility** | Surprise invoices | Tag, monitor, alert on spend |

---

## 7. Real-world use cases

**E-commerce — hourly order aggregates.** Source Postgres → CDC → raw → hourly dbt models. Scaling
lever: partition by `order_date`, incremental MERGE on `order_id`, and separate warehouses for
reporting vs. the hourly job so a heavy ad-hoc query cannot delay the SLA.

**Ad-tech — 5 billion daily events.** Kafka (200 partitions) → Spark Structured Streaming (1-minute
micro-batches, `maxOffsetsPerTrigger`) → Iceberg. Scaling lever: partition count = consumer
parallelism cap; compaction job merges small files hourly; backpressure manifests as watermark lag.

**Ride-hailing — driver telemetry.** Streaming sensor events with out-of-order arrival. Scaling lever:
watermarks bound state size; skewed partitions by city are handled by increasing partition count for
"hot" cities and windowing by key.

**Retail supply chain — nightly MRP run.** A single monolithic Spark job with a 4-hour runtime, at
risk of missing the morning SLA. Scaling levers: split the join graph into two stages (pre-aggregate
then join), replace a sort-merge join with a broadcast join on the (small) calendar/dimension tables,
and repartition by `sku` with salting for the hot SKUs.

**Financial services — end-of-day reconciliation.** Large, correctness-critical, partition-scoped.
Scaling lever: process each settlement date independently and in parallel; fan-in is trivial because
each date's result is independent.

**IoT — 1M devices × 1 message/sec.** Scaling lever: `maxOffsetsPerTrigger` caps per-batch work;
autoscale consumers with lag-based scaling; sampling for non-critical telemetry (load shedding as a
*deliberate* choice).

---

## 8. Senior-level deep dive

### 8.1 The scaling decision tree

```text
Is the pipeline too slow or too expensive?
├─ Too expensive, fast enough
│   ├─ Are scans pruned?           → no: partition/cluster (biggest win, cheapest fix)
│   ├─ Is it full-refresh?         → yes: make it incremental
│   └─ Is compute oversized/idle?  → right-size, use spot/serverless, turn jobs off
└─ Too slow
    ├─ Is one task the bottleneck (skew/fan-in)?  → salt / broadcast / two-phase fan-in
    ├─ Is there no parallelism?                   → partition the input, raise concurrency safely
    ├─ Is concurrency capped by the sink?         → increase sink capacity or stage writes
    └─ Is it serialized by the orchestrator?      → raise pools/limits, or delegate work to compute
```

### 8.2 Trade-off tables

**Incremental vs full-refresh vs append**

| Strategy | Cost | Correctness risk | When |
|----------|------|------------------|------|
| Full refresh | O(total history) | Low | Small tables; logic changed; periodic self-heal |
| Merge/upsert (incremental) | O(changes + lookback) | Medium (late data, deletes) | Most dimension/fact tables |
| Append-only | O(changes) | High (duplicates unless deduped downstream) | Event logs, immutable facts |
| Delete+insert by partition | O(affected partitions) | Low | Partition-scoped recompute |

**Scaling dimension × engine**

| Engine | Scale-out unit | Bottleneck | Backpressure mechanism |
|--------|---------------|-----------|-----------------------|
| Spark batch | Executor | Shuffle, skew | Memory spill; AQE |
| Spark SS | Executor + micro-batch | State store, sink | `maxOffsetsPerTrigger`, watermark |
| Flink | Task slot | State backend, network | Credit-based flow control |
| Kafka | Partition | Partitions, rebalancing | Pull-based `poll()` |
| Warehouse | Cluster/slot | Concurrency limits, spills | Query queueing, resource monitors |
| Airflow | Worker/scheduler | Metadata DB | Pools, `max_active_tasks` |

### 8.3 Failure modes under load

| Failure | Trigger | Symptom | Mitigation |
|---------|--------|---------|-----------|
| **OOM on driver** | `collect()` of huge result; broadcast too big | Driver crash, whole job restarts | Avoid collect; cap broadcast threshold |
| **OOM on executor** | Skew, huge partitions, insufficient memory | Task retries, speculative execution churn | Salt, repartition, raise memory, AQE skew join |
| **Shuffle spill** | Partition too large | Job slows 10×+ | Increase partitions; reduce data before shuffle |
| **Sink saturation** | Too many concurrent writers | Deadlocks, timeouts, 429s | Staging tables, batch writes, backoff |
| **Source rate-limit** | Unbounded fan-out | 429s, partial extracts | Pools, token buckets, exponential backoff |
| **Rebalance storm (Kafka)** | Long processing between polls | Repeated rebalances, no progress | `max_poll_interval_ms`, smaller batches |
| **Metadata explosion** | Over-partitioning | Planning slow, warehouse throttling | Coarser partitions, compaction |
| **Cost runaway** | Cross join, missing predicate, retry loop | Budget blown overnight | Resource monitors, spend alerts, query audits |
| **Catch-up collapse** | Lag grows further while trying to catch up | Never recovers | Raise parallelism, or *skip* to latest (explicitly) |

### 8.4 Debugging scalability problems — a procedure

1. **Quantify.** Establish baseline: rows in/out, runtime, cost, consumer lag, p95 latency. Without
   numbers you are guessing.
2. **Find the stage.** Spark UI / query profile / Flink backpressure monitor: which stage or operator
   is the longest?
3. **Check cardinality and distribution.** Is the partition histogram flat? Is one key dominant?
4. **Check the plan.** Shuffles, broadcasts, spills, scans — is the plan what you expect?
5. **Change one thing.** Partition count, salt factor, join strategy, or engine size — then re-measure.
6. **Look at the sink.** Often the "slow" stage is actually blocked on a lock, a single-writer
   constraint, or a rate limit downstream.
7. **Document the ceiling.** "This pipeline is linear to 5 TB/day and needs a redesign beyond that" is
   a senior deliverable.

### 8.5 Interview-grade trade-off questions to be able to answer

- Why is a broadcast join sometimes *slower* than a sort-merge join?
- You doubled the cluster size and runtime went from 40 to 35 minutes. Why? *(Hint: shuffle is fixed
  cost; you scaled the wrong dimension; skew.)*
- Why is consumer lag a better autoscaling signal than CPU?
- When does adding partitions to a Kafka topic *not* help?
- What is the difference between a shuffle partition and a file/table partition? *(Very commonly
  conflated.)*
- Why does over-partitioning cost more than under-partitioning in a lakehouse?
- How do you make a **streaming** job's cost predictable?

---

## 9. Common mistakes and how to avoid them

1. **Confusing "shuffle partitions" with "table partitions".** One is a runtime slicing of work, the
   other is a physical data layout. Tune them separately.
2. **Partitioning by a high-cardinality ID** (e.g., `user_id`) → millions of tiny directories. Use
   *clustering* or bucketing, not partitioning, for high-cardinality keys.
3. **Increasing cluster size to fix skew.** Adding workers to a skewed job just adds idle workers.
   Salt the key or broadcast the dimension.
4. **Ignoring file sizes.** Compact regularly; aim for 128 MB – 1 GB files.
5. **Leaving `spark.sql.shuffle.partitions=200`** for a 5 TB job. Let AQE coalesce, or size it to
   ~100–200 MB per partition.
6. **Unbounded task fan-out.** Always set `max_active_tasks` and pools; protect shared sources.
7. **No backpressure in custom consumers.** Cap batch sizes; never let a Python list grow with lag.
8. **Treating cost as someone else's problem.** Tag pipelines, monitor spend, and treat cost as an
   SLO. A pipeline that is "correct but unaffordable" will be turned off.
9. **Optimizing before measuring.** Use the query profile/Spark UI, not intuition.
10. **Ignoring the sink.** Your bottleneck is often the warehouse's write concurrency or the single
    writer on a target system, not your compute.
11. **Backfilling at full speed during business hours.** Backfills compete with production. Throttle
    them and schedule them off-peak.
12. **Assuming streaming is a strict upgrade.** It costs 24/7 compute and is harder to operate. Use it
    only when latency has business value.

---

## 10. Exercises / interview questions

1. A nightly job processes 2 TB and takes 6 hours, growing ~20% per month. Give three changes that
   each reduce cost by ≥30% and one that only *moves* the cost. *(Hint: partition pruning, incremental
   + lookback, file compaction; vs. buying a bigger cluster.)*
2. Your Spark job has 200 shuffle partitions and each output task takes 30 minutes while others take
   10 seconds. Diagnose and fix. *(Hint: skew, salting, AQE skewJoin.)*
3. Design partitioning for an events table queried by (a) day, (b) customer, (c) last 5 minutes. What
   do you partition by, and where do you live with a scan? *(Hint: day partitions + clustering; recent
   data in a different table/stream.)*
4. Explain how Kafka consumer parallelism is bounded and what you do if you need 2× throughput but
   cannot add partitions. *(Hint: batch processing, parallel processing *within* the consumer.)*
5. You must ingest 300 SaaS APIs with different rate limits. Sketch the concurrency architecture.
   *(Hint: pools/token buckets per source, bounded fan-out, retries with jitter.)*
6. Where should backpressure live: producer, broker, consumer, or sink? Justify with a failure
   scenario. *(Hint: it must exist at *every* stage boundary, but the decision logic is closest to the
   bottleneck.)*
7. Your warehouse bills $50k/month, 70% from one pipeline. Walk through your investigation.
   *(Hint: query history, scans without pruning, full refreshes, concurrency, idle warehouses.)*
8. When is *load shedding* the correct answer? Give a non-abusive example and an abusive one.
9. How does Iceberg *hidden partitioning* change your pipeline design vs. Hive-style `dt=` directories?
   *(Hint: no user-side partition transforms; partition spec evolution; pruning handled by metadata.)*
10. Define an SLO for a pipeline's cost and freshness simultaneously. What is the monitoring surface?
    *(Hint: cost per run/row, freshness lag, volume anomaly detection — see
    [04-monitoring-observability.md](./04-monitoring-observability.md).)*
11. You must guarantee that a consumer never loses data even if the sink is down for 6 hours. Sketch
    the architecture. *(Hint: bounded buffers + lag, not unbounded memory; lag-based alerting;
    backpressure to the producer or explicit degradation.)*

---

## References

- Apache Spark — Tuning & Adaptive Query Execution: https://spark.apache.org/docs/latest/sql-performance-tuning.html
- Apache Spark — Performance Tuning (partitioning, memory, serialization): https://spark.apache.org/docs/latest/tuning.html
- Apache Spark — Structured Streaming (backpressure / `maxOffsetsPerTrigger`): https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
- Apache Kafka — Design (partitions, consumer groups): https://kafka.apache.org/documentation/#design
- Apache Flink — Backpressure monitoring & flow control: https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/monitoring/back_pressure/
- Conduktor — Backpressure handling in streaming systems: https://www.conduktor.io/glossary/backpressure-handling-in-streaming-systems
- Snowflake — Warehouse sizing / multi-cluster / resource monitors: https://docs.snowflake.com/en/user-guide/warehouses-overview
- BigQuery — Slots, reservations, and query optimization: https://cloud.google.com/bigquery/docs/slots
- Databricks — Liquid clustering & predictive optimization: https://docs.databricks.com/aws/en/tables/clustering
- Apache Iceberg — Partitioning & hidden partitioning: https://iceberg.apache.org/docs/latest/partitioning/
- Delta Lake — Optimize / compaction (`OPTIMIZE`, Z-order): https://docs.delta.io/latest/optimizations-oss.html
- dbt — Incremental strategies (`merge`, `delete+insert`, `insert_overwrite`): https://docs.getdbt.com/docs/build/incremental-strategy
- AWS — Retry with backoff pattern: https://docs.aws.amazon.com/prescriptive-guidance/latest/cloud-design-patterns/retry-backoff.html
- AWS — Designing resilient SQS + Lambda pipelines (partial failures, DLQ, idempotency): https://builder.aws.com/content/3KV2wkCAoH1PjjP6g4ywzIWXD73/designing-resilient-sqs-and-lambda-pipelines-retries-partial-failures-and-idempotency
