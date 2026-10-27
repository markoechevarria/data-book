# Delivery Semantics: Exactly-Once vs At-Least-Once vs At-Most-Once

> "Exactly-once" is the most abused phrase in data engineering. It is not a property you buy from a broker; it is a property of a **closed loop** consisting of input, computation, state, and output, each of which must be atomic or idempotent with respect to the others. Kafka gives you exactly-once *within Kafka* (idempotent producer + transactions + `read_committed`). Spark and Flink give you exactly-once *state* via checkpointing, and exactly-once *end-to-end* only if the sink participates in the same commit protocol. A senior engineer's job is to know where the boundary is in their own pipeline, to state it honestly in the design doc, and to make everything outside the boundary idempotent.

## TL;DR

- **At-most-once**: never duplicates, may lose. `acks=0`/no retries, or commit-before-process.
- **At-least-once**: never loses (given correct commits), may duplicate. The default in practice; make consumers idempotent.
- **Exactly-once (effectively-once)**: the *effects* are applied once. Achieved by **atomic commit** (transactions) or **idempotence** (dedup keys, upserts, transactional file commits) — usually both.
- Kafka's exactly-once stack: **idempotent producer** (`enable.idempotence=true`, PID + sequence numbers + epoch) → **transactions** (`transactional.id`, transaction coordinator, commit markers in the log) → **`isolation.level=read_committed`** consumers that skip aborted data and stop at the LSO.
- `sendOffsetsToTransaction` is what makes consume→transform→produce *exactly-once* rather than just "at least once with no duplicates in the output topic": offsets and output records commit atomically.
- **Flink**: checkpoint barriers + aligned/asynchronous snapshots of state = exactly-once state; sinks are exactly-once only with `TwoPhaseCommit` (Kafka producer, filesystem with rollback, Iceberg/Delta) or when they are idempotent.
- **Spark Structured Streaming**: micro-batch offsets are written into the **checkpoint's offset WAL** *before* the batch executes; exactly-once requires an idempotent/transactional sink in `foreachBatch` (`txnVersion`) or the built-in transactional file sink.
- **Kafka Streams**: `processing.guarantee=exactly_once_v2` gives atomic state + changelog + sink within Kafka.
- Anything crossing to a non-participating system (email, webhook, S3 write by hand, JDBC insert) is **at-least-once + idempotency**. Say so.
- Cost of EOS: latency (commit round trips, LSO stalls), throughput (transaction overhead, smaller batches), and operational complexity (fencing, timeouts, `transactional.id` design). Use it where duplicates are business-fatal, not by default.

## 1. Definitions and core vocabulary

| Term | Definition | Where it lives |
|---|---|---|
| **At-most-once** | Each record processed 0 or 1 times; loss possible | Producer/consumer config choice |
| **At-least-once** | Each record processed ≥1 times; duplicates possible | The practical default |
| **Exactly-once (EOS)** | Effects applied exactly once | Needs atomicity and/or idempotence at every boundary |
| **Effectively-once** | Honest term for "at-least-once + idempotent effects" | Most production systems |
| **Idempotent producer** | Producer with PID + per-partition sequence numbers; broker dedups retries | `enable.idempotence=true` (default since 3.0) |
| **Producer ID (PID)** | Unique id assigned by the broker for an idempotent producer session | Stored per partition as producer state |
| **Producer epoch** | Bumped when a new instance takes over a `transactional.id` | Fences old instances |
| **Transaction coordinator** | Broker that manages transaction state | `transaction.state.log.*`; the log is `__transaction_state` |
| **Transaction marker** | `COMMIT`/`ABORT` control batch written into data partitions | The mechanism `read_committed` consumers use |
| **LSO (last stable offset)** | Highest offset before any open transaction | Consumers stop here under `read_committed` |
| **Zombie fencing** | Rejecting writes/commits from a superseded producer instance | `ProducerFencedException`, `ILLEGAL_GENERATION` |
| **Checkpoint** | Consistent snapshot of operator state + input offsets | Flink checkpoints; Spark `checkpointLocation` |
| **Barrier** | Marker injected into the stream to align checkpoints | Flink's Chandy–Lamport-style snapshotting |
| **Two-phase commit sink** | Sink that pre-commits then commits on checkpoint completion | Flink `TwoPhaseCommitSinkFunction` |
| **Offset WAL** | Write-ahead log of the batch's offsets written *before* processing | `checkpointLocation/offsets/` in Spark |
| **Idempotent sink** | Sink where reapplying a write is a no-op | UPSERT/MERGE on a key, `txnVersion` files |
| **Dedup key** | Business/event id used to detect reprocessing | `event_id`, `(aggregate_id, version)`, `idempotency_key` |
| **DLQ / retry topic** | Where poison messages go instead of blocking progress | Part of "at-least-once with a plan" |

### 1.1 Delivery semantics are a per-hop property

```text
producer ──▶ Kafka topic A ──▶ processor ──▶ Kafka topic B ──▶ sink (DB / S3 / API)

   hop 1        hop 2            hop 3           hop 4             hop 5

Semantics must be declared per hop. A pipeline is as strong as its weakest hop,
and the strength of a hop depends on where the offsets/state are committed.

"Exactly-once end-to-end" = every hop is atomic or idempotent.
"At-least-once + idempotent sink" = the pragmatic target for nearly all pipelines.
```

## 2. What problem it solves / why it matters

Duplicates and losses are not symmetric problems, and they are not the same problem in every domain:

| Domain | Duplicate cost | Loss cost | Tolerable semantic |
|---|---|---|---|
| Bank ledger postings | Double-charge, reconciliation nightmare, regulatory | Missing movement, worse | Exactly-once (idempotent by business key) |
| Billing / metering (usage aggregation) | Overbilling customers | Underbilling | Effectively-once (idempotent counters) |
| Inventory decrement | Oversell / undersell | Phantom stock | Effectively-once |
| Metrics / observability | Slightly inflated counters (usually acceptable) | Missing datapoints | At-least-once fine |
| Clickstream analytics | Inflated unique counts | Under-counting | At-least-once + approximate dedup |
| Notifications (email/push) | User gets 2 emails (annoying) | User gets none | At-least-once + dedup key |
| ML feature pipelines | Training skew | Skewed training | Effectively-once |
| Audit trails | Duplicate audit entries (bad) | Missing entries (catastrophic) | Exactly-once on the audit key |

The decisive question is always: **"what is the cost of a duplicate, and what is the cost of a loss, per hop?"** Answer that in writing and the architecture picks itself.

## 3. How it works internally

### 3.1 Level 1 — the idempotent producer (KIP-98, since 0.11)

```text
Producer gets PID=7 from the broker (per connection/session).
For each partition, it maintains a monotonically increasing sequence number:

  records sent to P0:  seq=0,1,2,3,4 ...
  network timeout after sending seq=3 → retry sends seq=3 again

Broker keeps the last 5 (max.in.flight) sequence numbers per (PID, partition):
  - duplicate seq (already applied) → DROP, return the original offset
  - unexpected seq (gap)           → OutOfOrderSequenceException

Result: retries cannot create duplicates => no duplication is possible
within the session. Also enforces ordering across in-flight requests
(that is why max.in.flight.requests.per.connection can be 5 under idempotence,
where previously it had to be 1 for ordering).

Limits:  (a) state is per-session; a producer restart is a NEW PID
         (b) 5 in-flight requests is the dedup window per partition
         (c) `producer.id.expiration.ms` (default 86400000) can expire an idle
             producer's state → subsequent sends start a new epoch
```

### 3.2 Level 2 — transactions

```mermaid
sequenceDiagram
  participant P as Producer (transactional.id=orders-eos-1)
  participant TC as Transaction Coordinator
  participant B as Partition Leader(s)
  participant C as Consumer (read_committed)
  P->>TC: InitProducerId (week-long: epoch bump fences zombies)
  P->>P: beginTransaction()
  P->>B: AddPartitionsToTxn(P0, P1)
  P->>B: Produce(records marked in-transaction)
  Note over B,C: C may fetch these bytes but CANNOT return them<br/>until a commit marker exists (stops at LSO)
  P->>B: AddOffsetsToTxn + TxnOffsetCommit (input offsets!)
  P->>TC: EndTxn(COMMIT)
  TC->>B: WriteTxnMarker(COMMIT) into each partition's log
  Note over B,C: now readable: batch (commit marker) then data
  B-->>C: fetch returns records + commit marker
  Note over C: C advances; offsets were committed atomically with outputs
```

Key internals:

- The transaction coordinator maintains a state machine per `transactional.id`: `Empty → Ongoing → PrepareCommit/PrepareAbort → CompleteCommit/CompleteAbort` in `__transaction_state`.
- Data partitions hold a **producer state** per PID/epoch (sequence numbers + open transactions) and control batches (`COMMIT`/`ABORT`).
- `read_committed` uses (a) the LSO to stop fetching and (b) `AbortedTransactions` metadata to filter aborted batches while scanning.
- **Fencing**: `InitProducerId` with the same `transactional.id` bumps the epoch; older instances get `ProducerFencedException` on their next operation. This is why a restarted Kafka Streams task or a restarted EOS app cannot double-write.
- Timeouts: `transaction.timeout.ms` (producer, default 60 s) must be ≤ `transaction.max.timeout.ms` (broker, default 900 s). Exceeding it aborts the transaction, unblocking `read_committed` consumers.
- Limitation: one producer can only have **one open transaction at a time**. High parallelism therefore requires multiple producers — this is exactly what KIP-447 enabled (one producer per task/partition stream, offsets included per consumer group).

### 3.3 Where EOS stops: boundary analysis

```text
                       ┌──────────────────────── Kafka boundary ───────────────────────┐
source topic ──▶ consumer ──▶ transform ──▶ producer ──▶ sink topic ──▶ consumer ──▶ external sink
                       └───────────────────────────────────────────────────────────────┘
                          EOS: possible with transactions   (killable)
                                                                             outside: needs
                                                                             idempotence or a
                                                                             transactional sink
```

| Boundary | Achievable semantic | Mechanism |
|---|---|---|
| Kafka → Kafka | Exactly-once | Transactions + `sendOffsetsToTransaction` + `read_committed` |
| Kafka → DB (idempotent upsert) | Effectively-once | Offsets stored *in the DB* within the same transaction as the row write |
| Kafka → DB (append-only insert) | At-least-once + growing duplicates | Add a unique constraint on the event id, use `ON CONFLICT DO NOTHING` |
| Kafka → S3/object store (Spark/Flink) | Exactly-once | Transactional file commit (Iceberg/Delta/Hudi, `fs.s3a.committer`, or the built-in `FileStreamSink` "exactly once" behavior) |
| Kafka → external API | At-least-once | Idempotency key at the receiver; outbox in the receiver's DB |
| Kafka → email/SMS | At-least-once with dedup | Sender-side idempotency (dedup on a message key) |
| Kafka → cache (Redis) | Effectively-once | `SET` with the value derived from a versioned record |

### 3.4 Flink: checkpointing + two-phase-commit sinks

```text
Flink exactly-once = consistent snapshots of operator state + input offsets, and
                     sinks that only publish on "checkpoint completed".

Timeline:
  barrier(n) ─▶ source snapshot offsets ─▶ operator state snapshot (async) ─▶ sink
                                                                             │ pre-commit
  ... stream continues while a snapshot is in progress (Chandy–Lamport) ...  │
  checkpoint n completed ──────────────────────────────────────────────────▶ │ commit

Two families of sinks:
  1. TwoPhaseCommitSinkFunction (Kafka producer w/ transactions, filesystem,
     Iceberg/Delta): pre-commit writes to a staging area / holds the txn open;
     on checkpoint completion the txn commits. On failure, abort.
  2. Idempotent sinks: re-applying a batch is a no-op (UPSERT on a unique key).
```

Flink's guarantees per sink (documented in the connector "guarantees" page): Kafka sink = exactly-once with `DeliveryGuarantee.EXACTLY_ONCE`; JDBC = exactly-once only with upserts and a key; Elasticsearch = at-least-once with idempotent upserts; file sink = exactly-once via the transactional committer.

### 3.5 Spark Structured Streaming: the offset WAL and checkpoint

```text
Micro-batch execution order (this ordering IS the delivery guarantee):

 1. Read the current offsets from Kafka (deterministic range per partition).
 2. WRITE the offsets to checkpointLocation/offsets/<batchId>  (write-ahead log)
 3. Execute the batch: read data (from those offsets), transform, write to sink
 4. On success, advance commits/ (batch committed marker)
 5. Crash mid-batch → on restart, Spark sees the WAL entry but no commit,
    re-executes the batch from the SAME offsets → re-delivery to the sink.

=> The source side is at-least-once across restarts (no loss, possible re-run).
=> Exactly-once requires the SINK to be idempotent or transactional:
     - file sink (parquet) : transactional rename => exactly-once by design
     - Kafka sink          : atomic per-batch via the producer + checkpoint
     - Delta/Iceberg       : txnVersion/txnAppId or MERGE with a unique key
     - JDBC                : idempotent UPSERT (always at-least-once otherwise)
     - foreach() arbitrary : at-least-once, your problem
```

### 3.6 Kafka Streams

```properties
application.id=order-enricher
processing.guarantee=exactly_once_v2      # or exactly_once (older)
# effects: one producer per stream thread; offsets + state store + output topics
#          commit in the same transaction; state stores are restored transactionally
commit.interval.ms=100                    # EOS needs frequent, small commits
transaction.timeout.ms=60000
```

Streams implements EOS by combining `sendOffsetsToTransaction` semantics internally: the consumer's position, the changelog writes, and the sink writes are all in one producer transaction per task per commit interval. Task restart = epoch bump = fencing.

## 4. How to implement it

### 4.1 Python: transactional produce (Kafka → Kafka EOS)

```python
from confluent_kafka import Consumer, Producer, KafkaException, TopicPartition
import json

producer = Producer({
    "bootstrap.servers": "kafka1:9092",
    "transactional.id": "orders-enricher-1",   # unique per running instance!
    "enable.idempotence": True,
    "acks": "all",
    "transaction.timeout.ms": 60000,
    "compression.type": "zstd",
})

consumer = Consumer({
    "bootstrap.servers": "kafka1:9092",
    "group.id": "orders-enricher",
    "enable.auto.commit": False,
    "isolation.level": "read_committed",
    "auto.offset.reset": "earliest",
})

producer.init_transactions()          # raises if the id is already in use → fenced
consumer.subscribe(["orders.v1"])

while True:
    msgs = consumer.consume(num_messages=1000, timeout=1.0)
    if not msgs:
        continue
    try:
        producer.begin_transaction()
        offsets = {}
        for m in msgs:
            if m.error():
                continue
            out = json.dumps(enrich(json.loads(m.value()))).encode()
            producer.produce("orders.enriched.v1", key=m.key(), value=out)
            offsets[TopicPartition(m.topic(), m.partition())] = m.offset() + 1

        # Atomically: output records + the consumer group's position.
        producer.send_offsets_to_transaction(
            [TopicPartition(t, p, o) for (t, p), o in offsets.items()],
            consumer.consumer_group_metadata(),
        )
        producer.commit_transaction()
    except KafkaException as e:
        # Abort => the batch will be re-read from the committed offset.
        producer.abort_transaction()
        # Non-retriable (fenced/out-of-order/authorization) => exit the process.
        if not e.args[0].retriable():
            raise
```

### 4.2 Java: two producers (two sinks) in one transaction — where EOS must stop

```java
// Anti-example to understand the boundary: the DB write is NOT part of the txn.
producer.beginTransaction();
for (ConsumerRecord<String,String> r : records) {
    producer.send(new ProducerRecord<>("orders.enriched.v1", r.key(), enrich(r.value())));
    jdbc.upsert("orders_view", r.value());       // ⚠️ outside the Kafka transaction
}
producer.sendOffsetsToTransaction(offsetsOf(records), consumer.groupMetadata());
producer.commitTransaction();                     // Kafka is atomic; the DB is not.
// Crash after the JDBC write but before commitTransaction:
//   => DB row exists, Kafka offset not advanced => the record is reprocessed
//   => the DB write MUST be idempotent (UPSERT on order_id) or the DB write must
//      happen in a way that can be rolled back with the Kafka commit.
```

Correct cross-system patterns (pick one and document it):

```text
(a) Offsets in the sink DB, one transaction:
      BEGIN; UPSERT orders_view ...; UPDATE kafka_offsets SET off=4211 WHERE topic/partition; COMMIT;
      => atomic for that hop; Kafka's group offsets are ignored for this consumer
         (use assign(), not subscribe(), or simply never commit to Kafka).

(b) Idempotent sink + at-least-once Kafka commits:
      Kafka txn or manual commit + UPSERT with a version guard.

(c) Transactional file/table commit:
      Delta/Iceberg/Hudi with txnVersion / snapshot commit = the sink participates.
```

### 4.3 Spark Structured Streaming: end-to-end exactly-once to Delta

```python
from pyspark.sql import functions as F
from delta.tables import DeltaTable

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", "kafka1:9092")
       .option("subscribe", "orders.v1")
       .option("startingOffsets", "earliest")
       .option("kafka.isolation.level", "read_committed")   # skip aborted producer txns
       .load()
       .select(F.from_json(F.col("value").cast("string"), schema).alias("e"))
       .select("e.*"))

def upsert_batch(batch_df, batch_id):
    # Two things make this exactly-once:
    #  (1) the checkpoint re-runs the SAME batch on failure (deterministic input range)
    #  (2) txnVersion makes the write idempotent at the table level
    if batch_df.isEmpty():
        return
    (batch_df.write
        .format("delta")
        .mode("append")
        .option("txnAppId", "orders-silver")   # combined with batch_id => dedup marker
        .option("txnVersion", batch_id)
        .option("mergeSchema", "true")
        .saveAsTable("silver.orders_raw"))

query = (raw.writeStream
         .foreachBatch(upsert_batch)
         .outputMode("append")
         .option("checkpointLocation", "s3://checkpoints/orders-silver/")
         .trigger(processingTime="30 seconds")
         .start())
```

For MERGE-style idempotency (dedup at the data level, independent of Spark's bookkeeping):

```sql
MERGE INTO silver.orders AS t
USING (SELECT * FROM staged_orders) AS s
ON t.order_id = s.order_id
WHEN MATCHED AND s.event_version > t.event_version THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

### 4.4 PyFlink: exactly-once Kafka → Kafka

```python
from pyflink.common import Types, WatermarkStrategy, Duration
from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaSink, KafkaOffsetsInitializer, KafkaRecordSerializationSchema,
    DeliveryGuarantee,
)
from pyflink.datastream.checkpoint_storage import FileSystemCheckpointStorage

env = StreamExecutionEnvironment.get_execution_environment()
env.enable_checkpointing(10_000)                               # 10s checkpoints
env.get_checkpoint_config().set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)
env.get_checkpoint_config().set_checkpoint_storage(
    FileSystemCheckpointStorage("file:///flink/checkpoints"))

source = (KafkaSource.builder()
    .set_bootstrap_servers("kafka1:9092")
    .set_topics("orders.v1")
    .set_group_id("flink-orders-enricher")     # for monitoring visibility
    .set_starting_offsets(KafkaOffsetsInitializer.earliest())
    .set_property("isolation.level", "read_committed")
    .set_value_only_deserializer(SimpleStringSchema())
    .build())

sink = (KafkaSink.builder()
    .set_bootstrap_servers("kafka1:9092")
    .set_record_serializer(
        KafkaRecordSerializationSchema.builder()
            .set_topic("orders.enriched.v1")
            .set_value_serialization_schema(SimpleStringSchema())
            .build())
    .set_delivery_guarantee(DeliveryGuarantee.EXACTLY_ONCE)   # two-phase commit producer
    .set_transactional_id_prefix("flink-orders-enricher")
    .build())

(env.from_source(source, WatermarkStrategy.no_watermarks(), "orders")
     .map(enrich, output_type=Types.STRING())
     .sink_to(sink))

env.execute("orders-enricher-eos")
```

Flink SQL equivalents:

```sql
-- Flink SQL: exactly-once end-to-end to Kafka, with a committable producer
CREATE TABLE orders_src (
  order_id STRING, amount DECIMAL(12,2), currency STRING,
  event_time TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'orders.v1',
  'properties.bootstrap.servers' = 'kafka1:9092',
  'properties.group.id' = 'flink-orders-sql',
  'properties.isolation.level' = 'read_committed',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);

CREATE TABLE orders_sink (
  order_id STRING, amount DECIMAL(12,2), currency STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'orders.enriched.v1',
  'properties.bootstrap.servers' = 'kafka1:9092',
  'sink.delivery-guarantee' = 'exactly-once',
  'sink.transactional-id-prefix' = 'flink-orders-sql',
  'format' = 'json'
);

INSERT INTO orders_sink SELECT order_id, amount, currency FROM orders_src;
```

### 4.5 Semantics matrix: what to configure where

| Component | At-most-once | At-least-once | Exactly-once (to Kafka) |
|---|---|---|---|
| Kafka producer | `acks=0` | `acks=all`, `enable.idempotence=false` | `acks=all`, `enable.idempotence=true`, `transactional.id`, `initTransactions/begin/commit` |
| Kafka consumer | auto-commit before processing | manual commit after processing | `read_committed` + `sendOffsetsToTransaction` |
| Kafka Streams | — | `processing.guarantee=at_least_once` | `processing.guarantee=exactly_once_v2` |
| Flink | `CheckpointingMode.AT_LEAST_ONCE` | checkpoints + at-least-once sink | `EXACTLY_ONCE` + 2PC sink or idempotent sink |
| Spark SS | no checkpoint (unsafe) | checkpoint + at-least-once sink | checkpoint + `foreachBatch` with txn/unique key, or file/Kafka sink |
| S3/Parquet sink | — | overwrite likely duplicates | transactional committers / Iceberg / Delta `txnVersion` |

## 5. Where to implement it

| Layer | Tool | Notes |
|---|---|---|
| Producer idempotence/transactions | Kafka clients, librdkafka (Python/C/Go) | `transactional.id` must be unique per concurrent instance |
| Stream processing EOS | Kafka Streams (`exactly_once_v2`), Flink (`EXACTLY_ONCE` + 2PC), Spark (`foreachBatch`) | Trade latency for correctness |
| Lake sinks | Delta `txnVersion`, Iceberg snapshot-isolation commits, Hudi `delete`/`upsert`, S3 committers | The table format is what makes file writes atomic |
| Warehouse sinks | Snowflake `MERGE` + `INSERT ... SELECT` in one statement; BigQuery `MERGE`/DML; Redshift MERGE | Dedup on a business key; put the offsets/dedup marker in the same statement |
| Managed services | Confluent Cloud (transactions enabled by default), MSK, Flink on Confluent/Kinesis Analytics | Check service limits on transaction timeouts and producer id allocations |
| CDC | Debezium | At-least-once by default; use the outbox pattern for atomicity |
| External APIs | Client-side idempotency keys | The receiver must persist the key |

> **Senior note.** Transactional producers have real costs: an extra round trip per transaction, broker-side state per PID/partition, and possible LSO-induced consumer latency. Do not turn on EOS "just in case". Turn it on where a duplicate breaks a business invariant, and measure the p99 effect. Many teams find that **idempotent sinks + at-least-once** meet the business requirement at a fraction of the complexity.
>
> Also note the classic Confluent Cloud / MSK gotcha: transactional producers require specific broker versions and configs; and `transactional.id` collisions across instances cause persistent fencing. In Kubernetes, derive it from the pod ordinal (StatefulSet), not a random name.

## 6. Patterns and anti-patterns

| Pattern | Mechanism | Fits |
|---|---|---|
| **Idempotent consumer** | Dedup table or version-guarded upsert | Everything at-least-once |
| **Transactional consume-transform-produce** | `sendOffsetsToTransaction` | Kafka → Kafka |
| **Offsets-in-sink (unit of work)** | Offsets written with the data in one DB transaction | Kafka → DB |
| **Transactional file/table commit** | Delta/Iceberg/Hudi txn ids, S3 committers | Lakehouse sinks |
| **Idempotency keys on external APIs** | Key stored receiver-side | Webhooks, payments |
| **Deterministic business key** | `(aggregate_id, version)` uniqueness | Any store with a unique index |
| **Retry topics + DLQ** | Bounded retries with backoff, then a DLQ | Poison messages |
| **Reconciliation job** | Nightly diff between source and sink | Detects silent drift |
| **Sequence/tombstone-based dedup** | Keep the max version per key | Late/out-of-order duplicates |

### Anti-patterns

| Anti-pattern | Consequence | Fix |
|---|---|---|
| "Kafka gives exactly-once so we're fine" | Sink duplicates/losses | Boundary analysis; idempotent sink |
| Auto-commit + EOS producer | Offsets advance independently of the transaction | `sendOffsetsToTransaction` |
| Non-unique `transactional.id` across instances | Constant `ProducerFencedException`, pipeline stalls | One id per task/instance |
| Long transactions | `read_committed` consumers stall at the LSO | Short transactions; bounded batches |
| Only one EOS producer per app for parallelism | Cannot parallelize beyond one open transaction | KIP-447-style multiple producers/consumers |
| Spark EOS claimed without sink support | Silent duplicates in the warehouse | Idempotent sink + txnVersion/MERGE |
| Re-randomized keys across a retry | Dedup fails, duplicates slip through | Deterministic keys on retry |
| Dedup table without expiry | Unbounded storage growth | Time-partitioned dedup table |
| Checking `isolation.level=read_uncommitted` "for speed" | Processing aborted records | Keep `read_committed` where transactions are used |
| Assuming `read_committed` means globally ordered | It does not; ordering is per partition | Keys + co-partitioning |
| EOS everywhere, including analytics | Cost with no business benefit | Reserve EOS for invariants |
| Ignoring `transaction.timeout.ms` interactions | Hung transactions block consumers for up to 15 min | Alert on open transactions |

## 7. Real-world use cases and industry applications

- **Payments and ledgers (Stripe/PayPal-style)**: idempotency keys at the API boundary, exactly-once postings keyed by a business transfer id, and reconciliation jobs as the last line of defense.
- **Usage-based billing**: Kafka → Flink/Spark EOS → Delta/Iceberg, with the invoice line item uniquely keyed by `(customer, usage_window, meter)`.
- **Fraud detection**: at-least-once is acceptable for alerts (dedup on the alert key); exactly-once for case creation and SAR filings.
- **Ad tech / attribution**: dedup on `impression_id`; late/duplicate events are tolerated with idempotent joins.
- **IoT telemetry into a lake**: EOS file commits prevent duplicate rows that would corrupt aggregates.
- **Retail inventory sync**: idempotent decrements keyed by `(sku, order_id)`; the write is naturally idempotent so at-least-once is enough.
- **CDC to a warehouse**: Debezium at-least-once + `MERGE` on the primary key = effectively-once at the table level.
- **Multi-region replication**: MirrorMaker 2 is at-least-once (it can duplicate on failover); consumers must dedup — a documented and often-forgotten caveat.

## 8. Senior-level deep dive

### 8.1 Why "exactly-once network delivery" is impossible (and why EOS still works)

A network cannot guarantee exactly-once *delivery*: if a message is lost, the sender cannot distinguish "lost" from "slow", and retrying causes duplicates. What is achievable is exactly-once **effects**:

```text
Delivery:  at-most-once or at-least-once (fundamental).
Effects:   exactly-once, via
           (1) atomic commit (transaction spanning the effect + the progress marker), or
           (2) idempotence (reapplying the effect is a no-op, detected by a unique key), or
           (3) a combination (transactions to make the common case cheap, idempotence as a backstop).
```

This is why "exactly-once with Kafka" is really "idempotent producer (kill duplicates) + transaction (kill partial writes) + read_committed (hide aborted data)".

### 8.2 Cost model of EOS

| Dimension | Effect |
|---|---|
| Latency | +1 round trip per transaction (commit marker); `commit.interval.ms` sets a floor (Streams/Flink: 100 ms–10 s) |
| Throughput | Transaction overhead per batch; smaller effective batches; broker sync on marker writes |
| Consumer latency | LSO stalls: with an open transaction, `read_committed` consumers cannot advance past the LSO on those partitions |
| Broker resources | Producer state per PID/partition (~tens of MB per 10k partitions), extra metadata in the log |
| Recovery | Aborted data lingers until the transaction timeout; coordinator state must be reconstructed |
| Operational | Fencing bugs are subtle; `transactional.id` design is a real constraint |
| Spark/Flink | Checkpoint frequency becomes a latency/throughput trade-off; long checkpoints risk timeouts |

### 8.3 Checkpointing mechanics comparison (Flink vs Spark)

| Aspect | Flink | Spark Structured Streaming |
|---|---|---|
| Model | Streaming with barriers; async state snapshots | Micro-batches with an offset WAL and batch commits |
| State location | Configured checkpoint storage (S3/HDFS/rocksdb) | `checkpointLocation` (offsets/, commits/, state/) |
| Offset commit to Kafka | Optional (`setCommitOffsetsOnCheckpoints`) | Optional (`kafka.group.id`) — the checkpoint is authoritative |
| Recovery unit | Latest completed checkpoint | Last committed batch |
| Exactly-once sink | `TwoPhaseCommitSinkFunction` / `DeliveryGuarantee.EXACTLY_ONCE` | `foreachBatch` + txn-versioned writes; built-in file/Kafka sinks |
| Latency cost | Checkpoint interval + barrier alignment (unaligned checkpoints mitigate backpressure) | Trigger interval (default: as fast as possible) |
| Backpressure behavior | Barriers can be delayed; unaligned checkpoints keep them flowing | Longer micro-batches; batch duration grows |
| Failure duplicate window | Records between snapshot and failure | The whole failed micro-batch |

### 8.4 Choosing idempotence over transactions (the most common senior decision)

```text
Given:  duplicates cost money, loss costs more.
Option A: EOS transactions everywhere.
   + clean semantics inside Kafka
   - latency, complexity, one-open-txn-per-producer limits

Option B: at-least-once + idempotent sink keyed by (aggregate_id, version|event_id).
   + simple, fast, works with any sink that has a unique index
   - requires a deterministic key and a guarding predicate (version > current)
   - requires dedup storage/cleanup discipline

Decision rule: use B unless the sink cannot enforce uniqueness (append-only logs,
external APIs without idempotency) — then use A or a transactional table format.
```

A version-guarded upsert makes retries free *and* handles out-of-order delivery:

```sql
INSERT INTO orders_view AS t (order_id, status, version, updated_at)
VALUES ('ORD-1', 'SHIPPED', 7, now())
ON CONFLICT (order_id) DO UPDATE
   SET status = EXCLUDED.status,
       version = EXCLUDED.version,
       updated_at = now()
 WHERE t.version < EXCLUDED.version;   -- stale/duplicate events are no-ops
```

### 8.5 Failure-mode catalog

| Failure | Semantic result | Detection | Mitigation |
|---|---|---|---|
| Producer retry after timeout (no idempotence) | Duplicates in the topic | Count anomalies | Enable idempotence |
| Crash between `poll()` and commit | Reprocessing (duplicates downstream) | Duplicate rows/dedup counters | Idempotent sink |
| Crash between commit and side effect | Data loss | Reconciliation job | Commit after effects; or offsets-in-sink |
| Aborted transaction not yet timed out | Consumers stalled at LSO | Stuck lag + open transaction metrics | Short transactions; abort on restart via shared `transactional.id` |
| Fenced producer loops | Pipeline stalls, no output | `ProducerFencedException` in logs | Unique `transactional.id` per instance |
| Spark WAL present but batch uncommitted | Batch re-executed | Duplicate batch in the sink | Idempotent sink |
| Flink checkpoint failure under backpressure | Checkpoint timeouts, restarts from an older snapshot | Checkpoint failure metrics | Unaligned checkpoints, more memory, higher interval |
| EOS assumed into a non-participating API | Silent duplicates/partial effects | Customer complaints | Idempotency keys |
| MM2 failover duplicates | Duplicate deliveries | Cross-region dedup counters | Consumer-side dedup |
| Dedup window shorter than redelivery window | Duplicates slip through | Rising duplicate rate | Size dedup retention > max replay window |

### 8.6 Debugging playbook

```text
1. Declare the intended semantics per hop (write it down).
2. Find the first hop where reality ≠ intent:
     - duplicates?  source: `kafka-dump-log` (are duplicates in the topic?)
                    sink: query for duplicate business keys
     - losses?      reconciliation query (source count vs sink count per key)
3. If duplicates are in the TOPIC: producer-side (retries without idempotence,
   dual write, MM2). Check producer logs for retries and `enable.idempotence`.
4. If duplicates appear only in the SINK: consumer/sink side (commit strategy,
   non-idempotent writes, Spark batch re-execution).
5. If data is missing: commit-before-process, `auto.offset.reset` misuse, offset
   expiry, `failOnDataLoss=false`, retention deletion.
6. Transactions: check `__transaction_state` for stuck transactions, and the
   consumers' LSO-based lag (`records-lag` > 0 with no new records = LSO stall).
7. Only then tune throughput.
```

## 9. Common mistakes and how to avoid them

1. **Claiming "exactly-once" without naming the transactional sink.** Write the boundary sentence in the design doc.
2. **`acks=0` or `acks=1` where a lost event is a business incident.** Baseline: `acks=all`, `min.insync.replicas=2`, RF 3.
3. **Transaction per record.** Kills throughput; batch, then commit.
4. **Reusing random/ephemeral `transactional.id`s.** Fencing storms in Kubernetes.
5. **Not handling non-retriable commit errors.** Zombie consumers must exit, not retry.
6. **Mixing `read_committed` and `read_uncommitted` on the same data.** Correctness bug.
7. **Forgetting that Kafka Streams EOS does not cover external side effects.**
8. **Assuming Spark's checkpoint alone gives exactly-once.** The checkpoint gives at-least-once input; the sink decides.
9. **Dedup keys computed from non-deterministic data** (timestamps, random ids) that change on retry.
10. **Unbounded dedup tables.** Time-partition and purge; keep the window > your max replay/redelivery window.
11. **Ignoring transaction timeouts.** A hung producer stalls `read_committed` consumers for up to the timeout.
12. **No reconciliation job.** You will not learn about drift until the business does.
13. **Treating "idempotent" as "the same message twice is harmless" without checking** — e.g. `SET balance = 42` is idempotent, `SET balance = balance + 10` is not.
14. **Assuming ordering is preserved with idempotent producers and >5 in-flight requests.** The dedup window is bounded.

## 10. Exercises / interview questions

*(Hints only.)*

1. A pipeline is "exactly-once" from Kafka to Kafka, then the consumer writes to Postgres with `INSERT`. What is the true end-to-end semantic? What single change makes it effectively-once? *(Hint: unique index + `ON CONFLICT`.)*
2. Explain how the LSO causes consumer lag without any producer error. Which config bounds the stall? *(Hint: open transaction, `transaction.timeout.ms` vs `transaction.max.timeout.ms`.)*
3. Why does an idempotent producer eliminate duplicates but not guarantee exactly-once *processing*? *(Hint: scope: single producer session, no offset atomicity, no sink coverage.)*
4. Design the key for a dedup table in a Kafka → S3 Iceberg pipeline that must be idempotent for a 30-day replay window. Where do you store the dedup index and how do you bound its size? *(Hint: event id + snapshot commits.)*
5. Spark SS re-executes batch 1234 after a crash; the sink is a Delta table written with `mode("append")` and no txnVersion. What happens, and what are two fixes?
6. You must decide between (a) EOS transactions in Kafka Streams and (b) at-least-once + upserts in the sink. List the five questions that decide it.
7. Give an example of a non-idempotent operation in your own domain, and design its idempotent form.
8. A Flink job with a JDBC sink claims exactly-once. Is that possible? Under what conditions? *(Hint: upsert requirement, key, and whether the connector supports it.)*
9. Your Kafka → Kafka EOS job stalls: no output, lag grows, no errors except occasional `ProducerFencedException`. Diagnose it. *(Hint: duplicate transactional.id across instances after a rolling restart.)*
10. Write the one-paragraph "delivery semantics" section of a design doc for an order pipeline, naming each hop and its guarantee. What would a reviewer challenge?

## References

- Apache Kafka — Message Delivery Semantics (design doc): <https://kafka.apache.org/documentation/#semantics>
- Confluent — Delivery semantics (at-most-once / at-least-once / exactly-once): <https://docs.confluent.io/kafka/design/delivery-semantics.html>
- Confluent — Kafka transactions and the transaction coordinator internals: <https://docs.confluent.io/platform/current/kafka/transactions.html>
- Confluent — Exactly-once semantics are possible: here's how Kafka does it: <https://www.confluent.io/blog/exactly-once-semantics-are-possible-heres-how-apache-kafka-does-it/>
- Confluent — Transactions in Apache Kafka (how the protocol works): <https://www.confluent.io/blog/transactions-apache-kafka/>
- KIP-98: Exactly Once Delivery and Transactional Messaging: <https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging>
- KIP-447: Producer scalability for exactly once semantics: <https://cwiki.apache.org/confluence/display/KAFKA/KIP-447%3A+Producer+scalability+for+exactly+once+semantics>
- Apache Kafka — producer configs (`enable.idempotence`, `transactional.id`, `transaction.timeout.ms`): <https://kafka.apache.org/documentation/#producerconfigs>
- Confluent — Kafka Streams developer guide (`processing.guarantee=exactly_once_v2`): <https://docs.confluent.io/platform/current/streams/developer-guide/config-streams.html>
- Flink — DataStream connector guarantees (which sinks can be exactly-once): <https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/datastream/guarantees/>
- Flink — Kafka connector (`DeliveryGuarantee.EXACTLY_ONCE`, transactional ids): <https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/datastream/kafka/>
- Flink — checkpointing: <https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/fault-tolerance/checkpointing/>
- Flink — state and fault tolerance concepts: <https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/fault-tolerance/state/>
- Flink — Table API connectors (Kafka with `sink.delivery-guarantee`): <https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/kafka/>
- Flink — connector overview / guarantees table: <https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/overview/>
- Spark — Structured Streaming programming guide (checkpointing, recovering from failures): <https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html>
- Spark — Structured Streaming + Kafka integration (`startingOffsets`, `isolation.level`): <https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html>
- Spark — `foreachBatch` API reference: <https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.DataStreamWriter.foreachBatch.html>
- Delta Lake — idempotent streaming writes (`txnAppId`, `txnVersion`): <https://docs.delta.io/latest/delta-streaming.html>
- Apache Iceberg — streaming writes / snapshot commit semantics: <https://iceberg.apache.org/docs/latest/spark-structured-streaming/>
- Databricks — Structured Streaming concepts (state, watermarks, checkpointing): <https://docs.databricks.com/aws/en/structured-streaming/>
- microservices.io — Idempotent consumer pattern: <https://microservices.io/patterns/communication-style/idempotent-consumer.html>
- Debezium — change data capture semantics and the outbox pattern: <https://debezium.io/documentation/reference/stable/architecture.html>
