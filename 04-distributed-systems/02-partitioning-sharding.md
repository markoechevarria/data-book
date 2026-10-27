# Partitioning / Sharding

> Partitioning (also called sharding) is how you split one logical dataset across many machines so that storage, write throughput, and read throughput all scale horizontally. It is the difference between "our Postgres capped out at 2 TB and 8k writes/s" and "we run on 200 nodes and add capacity by adding nodes". It is also where the hardest data-engineering problems live: a single hot key can melt a 100-node cluster, a badly chosen shard key forces you to rewrite your entire partitioning scheme at 3 a.m., and cross-shard joins/transactions turn a one-line SQL query into a distributed systems project. This chapter gives you the internals (hash rings, token ranges, consistent hashing, rebalancing), the practical mechanics in every system you will touch (Postgres, Kafka, Cassandra, DynamoDB, Snowflake, Spark, Iceberg), and the senior-level judgment to pick keys that survive growth.

## Why a Data Engineer Cares

- **Spark is a partitioning engine.** `repartition`, `partitionBy`, shuffle partitions, and skew all come straight from this chapter. A bad partition key in Spark produces one task doing 90% of the work (data skew), and AQE exists largely to fight partitioning mistakes.
- **Parquet/Iceberg/Delta are partitioned on disk.** `PARTITION BY (date, region)` decides how much data a query reads (partition pruning). Over-partitioning creates the "small files problem"; under-partitioning forces full scans.
- **Kafka *is* partitioning.** A topic's partition count fixes your maximum parallelism and ordering granularity. Choosing a message key = choosing a partition strategy.
- **Warehouses expose it.** Snowflake micro-partitions + clustering keys, BigQuery partitioning + clustering, Redshift distribution styles (KEY/EVEN/ALL) and sort keys — all are partitioning decisions with direct cost/pruning consequences.
- **Distributed SQL** (Spanner, CockroachDB, Yugabyte) hides sharding behind the scenes but still makes you choose primary keys, and a bad key ("monotonic increasing PK") creates a hot spot in every one of them.

Partitioning is the physical layer under every scalability story you will ever tell.

## TL;DR

- **Partitioning = splitting data into disjoint subsets (partitions/shards) placed on different nodes.** It scales storage and throughput and enables parallelism. Replication (ch. 1) and partitioning are orthogonal and usually combined (each partition has N replicas).
- **Three common schemes:** **range** (great for range scans, exposes hot spots on sequential keys), **hash** (even spread, kills range scans), and **consistent hashing / rendezvous** (minimize data movement when nodes change).
- **The shard key is the whole game.** It should (a) spread load evenly, (b) match your dominant access pattern, (c) avoid monotonic values, and (d) minimize cross-shard operations.
- **Hot partitions are the #1 scaling failure.** Caused by skewed keys, celebrity/whale tenants, monotonically increasing keys, or low-cardinality keys. Fixes: finer key granularity, salting, composite keys, write sharding, dedicated partitions for whales.
- **Rebalancing** can be **fixed partition count** (change assignment only — easy) or **dynamic split/merge** (change count too — better balance, more complex). Do it online with double-writes/read-fallback, never with a downtime window.
- **Secondary indexes** come in two flavors: **document-partitioned (local)** — index per shard, reads must scatter-gather; and **term-partitioned (global)** — index by term, writes fan out. Pick based on read/write balance.
- **Cross-shard joins and transactions** are the tax. Avoid in design; when unavoidable use scatter-gather, co-location, 2PC, or sagas.
- **Partition pruning** (skipping partitions that can't match the filter) is the single biggest analytical performance lever — design physical layout around the query predicates.

---

## 1. Definitions and Core Vocabulary

| Term | Meaning |
|---|---|
| **Partition / Shard** | A disjoint subset of the data. "Partition" is preferred in analytical/OLAP contexts, "shard" in OLTP; they're the same idea. |
| **Partition key / Shard key / Routing key** | The attribute used to decide which partition a record belongs to. |
| **Partition function** | The deterministic mapping `key → partition id` (`hash(key) % N`, `range(key)`, `consistent_hash(key)`). |
| **Rebalancing / Resharding** | Moving data between nodes when the cluster or load changes. |
| **Hot partition / Hot spot** | A partition receiving disproportionate traffic (skew). |
| **Celebrity / Whale problem** | One key (a celebrity, a large tenant) that is vastly hotter than all others. |
| **Skew** | Uneven distribution of data or load across partitions. |
| **Co-location** | Placing related data (e.g., all rows of a tenant) on the same partition to make joins/transactions local. |
| **Scatter-gather** | Querying all shards and merging results (needed for non-key lookups and global aggregates). |
| **Local (document-partitioned) index** | A secondary index maintained *within each partition*, covering only that partition's data. |
| **Global (term-partitioned) index** | A secondary index itself partitioned *by the indexed term*, spanning all data. |
| **Partition pruning** | Skipping partitions that cannot satisfy a query predicate (huge for OLAP). |
| **Routing / Coordination service** | Component that tells clients which node owns a key (ZooKeeper, etcd, gossip, catalog). |
| **Virtual node (vnode)** | Multiple points on a hash ring per physical node, smoothing load distribution and speeding rebalance. |
| **Consistent hashing** | Hashing scheme where adding/removing a node moves only ~`1/N` of keys. |
| **Rendezvous (HRW) hashing** | Hash each key against each node, pick max — no ring, naturally balanced, minimal movement. |
| **Monotonic key** | A key (timestamp, auto-increment ID) that grows over time; concentrated writes on the "last" shard. |
| **Micro-partition** | Snowflake's immutable, automatically-created storage unit (~50–500 MB compressed); the unit of pruning. |
| **Token range** | Cassandra's notion: the ring of `2^64` token values divided among nodes. |

> **Senior note:** Partitioning and replication are **orthogonal axes**. Partitioning splits *which* data; replication copies *each* split for durability. The classic layout: `P partitions × N replicas = P×N copies`. Confusing the two is why "we already have replicas, why is it slow?" is a common misconception — replicas scale *reads*, not *storage* or *write* throughput.

---

## 2. What Problem It Solves / Why It Matters

A single machine has hard walls:

| Resource | Single-node wall | Partitioning effect |
|---|---|---|
| **Disk / dataset size** | A few TB (NVMe) before cost/copy pain | Dataset = sum of node disks |
| **Write throughput** | ~10k–100k writes/s | Scales ~linearly with shards |
| **Read throughput** | Capped by CPU/IOPS | Scales with shards (and replicas) |
| **Query parallelism** | Number of cores | Each shard explores in parallel |
| **Blast radius** | Whole dataset at risk | Failure isolated to a shard |

**Business mapping:**

- "We outgrew our database" → you need partitioning (or you need to reduce data; both are valid).
- "Our pipeline takes 9 hours" → partition the data so Spark can run 200 tasks instead of 4.
- "Our report scans 40 TB to read 2 GB" → partition/prune.
- "Adding nodes didn't help" → the data isn't partitioned, so it's still one node's problem.

**The cost:** every partition boundary is a place where a *local, fast* operation becomes a *distributed, slow* one. Joins, transactions, aggregations, and uniqueness constraints that were free on one node now require coordination. The whole art is placing boundaries so the common operations stay local.

---

## 3. How It Works Internally (Mechanisms, Internals)

### 3.1 Range partitioning

Assign contiguous key ranges to partitions. Keys stay sorted within a partition.

```
Key range partitioning (e.g., by user_id):
  P0: [0,        10_000_000)
  P1: [10_000_000, 20_000_000)
  P2: [20_000_000, 30_000_000)
  ...

+  range scans are cheap (B-tree locality, sequential IO)
+  predictable layout (great for time-series by month)
−  hot spots when traffic concentrates on one range
   (e.g., partition by timestamp => all "now" writes hit the newest partition)
```

Used by: HBase, Bigtable, Spanner/Cockroach (key ranges + splitting), Postgres declarative range partitioning, BigQuery `PARTITION BY DATE(...)`, Iceberg/Delta `PARTITIONED BY (day)`.

### 3.2 Hash partitioning

Apply a hash to the key, then modulo (or a smarter map):

```
hash partitioning:
  partition = hash(key) % N            # N = number of partitions

+  even spread, no "newest range" hot spot
−  range scans are now scatter-gather across ALL partitions
−  changing N remaps ~most keys (the classic modulo problem)
```

**The modulo problem:** going from `N=4` to `N=5` with `hash % N` moves ~80% of keys, because almost every `hash % 4 != hash % 5`. That is why **consistent hashing** exists.

### 3.3 Consistent hashing

Place nodes and keys on a ring of size `2^m`. A key belongs to the first node clockwise.

```
            hash ring (0 … 2^32)
              ┌───────────────┐
        keyA ►│   Node A      │
   ┌──────────┤               ├──────────┐
   │          └───────────────┘          │
   │                                     │
┌──┴───┐                            ┌────┴───┐
│Node C│                            │ Node B │  keyB ►
└──────┘                            └────────┘
   │          ┌───────────────┐          │
   └──────────┤   Node D      ├──────────┘
              └───────────────┘

Add a node → only keys it "claims" move (~1/N).
Remove a node → only its keys move to the next node clockwise.
```

**Problem:** with few nodes, the arcs are uneven, so load is uneven. **Fix: virtual nodes** — each physical node gets many (100–256) points on the ring. Load smooths out, and when a node leaves, its load is spread across many others rather than dumped on one successor.

```
Physical node A → vnodes {A#1 … A#256} scattered on the ring
Physical node B → vnodes {B#1 … B#256}
This averages out randomness: std-dev of load shrinks ~1/sqrt(vnodes).
```

Used by: Dynamo, Cassandra (256 vnodes/node default), Riak, Amazon's `sharddb`, many caches (Redis Cluster uses fixed 16384 hash slots instead).

### 3.4 Rendezvous (highest-random-weight) hashing

For each `(key, node)`, compute `score = hash(key, node)`; the key belongs to the node with the max score.

```
owner(key) = argmax over nodes n of hash(key, n)

+  balanced (with a good hash), no ring data structure, no vnodes to tune
+  adding a node moves only that node's share of keys (~1/N)
−  O(N) hashes per lookup (fine for moderate N; can be cached/bounded)
```

Used by: CDNs, distributed caches, some load balancers.

### 3.5 Comparison

| Scheme | Balance | Key movement on scale | Range scans | Complexity |
|---|---|---|---|---|
| Range | Depends on data | Minimal (split a range) | Excellent | Low |
| Hash % N | Excellent | Terrible (~all keys) | None (scatter) | Trivial |
| Consistent hashing | Good (with vnodes) | ~1/N | None (scatter) | Medium |
| Rendezvous | Excellent | ~1/N | None (scatter) | Low |
| Fixed slots (Redis) | Good (reshard slots) | Control by moving slots | None | Medium |

### 3.6 Secondary indexes: local vs global

When you look up by something other than the partition key.

```
Document-partitioned (LOCAL):                 Term-partitioned (GLOBAL):
each shard indexes its OWN rows              the index is partitioned by the TERM

  P0 [rows 0..1M] → idx_P0                     idx "color=red"   → shard X
  P1 [rows 1M..2M]→ idx_P1                     idx "color=blue"  → shard Y
  P2 [rows 2M..3M]→ idx_P2                     idx "color=green" → shard Z
     ▲                                            ▲
   query "color=red" must ask ALL shards         query "color=red" asks ONE shard
   (scatter-gather)                              (but writes must update the global index)

+ local: writes are cheap (only local shard)  + global: reads for a term are concentrated
− local: reads fan out to every shard         − global: writes fan out; index can lag (async)
```

| | Local (document-partitioned) | Global (term-partitioned) |
|---|---|---|
| Write cost | Low (one shard) | High (index shard(s) globally) |
| Read cost for non-key query | High (scatter-gather) | Low |
| Consistency | Easy (same shard) | Hard (index async → stale reads) |
| Examples | Most NoSQL (per-partition indexes), DynamoDB LSIs | DynamoDB GSIs (term-partitioned!), Elasticsearch, Cassandra SASI/global indexes |

> **Senior note:** **DynamoDB GSIs are term-partitioned** and updated asynchronously. A `Query` on a GSI is eventually consistent by design, and a hot GSI partition (bad GSI PK) throttles writes to the base table. This is partitioning theory you will hit in production.

### 3.7 How clients find the right partition

Three routing strategies:

1. **Client-side routing / gossip:** the client library knows the ring (Cassandra drivers). Lowest latency per hop, but every client must track topology.
2. **Routing tier / proxy:** a smart proxy maps keys → nodes (MongoDB `mongos`, Vitess VTgate, ProxySQL, CockroachDB gateway).
3. **Coordination service:** a small consensus cluster (ZooKeeper, etcd) holds the authoritative partition map; clients/proxies watch it.

```
        ┌──────────────┐
client ─▶│ routing tier │──▶ knows partition map ──▶ shard 7
        └──────┬───────┘
               │ watches
        ┌──────▼───────┐
        │  ZooKeeper / │  (authoritative map: key → owner)
        │     etcd     │
        └──────────────┘
```

### 3.8 Rebalancing: fixed vs dynamic partitioning

**Fixed number of partitions** (Kafka, Redis Cluster, Riak): create far more partitions than nodes (e.g., 1000 partitions on 10 nodes). Adding a node moves whole partitions; the *count* never changes.

```
Before: 10 nodes × 100 partitions     After: 11 nodes × ~91 partitions
Move whole partitions (fast, atomic, no re-hash of keys inside a partition).
Choose partition count >> node count so future growth is easy.
```

**Dynamic partitioning** (HBase, Spanner, MongoDB, Cassandra vnodes): partitions split when they get too big / too hot and merge when small.

```
range A [0..1M] is hot → split into [0..500k] and [500k..1M] on two nodes
```

| | Fixed count | Dynamic |
|---|---|---|
| Rebalance operation | Move partition | Split/merge then move |
| Complexity | Low | High |
| Balance quality | Good if count ≫ nodes | Excellent (adapts to data) |
| Good for | Logs/streams (Kafka) | Key-value stores with skew |

> **Senior note:** Kafka's "add brokers doesn't rebalance automatically" behavior is a consequence of *fixed-count* partitioning: partitions are the unit of placement, and you must explicitly move some with `kafka-reassign-partitions.sh`. Teams that expect automatic rebalancing get 40% idle capacity.

### 3.9 Splitting, merging, and partition evolution

- **Split:** one hot/large partition becomes two. Must be atomic: record the split point in the routing map, then stream the second half to the new owner, then flip reads.
- **Merge:** two small partitions combine (Iceberg/Delta "partition evolution" and compaction).
- **Partition evolution:** changing the partitioning scheme *without rewriting history* — Iceberg and Delta support this (old files keep the old layout; the metadata describes both), which is a huge advantage over Postgres/Hive where you'd rewrite the table.

---

## 4. How to Implement It (Step-by-Step, with Code / Config)

### 4.1 PostgreSQL: declarative partitioning

```sql
-- Range partitioning by month (great for time-series, enables pruning)
CREATE TABLE events (
    id           bigint,
    occurred_at  timestamptz NOT NULL,
    tenant_id    bigint,
    payload      jsonb
) PARTITION BY RANGE (occurred_at);

CREATE TABLE events_2026_01 PARTITION OF events
    FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');
CREATE TABLE events_2026_02 PARTITION OF events
    FOR VALUES FROM ('2026-02-01') TO ('2026-03-01');

-- Hash partitioning by tenant (spreads writes, keeps a tenant on one partition)
CREATE TABLE sessions (
    id bigint, tenant_id bigint, data jsonb
) PARTITION BY HASH (tenant_id);
CREATE TABLE sessions_p0 PARTITION OF sessions FOR VALUES WITH (MODULUS 8, REMAINDER 0);
-- ... p1..p7

-- Verify pruning:
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM events WHERE occurred_at >= '2026-02-01' AND occurred_at < '2026-03-01';
-- Expect: scans only events_2026_02
```

**Sharding across *machines*** in Postgres needs an extension (Citus) or an external layer (Vitess is MySQL; for PG use Citus):

```sql
-- Citus: distribute a table by a shard key across worker nodes
SELECT create_distributed_table('orders', 'customer_id');
-- Co-locate a second table on the same key → joins stay local
SELECT create_distributed_table('order_items', 'customer_id');
```

### 4.2 Kafka: partitions = the unit of parallelism

```bash
kafka-topics.sh --create --topic clicks \
  --partitions 24 --replication-factor 3
```

Producer partitioning logic:
- **With a key:** `partition = murmur2(key) % numPartitions` → all events with the same key are ordered, but a hot key = a hot partition.
- **Without a key:** sticky/round-robin across partitions → no ordering guarantee.

```
Ordering is PER PARTITION, not per topic.
24 partitions → max 24 parallel consumers in a group.
Key = user_id  → per-user ordering ✅, but a whale user can hot-spot one partition ❌
Key = null     → even spread ✅, no ordering ❌
Key = (shard_salt, user_id) → spread + per-user order, but breaks global per-user order
```

**Adding partitions** changes the key→partition map and *breaks* per-key ordering for pre-existing keys. Prefer over-provisioning up front.

### 4.3 Cassandra: token ring + vnodes

```sql
CREATE KEYSPACE app WITH replication = {
  'class': 'NetworkTopologyStrategy', 'dc1': 3
};

-- Partition key choice is permanent-ish; choose carefully.
CREATE TABLE events (
    tenant_id  uuid,
    event_date date,
    event_id   timeuuid,
    payload    text,
    PRIMARY KEY ((tenant_id, event_date), event_id)   -- composite PARTITION key
) WITH CLUSTERING ORDER BY (event_id ASC);
```

Cassandra's partition key is `(tenant_id, event_date)`: bucketing by day prevents unbounded partition growth (Cassandra rows should stay under ~100 MB / 100k rows). This "bucketing" pattern is essential for time-series.

```bash
nodetool ring               # show token ranges per node
nodetool status             # ownership % per node (watch for imbalance)
nodetool cleanup            # after topology change
nodetool repair             # anti-entropy (see ch. 5)
```

### 4.4 DynamoDB: partition key (+ optional sort key)

```
PK (partition key)     → hashed to a physical partition
SK (sort key)          → ordered within the partition; enables range queries within a PK

Table: PK=tenant_id, SK=order_date#order_id

+ Efficient: Query PK=tenant, SK BEGINS_WITH "2026-02"
− Hot: a single tenant with 10k writes/s exhausts one partition (1000 WCU / 3000 RCU each)
```

Fixes for a whale tenant: **write sharding** — add a random suffix to the PK and fan writes across shards, then scatter-gather reads.

```
PK = tenant_id#shard_0 … tenant_id#shard_N    (N ≈ expected_peak / partition_limit)
Write: pick a shard at random (or by hash of a second attribute)
Read all:  Query each shard and merge   (Parallel Scan / N queries)
```

### 4.5 Snowflake: micro-partitions + clustering keys

```sql
-- Natural micro-partitioning happens automatically (insert order).
-- Clustering keys re-organize for pruning on large tables:
ALTER TABLE events CLUSTER BY (DATE_TRUNC('day', occurred_at), tenant_id);
-- Check pruning quality:
SELECT * FROM TABLE(information_schema.query_history())
WHERE query_text ILIKE '%events%';
-- Look at PARTITIONS_SCANNED vs PARTITIONS_TOTAL in query profile.
```

Snowflake's rule of thumb: cluster on the columns in your most common `WHERE`/`GROUP BY`, and only on large tables where pruning matters (> ~1 TB). Clustering has automatic maintenance cost.

### 4.6 BigQuery: partition + cluster (both exist, use both)

```sql
CREATE TABLE analytics.events (
  occurred_at TIMESTAMP,
  tenant_id   INT64,
  payload     JSON
)
PARTITION BY DATE(occurred_at)          -- pruning on time filter
CLUSTER BY tenant_id;                   -- sorting within partitions (cheap pruning on tenant)

-- Query that prunes:
SELECT COUNT(*) FROM analytics.events
WHERE occurred_at >= '2026-02-01' AND occurred_at < '2026-03-01'
  AND tenant_id = 42;
```

### 4.7 Spark: partitioning for compute

```python
# Write partitioned files (on-disk partitioning → pruning for consumers):
df.write.mode("overwrite") \
  .partitionBy("event_date", "region") \
  .parquet("s3://lake/events/")

# Control shuffle partitions (the #1 tuning knob):
spark.conf.set("spark.sql.shuffle.partitions", "800")   # ~2-3x number of cores
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# Fix a known hot key with salting:
from pyspark.sql import functions as F
SALT = 16
salted = df.withColumn("salt", (F.rand() * SALT).cast("int"))
# join on (key, salt) against a salted copy of the other side, then aggregate away salt
```

### 4.8 Iceberg / Delta: partition evolution

```sql
-- Iceberg: change partitioning without rewriting old data
ALTER TABLE events SET PARTITION SPEC (day(occurred_at), bucket(16, tenant_id));
-- Old files keep the old spec; metadata reconciles both. New writes use the new spec.

-- Hidden partitioning: users query on occurred_at; Iceberg derives the partition.
SELECT * FROM events WHERE occurred_at >= '2026-02-01';  -- prunes via transform
```

### 4.9 Sharding a monolithic MySQL/Postgres (the migration path)

```
Phase 0: logical partitioning in one DB (validate key choice, measure skew)
Phase 1: stand up N shards; double-write from app (bounded scope)
Phase 2: backfill historical data in background (chunked, throttled)
Phase 3: read from shards, verify parity, keep leader as fallback
Phase 4: flip writes to shards; keep leader read-only for rollback window
Phase 5: decommission leader; add a router (Vitess/ProxySQL/Citus)
```

The non-negotiable parts: **idempotent double-writes**, **a rollback path at every phase**, and **online verification of row counts/checksums per shard**.

---

## 5. Where to Implement It (Systems and When Each Fits)

| System | Default partitioning | Tunable? | Pick it when |
|---|---|---|---|
| **PostgreSQL** | Declarative RANGE/LIST/HASH within a node; external sharding via Citus | Yes (per table) | Relational, single-node-to-moderate scale, CDC-friendly |
| **MySQL / Vitess** | Sharded by Vitess keyspace | Yes | Large OLTP, YouTube-scale |
| **Kafka** | Fixed partitions per topic | Partition count (choose up front) | Streams; ordering per key; parallelism |
| **Cassandra** | Consistent hashing token ring + vnodes | Partition key (fixed at table creation) | Huge write throughput, multi-DC AP |
| **DynamoDB** | Hash(PK) → partition; SK range within | PK + SK design | Serverless KV, single-digit-ms scale |
| **MongoDB** | Sharded cluster by shard key | Shard key (can change, expensively) | Document apps needing horizontal scale |
| **HBase / Bigtable** | Row-range regions, auto-split | Row key design | Wide-column, time-series, HBase-on-HDFS |
| **Spanner / Cockroach / Yugabyte** | Auto-split key ranges, ABSL/"range" shards | Primary key design | Globally consistent SQL |
| **Snowflake** | Automatic micro-partitions + clustering keys | `CLUSTER BY` | Analytical, wants pruning without manual layout |
| **BigQuery** | `PARTITION BY` + `CLUSTER BY` | Yes | Serverless analytics; prune to control cost |
| **Redshift** | Distribution styles (KEY/EVEN/ALL) + sort keys | Yes | Provisioned DW; co-locate joins |
| **Iceberg / Delta / Hudi** | Table partition spec + hidden partitioning | Yes, with evolution | Lakehouse; change layout over time |
| **Spark** | RDD/DataFrame partitions | `repartition`, `partitionBy`, shuffle count | Compute parallelism and skew control |
| **Elasticsearch** | Shards per index (fixed) | Shard count + routing | Search; routing key = shard |

**Choosing guide:**
- **OLTP, need relational + CDC** → Postgres (partition now, shard later via Citus) or MySQL/Vitess.
- **Massive write throughput, multi-region AP** → Cassandra or DynamoDB.
- **Time-series with range scans** → range/time partitioning (Timescale, BigQuery, Postgres range partitions, HBase row key `salt + timestamp`).
- **Analytics with cost control** → partition + cluster (BigQuery/Snowflake) or Iceberg hidden partitioning.
- **Streams** → Kafka partitions (over-provision; key for ordering).
- **Globally consistent SQL** → Spanner/Cockroach/Yugabyte (and choose a non-monotonic PK).

---

## 6. Patterns and Anti-Patterns

### Patterns

- **Composite/bucketed key for time-series:** `(entity_id, time_bucket)` instead of raw timestamp — spreads writes while keeping per-entity locality and bounding partition size.
- **Write sharding for whales:** random-suffix PKs for hot tenants; scatter-gather on read.
- **Co-location for joins:** shard related tables on the same key (Citus `create_distributed_table` with the same key) so joins are local.
- **Salting for skew:** add a random salt prefix/suffix to redistribute a hot key during aggregation, then drop it after the first aggregation stage.
- **Two-level partitioning:** coarse (day) + fine (hash of id) to balance pruning and write spread.
- **Consistent hashing with vnodes for stateful nodes** (caches, Cassandra) so rebalance moves little data.
- **Read-replica routing to keep hot analytical reads off shards.**
- **Redis Cluster hash tags `{...}`:** `{user:42}:name` and `{user:42}:cart` land on the same slot → multi-key ops allowed.
- **Compaction/small-file management:** regularly compact partitions so file count stays sane (Iceberg `rewrite_data_files`, Delta `OPTIMIZE`).

### Anti-Patterns

- **Monotonic shard key.** Auto-increment ID or raw timestamp as the *only* partition dimension → all writes hit the newest/last shard. Fix: hash or bucket it.
- **Low-cardinality shard key.** `partition by country` on data that is 80% one country → one giant partition.
- **`hash % N` in application code.** Every reshard remaps almost everything. Use consistent hashing or fixed slots.
- **Over-partitioning.** Millions of tiny partitions/files → metadata overhead, slow planning, small-file hell. Symptom: "query planning takes longer than the query".
- **Under-partitioning** so you can never add parallelism without rewriting.
- **Sharding then doing cross-shard joins in a hot loop.** Scatter-gather per request = N× latency and N× failure surface.
- **Choosing a shard key that doesn't match access patterns.** E.g., shard by `tenant` but every query filters by `user_id` → all queries scatter-gather.
- **Global secondary indexes with no thought to write amplification.** Every write fans out to the global index.
- **No skew monitoring.** You learn a partition is hot from a p99 latency graph during a launch.
- **Changing Kafka partition count on a live topic with keyed data.** Silently breaks per-key ordering.
- **Treating a GSI/secondary index as strongly consistent.** In DynamoDB it is not; in Cassandra global indexes usually aren't either.

---

## 7. Real-World Use Cases and Industry Applications

- **Discord — Cassandra → ScyllaDB for messages.** Billions of messages, partitioned by channel/date buckets; they publicly documented the migration and the "time-series bucketing" needed to avoid unbounded partitions.
- **YouTube — Vitess/MySQL sharding.** The canonical sharded OLTP: a routing tier (VTgate) + sharded MySQL, with shard keys chosen per table and explicit resharding workflows.
- **Instagram — Postgres → sharded Postgres.** Logical shards mapped onto physical machines; the mapping layer lets them move logical shards between physical hosts without changing application code — a clean example of fixed-slot partitioning.
- **Uber — Schemaless / Docstore over MySQL.** Sharded by UUID (hashed), avoiding monotonic hot spots.
- **Facebook — TAO and sharded MySQL.** Shard by object ID; reads fan out across replicas; a routing layer hides the topology.
- **Amazon — Dynamo/DynamoDB.** Consistent hashing (Dynamo) → managed hash partitioning (DynamoDB); adaptive capacity shifts throughput to hot partitions automatically (a managed response to the hot-partition problem).
- **Snowflake / BigQuery — automatic micro-partitioning.** Turns "should I shard?" into "how do I cluster/prune?" — a deliberate abstraction that shifts the DE's job from plumbing to modeling.
- **Netflix — Kafka partition counts and eviction flows.** Over-provisioned partitions so consumer groups can scale, with keyed events for per-entity ordering.
- **Spark at any large company — skew.** The "one giant partition" story (`GROUP BY country` where 60% is one country) is why AQE skew join exists.

---

## 8. Senior-Level Deep Dive

### 8.1 Choosing the shard key: a decision procedure

```
1. Enumerate the TOP 3 access patterns (query shapes + frequency).
2. For each candidate key, ask:
   a. Cardinality — enough distinct values? (>= 10x partition count is healthy)
   b. Distribution — any single value > 5–10% of rows/writes?
   c. Locality — does it co-locate data you join/scan together?
   d. Monotonicity — does it grow over time? (if yes, bucket it)
   e. Immutability — can it change? (if yes, avoid: updates become moves)
3. Stress test: simulate peak write rate; compute per-partition load.
   If any partition > ~(2× average), redesign (composite, salt, or bucket).
4. Stress test reads: how many shards does a p95 query touch? If > 2–3, reconsider.
5. Plan the rebalancing story: fixed slots or dynamic split? What moves on growth?
```

### 8.2 Hot partition math (what "hot" actually costs)

```
DynamoDB example:
  A partition supports ~1,000 WCU and ~3,000 RCU, and ~10 GB.
  A whale tenant does 6,000 writes/s → 6x over a single partition's limit.
  Even though the table has adaptive capacity, a single PK cannot exceed
  the throughput of one partition. Fix: write-shard the PK into k=8 shards.
  Then each shard sees 750 writes/s → under the 1,000 limit. ✅

Kafka example:
  24 partitions, 3 consumers per partition budget... but key = user_id and
  one user = 40% of traffic → one partition gets 40% of the load; the other 23
  sit idle. Adding consumers doesn't help: a partition is consumed by ONE
  consumer in a group. Fix: change the key, or split the whale to a dedicated
  topic, or salt the key (losing global per-user order).

Cassandra example:
  Partition key = user_id for a "user events" table; a bot writes 1M events for
  one user. Partition grows past 100 MB → GC pressure, timeouts, hinted
  handoff storms. Fix: bucket key = (user_id, day_hour).

Spark example:
  df.groupBy("country").count() with 60% "US" → one task processes 60% of the data;
  job time = that task's time regardless of cluster size. Fix: salt + two-stage agg.
```

### 8.3 Cross-shard operations: the taxonomy and the escape hatches

| Operation | Cost without co-location | Escape hatch |
|---|---|---|
| Point read/write by shard key | Local, fast | — |
| Range read | Local if range key (hash → scatter-gather) | Range partition scheme |
| Join | Scatter-gather or shuffle | Co-locate on join key; broadcast small dims |
| Aggregation | Map-partial + merge (scatter-gather) | Pre-aggregate per shard |
| Uniqueness constraint | Needs global coordination | Avoid; use ID allocation or per-shard uniqueness |
| Transaction across shards | 2PC (blocking, slow) or saga (compensating) | Co-locate; or accept saga semantics |
| Sorting / top-N | Scatter + k-way merge | Push local top-N, merge at coordinator |

**2PC vs Saga:**

```
2PC (two-phase commit):    prepare-all → commit-all
  + atomic across shards
  − coordinator is a SPOF; locks held across the network; blocks on failure
  Used by: Spanner (Paxos-committed), CockroachDB, some XA setups

Saga:                      T1 → T2 → T3 ...  with compensating C1, C2, C3 on failure
  + no distributed locks; long-lived; service-friendly
  − eventual consistency; you must write compensations; intermediate states visible
  Used by: microservices, order/payment flows
```

### 8.4 Rebalancing without downtime (the senior version)

```
Goal: move shard S from node A to node C with zero downtime and no data loss.

1. Mark S as "migrating": routing still points to A (authoritative).
2. Copy a snapshot of S to C.
3. Stream subsequent writes: dual-write to A and C (or ship A's log to C).
4. When C is caught up (lag → 0), briefly pause writes to S (or use a consensus
   leader handoff), flip the routing map to C, and unpause.
5. Keep A as a read-only shadow for a rollback window, then drop it.

Guarantees to preserve:
  - Exactly one authoritative owner at any instant (avoid split-brain).
  - Writes are not lost in the handoff (fence A after the flip).
```

If you cannot tolerate even a millisecond pause, use a consensus-backed ownership flip (Raft group per shard, as Spanner/Cockroach do) rather than an application-level flip.

### 8.5 Partition pruning and physical layout (the OLAP payoff)

```
Rule: the partition/cluster columns must appear as CONSTANTS or simple ranges
in the query predicates. Wrapping them in functions defeats pruning.

  WHERE date(occurred_at) = '2026-02-01'   ❌ function on the column
  WHERE occurred_at >= '2026-02-01'
    AND occurred_at <  '2026-02-02'        ✅ range on the column

BigQuery/Snowflake: check PARTITIONS_SCANNED / PARTITIONS_TOTAL (Snowflake)
or bytes-billed (BigQuery) to prove pruning works. If pruning fails, you pay
for a full scan — the #1 cause of runaway cloud analytics bills.
```

### 8.6 Small files: the lakehouse partitioning trap

```
Over-partitioned lake:
  PARTITION BY (date, hour, region, tenant, event_type)
  => millions of tiny Parquet files
  => metadata/listing dominates; planners choke; object store costs spike.

Heuristics:
  - Target 100–1000 MB per file after compaction (not per insert).
  - Partition on low-cardinality, commonly-filtered columns (date, region).
  - Cluster/sort within partitions for the second-most-common filter.
  - Compact regularly (Iceberg rewrite_data_files, Delta OPTIMIZE, Hudi clustering).
```

### 8.7 Debugging partitioning problems: a runbook

1. **Measure skew per partition.** `nodetool status`/`nodetool tablehistograms` (Cassandra), CloudWatch `ConsumedWriteCapacity` per partition (DynamoDB), Kafka per-partition throughput/lag, Spark task duration distribution.
2. **Identify the hot key.** Rank keys by requests/bytes. Look for a single dominant value.
3. **Classify the cause:** cardinality (too few values), distribution (whale), monotonicity (newest shard), or access pattern (one key queried constantly).
4. **Pick the smallest fix:** bucket/salt the key (write path) or add a cache/read replica (read path) before considering resharding.
5. **Watch the tail:** p99 per-partition latency, not just averages; a single hot partition hides in averages.
6. **Verify pruning:** confirm queries touch only intended partitions (EXPLAIN, query profile).

### 8.8 Partitioning vs replication: putting them together

```
Full distributed layout:
                     ┌─────────────┐
   key ──▶ route ──▶ │ Partition 7 │
                     │  leader  ●──┼──▶ replica (AZ-b)
                     │          ●──┼──▶ replica (AZ-c)
                     └─────────────┘
   Partitioning decides WHICH partition owns the key (this chapter).
   Replication decides HOW MANY copies and WHO is leader (ch. 1 & 6).

Typical configurations:
  - 3 replicas × 100 partitions = 300 copies; 3-node cluster of 100 partitions each.
  - Cassandra: RF=3, 256 vnodes/node, N=3 nodes → each key on 3 nodes.
  - Kafka: RF=3, 24 partitions → each partition leader+2 followers.
```

---

## 9. Common Mistakes and How to Avoid Them

1. **Picking a shard key from the schema instead of the access pattern.** → Derive the key from your top queries, not from the PK column by default.
2. **Monotonic keys (timestamp/auto-increment) as the sole partitioner.** → Bucket or hash them.
3. **`hash % N` in code.** → Consistent hashing, rendezvous, or fixed slots.
4. **Under- or over-partitioning.** → Fixed count ≫ nodes for streams; 100–1000 MB files for lakes; prune before you add partitions.
5. **Ignoring the whale.** → Capacity-plan for the largest tenant, not the average.
6. **Cross-shard joins in the request path.** → Co-locate or denormalize; push analytics to a replica/warehouse.
7. **Breaking ordering by adding Kafka partitions.** → Over-provision; if you must add, accept the re-keying and document it.
8. **Assuming GSIs/secondary indexes are strongly consistent.** → Treat them as eventually consistent; verify with `ConsistentRead` where available.
9. **Rebalancing with a maintenance window.** → Online migration with dual-write + routing flip + fencing.
10. **No partition-level monitoring.** → Per-partition/per-task metrics everywhere; alert on imbalance, not just totals.
11. **Partitioning by a mutable attribute.** → Updates become cross-shard moves; pick immutable or slowly-changing keys.
12. **Rewriting a whole table to change partitioning.** → Use systems with partition evolution (Iceberg/Delta) or plan the change as a migration.
13. **Forgetting small-file compaction.** → Schedule compaction; alert on file count growth.

---

## 10. Exercises / Interview Questions

> Hints only — no full answers inline.

1. **Key selection.** An orders table is queried (a) by `order_id` (90%), (b) by `customer_id` (8%), (c) by `date` range (2%). Pick a shard key and justify it. *(Hint: optimize the dominant pattern; consider a composite key and where the 8% goes.)*
2. **Hot key.** A messaging app has one celebrity followed by 100M users; every post fans out to followers. Where is the hot partition, and how do you fix it? *(Hint: fan-out-on-write vs fan-out-on-read; salting; caching; dedicated partitions.)*
3. **Consistent hashing.** With 8 physical nodes and no vnodes, why is load uneven, and how do vnodes fix it? How many vnodes would you use? *(Hint: arc-length randomness; 1/sqrt(vnodes) variance reduction.)*
4. **Modulo problem.** Prove that `hash % N` remaps most keys when N changes from 10 to 11. *(Hint: for how many h is `h % 10 == h % 11`? It's a small fraction.)*
5. **Secondary indexes.** Compare a document-partitioned and term-partitioned index for a workload that is 95% reads by tag. Which wins, and what's the write cost? *(Hint: read fan-out vs write fan-out.)*
6. **Rebalancing.** Design a zero-downtime migration of shard S from node A to node C. What invariant must hold at all times, and how do you fence A? *(Hint: single authoritative owner; fencing tokens.)*
7. **Spark skew.** `df.groupBy("merchant").count()` takes 4 hours; one task dominates. Give two fixes and their trade-offs. *(Hint: salting + two-stage aggregation; AQE skew join; partial pre-aggregation.)*
8. **DynamoDB.** A tenant does 5,000 writes/s on one PK. Why doesn't adaptive capacity save you, and what do you do? *(Hint: per-partition throughput ceiling; write sharding with a random suffix.)*
9. **Pruning failure.** A BigQuery query filters `WHERE DATE(ts) = '2026-02-01'` but scans the whole table. Why? *(Hint: function on the partition column; compare to a range predicate.)*
10. **Cross-shard transaction.** You must atomically debit one shard and credit another. Compare 2PC and saga for latency, failure behavior, and complexity. *(Hint: blocking vs compensating; coordinator failure.)*
11. **Kafka ordering.** You key by `user_id` for ordering, but one user generates 30% of traffic. Enumerate three mitigations and what each costs. *(Hint: salt (loses order), dedicated topic, change key granularity.)*
12. **Partition evolution.** How do Iceberg/Delta let you change partitioning without rewriting old files, and what does that imply for query planning? *(Hint: per-file partition specs in metadata; hidden partitioning.)*

---

## References

- Kleppmann, M. — *Designing Data-Intensive Applications*, Chapter 6: "Partitioning". https://dataintensive.net/
- DeCandia et al. — "Dynamo: Amazon's Highly Available Key-value Store" (consistent hashing, vnodes, sloppy quorum): https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf
- Karger et al. — "Consistent Hashing and Random Trees" (STOC 1997): https://dl.acm.org/doi/10.1145/258533.258660
- PostgreSQL Documentation — Table Partitioning: https://www.postgresql.org/docs/current/ddl-partitioning.html
- Citus Documentation — Distributed tables and co-location: https://docs.citusdata.com/
- Apache Kafka Documentation — Topics and partitions: https://kafka.apache.org/documentation/#intro_concepts_and_terms
- Apache Cassandra — Data modeling / partition keys: https://cassandra.apache.org/doc/latest/cassandra/developing/data-modeling/index.html
- DataStax — Cassandra data modeling best practices (bucketing): https://www.datastax.com/blog/basic-rules-of-cassandra-data-modeling
- Amazon DynamoDB — Partition key design & best practices: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/bp-partition-key-design.html
- Amazon DynamoDB — Global secondary indexes: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GSI.html
- Snowflake Documentation — Micro-partitions and clustering: https://docs.snowflake.com/en/user-guide/tables-clustering-micropartitions
- Google Cloud BigQuery — Partitioned tables and clustering: https://cloud.google.com/bigquery/docs/partitioned-tables
- Amazon Redshift — Distribution styles and sort keys: https://docs.aws.amazon.com/redshift/latest/dg/c_choosing_dist_sort.html
- Apache Iceberg — Partitioning and partition evolution: https://iceberg.apache.org/docs/latest/partitioning/
- Delta Lake — Concurrency control and partitioning: https://docs.delta.io/concurrency-control/
- Vitess Documentation — Sharding: https://vitess.io/docs/
- MongoDB Documentation — Sharding: https://www.mongodb.com/docs/manual/sharding/
- Jepsen — Analyses (real-world partitioning/consistency findings): https://jepsen.io/analyses
