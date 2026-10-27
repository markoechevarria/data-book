# Replication

> Replication is the practice of keeping multiple copies of the same data on different machines so the system survives machine failure, serves reads from many places, and keeps working while you upgrade hardware. It sounds simple — "just copy the data" — and it is the single largest source of subtle bugs in data engineering: two copies drift apart, a reader sees yesterday's value, a failover promotes a replica that was 4 seconds behind, and your overnight revenue report double-counts. This chapter explains *why* we replicate, *how* the copies are kept in sync internally, the precise cost of every flavor of "in sync", and how to reason about replication lag, quorum math, and multi-leader topologies well enough to design and debug real pipelines.

## Why a Data Engineer Cares

You already know Spark, Kafka, Snowflake. You may think replication is "the database team's problem". It is not:

- **Kafka is a replication system first.** A topic is a partitioned, replicated log. `replication.factor`, `min.insync.replicas`, `acks`, and `unclean.leader.election.enable` are replication parameters, and they decide whether a broker failure silently drops events or blocks producers.
- **Every distributed warehouse replicates underneath you.** Snowflake micro-partitions live in object storage replicated across zones; BigQuery's Colossus storage is replicated; Redshift, Synapse, and Databricks all keep multiple copies of data and metadata. Their "consistency" and "staleness" behavior is replication behavior.
- **Your pipelines read replicas.** You routinely hit Postgres read replicas, MongoDB secondaries, or a Kafka follower for analytical loads. If you do not understand replication lag, your CDC (change data capture) job will emit rows out of order or miss the tail of a transaction.
- **Failure semantics are your SLAs.** RPO (how much data you may lose) and RTO (how long you may be down) are replication design decisions. A DE who cannot argue "we need synchronous commit here and async there" cannot defend a data contract.

Replication is where the theory (CAP, consistency models, consensus) meets the plumbing you configure every week.

## TL;DR

- **Replication = keeping N copies of data on N nodes.** Its three jobs: **durability** (survive failures), **availability** (keep serving), **read scaling / latency** (serve reads near users).
- **Two main families:** single-leader (leader/follower, "primary/replica") and multi-leader / leaderless (Dynamo-style quorums). Single-leader is the default mental model; multi-leader appears for geo-active-active and offline sync.
- **Synchronous replication** waits for replicas before acknowledging the write → no data loss on failover, but latency = slowest replica, and availability drops if a replica is down. **Asynchronous** acknowledges after the leader writes → fast and available, but can lose data on failover and exposes readers to **replication lag**.
- **Semi-synchronous** (`ANY 1`, `w:majority`, `QUORUM`) is the practical middle: wait for *k of N*, not all. This is a **quorum** and it is the workhorse of modern systems.
- **Quorum rule of thumb:** with N replicas, write to W, read from R. If **W + R > N**, every read quorum intersects every write quorum → reads see the latest acknowledged write (for a single value, in a single-key sense).
- **Replication lag** is the killer. It breaks **read-your-writes**, **monotonic reads**, and **consistent prefix** unless you engineer around it (sticky routing, LSN/version checks, causal tokens).
- **Multi-leader** and **leaderless** let multiple nodes accept writes but push the hard problem to **conflict resolution** (see `05-eventual-consistency.md`).
- **Replication topologies matter**: chain/cascade, star, all-to-all. All-to-all + naive LWW = write loss over long paths.
- **Design checklist:** decide RPO/RTO first, then pick sync vs async per operation, then choose topology, then instrument lag and failover.

---

## 1. Definitions and Core Vocabulary

| Term | Meaning |
|------|---------|
| **Replica** | A copy of the data on another node. Also called follower, secondary, standby, mirror, or read replica. |
| **Leader / Primary** | The node that accepts writes for a given datum (in single-leader replication). |
| **Follower / Replica / Standby** | Node that applies the leader's changes and helps serve reads. |
| **Replication log** | The ordered stream of changes shipped from leader to followers (WAL, binlog, oplog, Raft log, Kafka partition). |
| **LSN / Offset / GTID** | A monotonically increasing position in the replication log. Postgres calls it LSN, Kafka calls it offset, MySQL calls it GTID/binlog position, MongoDB calls it oplog timestamp. |
| **Replication lag** | Difference in time (or bytes/positions) between the leader's log head and a follower's applied position. |
| **Synchronous replication** | Write is acknowledged only after it is durably applied on the required replicas. |
| **Asynchronous replication** | Write is acknowledged after the leader has it; replicas catch up later. |
| **Semi-synchronous** | Acknowledge after a quorum (subset) of replicas applies it. |
| **Quorum** | A subset of replicas, usually a strict majority (⌊N/2⌋+1), whose agreement is required for progress. |
| **Read quorum (R), Write quorum (W), Replication factor (N)** | Tunable counts in leaderless systems. `W + R > N` gives read-your-writes on a single key. |
| **RPO** | Recovery Point Objective: acceptable amount of data loss, measured in time. Drives sync vs async choices. |
| **RTO** | Recovery Time Objective: acceptable downtime during failover. Drives topology and automation. |
| **Failover** | Promoting a follower to leader after the leader dies. |
| **Split-brain** | Two nodes both believe they are leader (see `06-leader-follower-replication.md`). |
| **Leaderless / quorum replication** | Any node may accept a write; consistency emerges from overlapping quorums (Dynamo, Cassandra, Riak). |
| **Multi-leader** | More than one node accepts writes; conflicts must be resolved (CouchDB, Galera, BDR, some geo-active-active setups). |
| **Hinted handoff** | A neighbor temporarily stores a write destined for an unreachable node and forwards it later (Dynamo/Cassandra). |
| **Anti-entropy** | Background reconciliation (e.g., Merkle-tree comparison) to repair divergence. |
| **Read repair** | On a read, the coordinator detects divergent replicas and fixes them on the spot. |
| **Replication slot** | Postgres mechanism that holds the WAL so a slow consumer can't lose data. |
| **Change data capture (CDC)** | Reading the replication log to emit row-level changes to downstream systems (Debezium, etc.). |

> **Senior note:** "Replica" is overloaded. A *read replica* scales reads but is not a failover target unless promoted; a *standby* (hot/warm) is a failover target; a *logical replica* replicates at the row level and may have a different schema. Confusing these is a classic production incident.

---

## 2. What Problem It Solves / Why It Matters

Replication exists because of four hard truths:

1. **Machines fail.** Disks die, racks lose power, whole AZs go dark. One copy = one failure away from permanent loss.
2. **Single-node throughput is finite.** A 16-vCPU database can serve maybe a few thousand read queries/sec. Replication lets you fan reads out to N replicas → near-linear read scaling.
3. **Distance costs latency.** A user in Tokyo reading from `us-east-1` pays ~150 ms RTT per request. A local replica cuts that by 10–20x.
4. **Maintenance requires spare capacity.** Rolling upgrades, re-indexing, and physical migrations need somewhere to fail over to.

Mapping to business outcomes:

| Business requirement | Replication mechanism |
|---|---|
| "We cannot lose a committed payment" | Synchronous / majority replication (RPO = 0) |
| "The site must survive an AZ outage" | Multi-AZ synchronous replica + automated failover |
| "Our dashboard must not slow down checkout" | Async read replicas / follower reads |
| "Users in 3 continents need < 50ms reads" | Geo-replicas, region-local read quorums |
| "We need near-real-time CDC into the lake" | Read the replication log (WAL/binlog) into Kafka |

**The cost** is the Eternal Triangle of replication: **consistency, availability, latency**. You cannot maximize all three. Synchronous replication buys consistency at the cost of latency and availability; asynchronous buys the reverse. Every design decision in this chapter is trading among these three.

---

## 3. How It Works Internally (Mechanisms, Internals)

### 3.1 The generic shape

```
                 writes                          applies log
   clients ───────────────▶  ┌─────────┐  replication  ┌──────────┐
                             │ LEADER  │─── log ──────▶│ FOLLOWER │◀── reads (maybe)
                             │ (R/W)   │──────────────▶└──────────┘
                             └─────────┘     ┌──────────┐
                                  │  ────────▶│ FOLLOWER │◀── reads (maybe)
                                  ▼          └──────────┘
                             durable log
```

Every replication system is: **a leader that serializes writes into an ordered log, and followers that replay that log.** The differences are *what* is in the log and *how* the leader knows the follower is caught up.

### 3.2 What travels over the wire: five replication log types

| Method | What it ships | Examples | Notes |
|---|---|---|---|
| **Statement-based** | The SQL statement (`UPDATE ... WHERE ...`) | Old MySQL binlog (SBR) | Non-deterministic funcs (`NOW()`, `RAND()`, `LAST_INSERT_ID()`) break it. |
| **Row-based / logical** | Before/after images of rows | MySQL RBR, Postgres logical decoding, MongoDB oplog | Verbose, deterministic, decodable by external tools → the foundation of CDC. |
| **Write-ahead log (WAL) shipping / physical** | Raw storage-engine byte changes | Postgres streaming replication, SQL Server log shipping | Byte-identical replica; version-specific; fast; not decodable without the engine. |
| **Trigger-based** | Application-level trigger writes to a shadow table | Legacy Oracle/Postgres triggers, some CDC | Flexible but slow and fragile. |
| **Consensus log / Raft-Paxos** | Proposals appended to a replicated log | etcd, Consul, Kafka KRaft, TiKV, CockroachDB, Spanner | The log itself is the replication; leader election is part of the same protocol. |

> **Senior note:** Physical WAL shipping and logical replication give the same end state but different guarantees. Physical gives you an exact block-for-block copy (great for HA, useless for a partial-table CDC). Logical gives you row events you can transform (great for CDC, no guarantee of physical identity). Choose based on *what the consumer needs*, not on which is "faster".

### 3.3 Bringing a new replica online (snapshot + catch-up)

Naively copying a live database is racy. The universal recipe:

1. **Take a consistent snapshot** of the leader at log position P (e.g., `pg_basebackup` records the LSN; MySQL uses a consistent snapshot + binlog coordinates; a storage-level snapshot is only safe if it captures the log position atomically).
2. **Ship the snapshot** to the new node.
3. **Replay the log from position P** onward, continuously.
4. **When the replay lag → ~0, the replica is safe to serve reads / become a failover target.**

```
snapshot@LSN=P ──ship──▶ new node
leader keeps writing:  P ──────────────────▶ P+Δ
new node replays:      P ──────────────────▶ P+Δ   (chasing)
when lag≈0: replica is "caught up"
```

> **Senior note:** Most "replica is broken" incidents come from step 4 never being reached because the leader *deleted* the log segments the replica needed (log truncation / `wal_keep_size` / Kafka log retention). **Replication slots** and **retention windows** exist to prevent exactly this — and they create their own failure mode: an absent consumer holds the log forever and fills the disk. Monitor both sides.

### 3.4 Synchronous vs asynchronous vs semi-synchronous — the actual mechanics

```
sync:      client ──write──▶ leader ──ship──▶ follower ──ack──▶ leader ──ack──▶ client
                                                  (client waits for the slowest required replica)

async:     client ──write──▶ leader ──ack──▶ client
                                    └──ship──▶ follower (later; client never waits)

semi-sync: client ──write──▶ leader ──ship──▶ {follower1, follower2, follower3}
                                    ack when ANY 1 (or majority) applies ──▶ client
```

Concrete consequences:

| Mode | Write latency | Data loss on failover | Availability if a replica is down | Sync commit cost |
|---|---|---|---|---|
| Async | Lowest | Possible (`RPO > 0`) | Unaffected | None |
| Sync (all) | = slowest replica | None (`RPO = 0`) | **Drops** — one slow replica stalls writes | High |
| Semi-sync (`ANY 1` / majority) | = k-th fastest replica | None if quorum survives | Tolerates minority failure | Medium |

Postgres example semantics:
- `synchronous_commit = off` → async, can lose ~`wal_writer_delay` worth of transactions (usually ~200 ms) on crash.
- `synchronous_commit = on` + `synchronous_standby_names = 'ANY 1 (s1, s2, s3)'` → semi-sync: the leader waits for *one* of the three to flush.
- `synchronous_commit = remote_apply` → waits until the standby has *applied* (visible to reads), not just flushed.

### 3.5 Replication lag: where it comes from and what it breaks

Lag has several distinct sources, and they have different fixes:

| Source | Description | Mitigation |
|---|---|---|
| **Network delay** | Ship time between nodes | Co-locate nodes; compression; dedicated links |
| **Follower CPU/IO saturation** | Follower applies log slower than leader produces | More replicas; better hardware; parallel apply |
| **Long transactions** | One huge transaction blocks the apply position | Batch/chunk big writes |
| **Single-threaded apply** | Follower replays serially | Parallel replay (MySQL MTS, Postgres `max_parallel_workers` for logical) |
| **Backpressure from reads** | Heavy analytical reads compete with apply | Separate read replicas from failover standbys |
| **Deliberate lag** | Follower intentionally behind for "oops" recovery | None needed; it's a feature |

Lag breaks three guarantees that look obvious to users:

- **Read-your-writes:** you POST a record, then GET it from a replica and it's missing.
- **Monotonic reads:** a user's first read hits a fresh replica, the second hits a stale one, and time appears to go backwards.
- **Consistent prefix:** a replica shows the *child* row of a transaction but not the *parent*.

Engineering fixes (details in `04-consistency-models.md`): **sticky routing** (route a user's reads to the same replica), **read-from-leader-after-write** for a bounded window, **version/LSN tokens** sent to the client and checked before serving, and **causal tokens**.

### 3.6 Quorums: the math that makes leaderless systems work

With `N` replicas, write to `W`, read from `R`:

```
Strong read-your-write on a single key requires:      W + R > N

Example, N=3:
  W=2, R=2  → 2+2=4 > 3  ✅ overlap guaranteed
  W=1, R=1  → 1+1=2 ≤ 3  ❌ stale reads possible
  W=3, R=1  → 3+1=4 > 3  ✅ but writes fail if any node is down
```

Majority quorum is `W = R = ⌊N/2⌋ + 1`. With `N=5`: majority = 3. You tolerate 2 failures.

> **Senior note:** `W + R > N` alone does **not** give you linearizability. It guarantees the read set intersects the write set, so *some* node has the value — but a stale node might respond first and you may accept it. Dynamo-style systems use **version numbers/vector clocks + read repair** to pick the newest; without that, you can still return stale data. This is why "quorum = strong consistency" is a myth (see `03-cap-theorem.md`).

### 3.7 Sloppy quorums, hinted handoff, read repair, anti-entropy

- **Sloppy quorum:** accept writes on the first N healthy nodes *on the ring*, even if they aren't the "home" replicas. Keeps availability during partitions, weakens the quorum guarantee.
- **Hinted handoff:** the temporary node stores a "hint" (the write) for the unreachable home node and delivers it when the node returns.
- **Read repair:** when a coordinator reads from R replicas and sees differing versions, it returns the newest and pushes it to the stale replicas.
- **Anti-entropy:** periodic background comparison (Merkle trees in Dynamo/Cassandra) to catch divergence that neither reads nor hints touched.
- **Merkle tree:** a hash tree over key ranges; two nodes compare roots and descend only into differing subtrees → cheap divergence detection.

```
        Merkle root (hash)
        /              \
   hash(L)            hash(R)
   /     \            /     \
 h..    h..        h..     h..      ← compare level by level;
(differ)  (same)  (same)  (same)      only descend where hashes differ
```

### 3.8 Topologies

```
Chain / cascade          Star                        All-to-all (mesh)
 A ─▶ B ─▶ C             B ─▶ A ◀─ C                 A ◀─▶ B
                          ◀─────                      ▲   ╲ ╱   ▲
                                                       ╲  ╳  ╱
                                                        ╲╱ ╲╱
                                                        C ◀──▶ D

pro: fan-out, lower      pro: simple, one hop       pro: low latency, resilient
     leader load         con: leader is SPOF        con: msgs can overtake; ordering
con: deeper lag chain    con: fan-in bottleneck          hazards; conflict resolution
```

**Causal ordering hazard:** in a mesh, a message can reach C from B before an *earlier* message reaches C from A. Naive last-write-wins (LWW) then silently discards the earlier, "newer by real time" value. This is *exactly* why vector clocks and CRDTs exist (`05-eventual-consistency.md`).

---

## 4. How to Implement It (Step-by-Step, with Code / Config)

### 4.1 PostgreSQL physical streaming replication (HA standby)

**Primary (`postgresql.conf`):**
```conf
wal_level = replica                # enough info for physical replication
max_wal_senders = 10               # allow up to 10 replicas
wal_keep_size = 1GB                # keep WAL so a lagging replica can catch up
# Durability / sync policy:
synchronous_commit = on
synchronous_standby_names = 'ANY 1 (standby_a, standby_b)'   # semi-sync
```

**Primary (`pg_hba.conf`):**
```conf
# TYPE  DATABASE  USER       ADDRESS        METHOD
host    replication  repl     10.0.0.0/24    scram-sha-256
```

**On the standby (bootstrap + start):**
```bash
pg_basebackup -h primary.internal -D /var/lib/postgresql/data \
              -U repl -Fp -Xs -P -R          # -R writes standby.signal + primary_conninfo
# standby.signal presence → postgres starts in recovery (read-only) mode
```
```conf
# postgresql.conf on standby
hot_standby = on                   # allow read queries while in recovery
```

**Verify lag:**
```sql
-- On primary:
SELECT application_name, state, sync_state,
       pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn) AS replay_lag_bytes,
       write_lag, flush_lag, replay_lag
FROM pg_stat_replication;
```

### 4.2 PostgreSQL logical replication + CDC into Kafka (the DE path)

```sql
-- On primary: enable logical decoding for a publication
ALTER SYSTEM SET wal_level = logical;
CREATE PUBLICATION orders_pub FOR TABLE public.orders;
```
```sql
-- On the consumer/subscriber:
CREATE SUBSCRIPTION orders_sub
  CONNECTION 'host=primary dbname=app user=repl'
  PUBLICATION orders_pub;
```
For CDC into Kafka, Debezium reads the logical replication slot and emits row-level events:
```json
{
  "name": "orders-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "primary.internal",
    "database.dbname": "app",
    "plugin.name": "pgoutput",
    "slot.name": "debezium_orders",
    "publication.name": "orders_pub",
    "topic.prefix": "cdc",
    "snapshot.mode": "initial"
  }
}
```
> **Senior note:** a replication slot that no consumer drains will retain WAL forever and fill the primary's disk — a top cause of "the OLTP database died because of the data team". Set `max_slot_wal_keep_size` and alert on `pg_replication_slots` lag.

### 4.3 Kafka topic replication (the log *is* the database for a DE)

```bash
kafka-topics.sh --create --topic orders \
  --partitions 12 \
  --replication-factor 3 \
  --config min.insync.replicas=2
```

Producer side (`acks=all`) + broker settings determine durability:
```properties
# producer
acks = all                 # wait for all ISR (subject to min.insync.replicas)
enable.idempotence = true  # exactly-once per partition, dedup retries
max.in.flight.requests.per.connection = 5

# broker/topic
min.insync.replicas = 2
unclean.leader.election.enable = false   # never promote an out-of-sync replica
default.replication.factor = 3
```

Durability matrix:

| `acks` | `min.insync.replicas` | If a broker dies | Data loss? |
|---|---|---|---|
| `0` | any | fire-and-forget | Yes, easily |
| `1` | any | leader acked before replication | Yes if leader dies |
| `all` | 1 | ack from leader only (degenerate) | Yes if leader dies pre-replication |
| `all` | 2 (RF=3) | tolerates 1 broker down | No (with ULE disabled) |
| `all` | 3 (RF=3) | blocks if any broker down | No loss, but availability drops |

### 4.4 Cassandra / Dynamo-style quorum config

```sql
-- Keyspace with RF=3 in one DC
CREATE KEYSPACE app WITH replication = {
  'class': 'NetworkTopologyStrategy', 'dc1': 3, 'dc2': 3
};

-- Tunable consistency per query:
CONSISTENCY LOCAL_QUORUM;   -- majority within the local DC (multi-DC friendly)
INSERT INTO users (id, name) VALUES (1, 'ana');
CONSISTENCY QUORUM;         -- majority across all DCs (slower, cross-region)
SELECT * FROM users WHERE id = 1;
```

| Level | Meaning | Notes |
|---|---|---|
| `ONE` / `LOCAL_ONE` | 1 replica | Fast, stale reads possible |
| `QUORUM` | majority of all replicas | `W+R>N` if both quorum |
| `LOCAL_QUORUM` | majority within local DC | Default for multi-DC |
| `EACH_QUORUM` | quorum in *each* DC | Strong-ish across DCs; high latency |
| `ALL` | all replicas | Zero tolerance for failure |

### 4.5 A minimal quorum replication in code (to demystify the math)

```python
import asyncio

class QuorumStore:
    def __init__(self, replicas, n):
        self.replicas, self.n = replicas, n

    async def write(self, key, value, version, w):
        acks = 0
        async def _w(r):
            nonlocal acks
            await r.put(key, value, version)   # set-if-newer by version
            acks += 1
        done, _ = await asyncio.wait([_w(r) for r in self.replicas],
                                     return_when=asyncio.FIRST_COMPLETED)
        for t in asyncio.as_completed([_w(r) for r in self.replicas]):
            await t
            if acks >= w:
                break
        return acks >= w

    async def read(self, key, r):
        versions = await asyncio.gather(*(rep.get(key) for rep in self.replicas))
        fresh = [v for v in versions if v is not None]
        if len(fresh) < r:
            raise RuntimeError("read quorum not met")
        newest = max(fresh, key=lambda x: x.version)   # <-- read repair trigger
        await self.repair(key, newest)                 # push newest to stale replicas
        return newest
```

The two crucial lines are `max(..., version)` and `repair(...)`: **versioning + read repair is what turns overlapping quorums into actual consistency.**

### 4.6 AWS RDS / Aurora: what "Multi-AZ" versus "read replica" means

| Feature | Replication | Failover | Reads from standby? |
|---|---|---|---|
| **RDS Multi-AZ (instance)** | **Synchronous** to a standby in another AZ | Automatic (DNS flip, ~60–120s) | **No** (unless Multi-AZ cluster mode) |
| **RDS Multi-AZ (cluster)** | Semi-sync to 2 readable standbys | Automatic, fast (~35s) | Yes |
| **RDS Read Replica** | **Asynchronous** | Not automatic (must promote) | Yes |
| **Aurora** | Redo-log-based, storage-level; 6 copies across 3 AZs | Fast, automated | Yes (up to 15 replicas) |

The senior takeaway: **"we have a read replica" does not mean "we have HA"**. A read replica is async and may lag; a standby is (semi-)sync but may not serve reads. Many teams learn this during their first failover.

---

## 5. Where to Implement It (Systems and When Each Fits)

| System | Default replication style | Tunable? | Fits when |
|---|---|---|---|
| **PostgreSQL** | Single-leader, physical (WAL) or logical | `synchronous_commit`, `synchronous_standby_names`, slots | OLTP + read replicas + CDC; you want a relational core |
| **MySQL / InnoDB** | Single-leader, binlog (row/statement), GTID | `semi-sync`, group replication | Legacy OLTP, wide ecosystem |
| **MongoDB** | Replica set, oplog, automatic failover | `writeConcern: w:majority`, `readConcern: majority/linearizable` | Document workloads needing HA + flexible schema |
| **Kafka** | Partitioned log, ISR, leader per partition | `acks`, `min.insync.replicas`, RF, ULE | Event streaming, CDC backbone, decoupling |
| **Cassandra** | Leaderless, tunable quorum per DC | CL per query, RF per DC | Huge write throughput, multi-region, AP workloads |
| **DynamoDB** | Leaderless, Multi-AZ by default | `ConsistentRead`, global tables (MREC/MRSC) | Serverless KV; global active-active |
| **Riak** | Leaderless, sloppy quorum, CRDTs | N/R/W, `allow_mult` | AP, multi-DC, offline-tolerant |
| **Spanner / CockroachDB / Yugabyte** | Paxos/Raft per shard, multi-leader group | (mostly fixed: strong) | Globally consistent SQL, financial |
| **etcd / Consul / ZooKeeper** | Raft/ZAB consensus | Fixed quorum | Coordination, leader election, config |
| **Aurora / RDS** | Storage-level or WAL, sync/async mix | Multi-AZ vs read replica | Managed Postgres/MySQL HA |
| **Snowflake / BigQuery / Redshift** | Managed, hidden multi-AZ replication | (managed) | Analytical; you consume their consistency, not configure it |
| **Delta Lake / Iceberg** | Object storage (replicated) + metadata log | Snapshot isolation, OCC | Lakehouse; replication is the cloud provider's job |

**Choosing guide (short):**
- Need a **failover target with RPO=0** → synchronous/semi-sync standby (RDS Multi-AZ, Postgres `ANY 1`, Cosmos strong).
- Need **read scaling** → async read replicas / follower reads (cheap, tolerant of staleness).
- Need **global active-active writes** → multi-leader / leaderless with conflict resolution (DynamoDB global tables, Cassandra multi-DC, Cosmos multi-region) — *and* accept that conflicts are now your problem.
- Need **coordination/leader election** → consensus (etcd/Consul/ZooKeeper/Raft).
- Need an **event backbone / CDC** → Kafka replication semantics.

---

## 6. Patterns and Anti-Patterns

### Patterns

- **Sync for the money, async for the metrics.** Use synchronous/majority commit on the small set of write paths where loss is unacceptable; use async everywhere else to keep latency low.
- **Follower reads with a freshness budget.** Serve analytical reads from replicas but require a max-staleness check (LSN/version) before trusting them.
- **Read-your-writes via routing, not via waiting.** Route a session's reads to the leader (or the follower that just saw its write) for a short window after a write.
- **CDC off the log, not off triggers.** Read the replication log for change streams; never poll `updated_at` (misses deletes, races, and backdated writes).
- **Replication slot + retention guardrails.** Always cap how much log a lagging consumer can retain (`max_slot_wal_keep_size`, Kafka `retention.ms`/`retention.bytes`).
- **Idempotent apply.** Make followers/consumers tolerate duplicate events (upserts, `INSERT ... ON CONFLICT`, event IDs) so retries are safe.
- **Fenced writes.** Any promoted leader must fence the old one (see `06-leader-follower-replication.md`).

### Anti-Patterns

- **"Belt and braces" synchronous to *all* replicas.** `synchronous_standby_names = 'ANY 2 (a,b,c)'` with an unhealthy replica can stall the whole primary. Availability quietly dies.
- **Treating a read replica as a failover target.** They're async; promoting one loses the tail.
- **Running heavy analytics on the primary's synchronous standby.** You starve the apply process → lag → failover risk.
- **Assuming `W+R>N` = linearizable.** It isn't, without versioning and read repair.
- **Unbounded replication slots.** Disk fills; the primary crashes; the whole company notices.
- **Two leaders writing the same key with `ON CONFLICT DO UPDATE` and no coordination.** Silent last-writer-wins data loss.
- **Ignoring clock skew in LWW.** Multi-region LWW with skewed clocks discards the *causally newer* value (see `05-eventual-consistency.md`).
- **No lag SLO.** If nobody alarms on `pg_stat_replication.replay_lag`, you discover lag during an incident.

---

## 7. Real-World Use Cases and Industry Applications

- **Facebook / Meta — MySQL at planetary scale.** Sharded MySQL with a single writer per shard and asynchronous replicas, plus a custom raft-based automatic failover (the "MySQL Raft" work). Reads fan out to many replicas; the writer is the serialization point. This is the canonical single-leader-at-scale architecture.
- **LinkedIn — Kafka as the replication backbone.** Kafka's ISR (in-sync replicas) + `acks=all` + `min.insync.replicas` give durable, ordered replication of events; the rest of the company (databases, caches, search, lake) derives from it. The log is the source of truth.
- **Amazon — Dynamo / DynamoDB.** Leaderless replication with consistent hashing, vector clocks (Dynamo), tunable reads (`ConsistentRead`), and global tables for multi-region active-active. Built to never refuse a write; pushed conflict resolution to the application.
- **Google — Spanner.** Per-shard Paxos groups (multi-leader *within* a shard) plus TrueTime for externally consistent, globally distributed transactions. Strong consistency without giving up geo-distribution — because they invested in clocks.
- **Aurora / RDS — cloud HA as a service.** Storage-level replication (6 copies, 3 AZs) and automated failover made "multi-AZ" a checkbox. The trade-off (a shared storage layer) changes the failover profile versus classic log shipping.
- **Snowflake / BigQuery — analytical replication hidden.** Data is replicated across zones (and, optionally, regions) by the platform; you get "read-after-write within a session" semantics and near-zero operational replication work. You *do* care about cross-region replication lag and cost.
- **Debezium / Kafka Connect — CDC as a product.** Turns Postgres/MySQL/MongoDB/Oracle replication logs into Kafka topics, feeding lakehouse silver/gold layers in near real time.

---

## 8. Senior-Level Deep Dive

### 8.1 Decision framework: choose replication per write-path, not per system

```
For each write path, answer in order:
1. RPO?  Can we lose this write on failover?      no → synchronous/majority; yes → async
2. Latency budget?  What p99 is acceptable?        tight → async or semi-sync with nearby quorum
3. Failure domain?  AZ only, region, or global?    multi-region → pick conflict semantics first
4. Read fan-out?  How many readers, how fresh?     stale-tolerant → async read replicas
5. Blast radius of a stall?  If writes block,      high → avoid sync-to-all; prefer ANY k
   what breaks downstream?
```

The mistake juniors make is choosing *one* replication mode for the whole system. Seniors choose *per operation*: the ledger uses majority commit; the clickstream uses `acks=1`; the profile update tolerates LWW.

### 8.2 Replication lag math and SLOs

Express lag two ways, and alert on both:

- **Time lag** (`replay_lag`): how many seconds behind. Intuitive, but noisy under idle traffic.
- **Byte/position lag** (`pg_wal_lsn_diff`, Kafka consumer lag): monotonic, better for alerting.

```
Replica freshness SLO example:
  p99 replay_lag < 500 ms   (normal ops)
  p99 replay_lag < 5 s      (peak, bounded)
  max replay_lag < 30 s     (hard alarm → page)

Capacity math:
  lag ≈ (leader write throughput in bytes/s) / (follower apply throughput in bytes/s) − 1
  If the leader writes 40 MB/s and a follower applies 30 MB/s,
  lag grows unboundedly:   backpressure = (40−30)/30 ≈ 33% per second of backlog.
  Fix: faster disks, parallel apply, fewer replicas, or shed write load.
```

> **Senior note:** lag is usually a *write amplification* problem, not a network problem. Check what is writing (batch jobs doing 1M-row updates?, unindexed bulk deletes?) before blaming the network.

### 8.3 Failure modes and their signatures

| Failure | Signature | Root cause | Fix / mitigation |
|---|---|---|---|
| **Replica can't catch up** | Byte lag grows, never recovers | Leader writes > follower applies | Faster follower, parallel apply, batch writes |
| **Slot/WAL bloat** | Disk fills on leader | Consumer never drains slot | `max_slot_wal_keep_size`, alert on slot lag, drop dead slots |
| **Failover loses tail** | Rows missing right after promotion | Async replica promoted | Use semi-sync standby for critical paths |
| **Split-brain** | Two writers, divergent data | No quorum/fencing on failover | Majority quorum + fencing tokens (see ch. 6) |
| **Stale read reported as fresh** | User sees old value after write | Read routed to lagging replica | Read-your-writes routing, version checks |
| **Apply stalls on one transaction** | Follower pinned at one LSN | Huge transaction / DDL | Chunk writes; avoid 100M-row single txn |
| **Schema drift on logical replica** | Apply error, replication stops | `ALTER TABLE` on publisher/replica mismatch | Manage schema migrations through replication |
| **Overloaded standby** | Lag spikes correlated with analytics | Analytics competing with apply | Dedicated read replicas; resource groups |

### 8.4 Debugging replication: a practical runbook

1. **Quantify.** `pg_stat_replication` (Postgres), `SHOW REPLICA STATUS` (MySQL), `rs.status()`/`db.serverStatus().oplog` (Mongo), `kafka-consumer-groups.sh --describe` (Kafka lag), `nodetool tpstats`/`ring` (Cassandra).
2. **Locate the bottleneck.** Is the leader's WAL/binlog production the problem, or the follower's apply? Compare write throughput to apply throughput.
3. **Look for one slow transaction.** A single large transaction can pin the apply position for minutes. Find it in the log stream.
4. **Check resource saturation.** Follower IOPS, CPU, and (crucially) single-threaded apply if parallelism is off.
5. **Check log retention.** Has the leader already truncated the segment the follower needs? (If so, the replica is unrecoverable without a re-snapshot.)
6. **Check the consumer/network.** TCP retransmits, MTU issues, cross-AZ bandwidth throttling.
7. **Reproduce with a replay test.** Re-init a replica from a snapshot to see if it's systemic or node-specific.

### 8.5 The consistency/latency frontier, concretely

```
Latency (p99 write) vs durability:

async,        1–2 ms    RPO > 0
semi-sync     ~ (k-th replica RTT)  e.g. 3–8 ms same-AZ
sync-to-all   max replica RTT       e.g. 10–15 ms, and a slow replica => seconds
geo sync      cross-region RTT      e.g. 60–200 ms   (only for global ledgers, e.g. Spanner)

Availability vs durability:
  async  → survives losing any replica, may lose data
  sync   → never loses data, but a single bad replica blocks writes
  majority → the sweet spot: tolerate ⌊N/2⌋ failures, keep RPO=0
```

### 8.6 Replication in the modern data stack (Spark / Kafka / warehouses)

- **Kafka → Bronze.** A replicated Kafka log is your immutable bronze layer; replication factor and `min.insync.replicas` determine its durability.
- **Debezium → Silver.** Logical replication slots feed CDC topics; Spark Structured Streaming or Flink applies them idempotently into a lakehouse table (with a merge/upsert keyed on the primary key and LSN).
- **Warehouses → Gold.** Snowflake/BigQuery replicate (and, cross-region, copy) micro-partitions/snapshots; your "freshness" SLA is partly the platform's replication lag.
- **Object storage** (S3/GCS/ADLS) is internally replicated (11 nines in S3's standard/reduced classes), so lakehouse *files* are durable; the *metadata* (Delta log, Iceberg catalog) is what you must protect.
- **Idempotency is the through-line.** Because replication guarantees *at-least-once* delivery of change events, every downstream layer must be upsert-safe. This is where replication theory becomes daily DE work.

---

## 9. Common Mistakes and How to Avoid Them

1. **Assuming async replication is "good enough" for RPO=0 requirements.** → Map every critical write path to a sync/majority mode; document RPO per path.
2. **Reading from a replica immediately after writing.** → Route that read to the leader, or use a version/LSN gate.
3. **Calling `W+R>N` "strong consistency".** → Add versioning + read repair; or use a system that actually implements linearizability.
4. **Using `synchronous_standby_names = '* '` / sync-to-all.** → Use `ANY k (…)` to keep availability while bounding loss.
5. **Forgetting log retention for the consumer.** → Configure slots with `max_slot_wal_keep_size`; alert on slot lag; drop dead slots.
6. **Promoting an async replica in a panic.** → Automate failover with quorum + fencing; never let a human pick the newest-looking node under stress.
7. **No replication-lag SLO or alarm.** → Alert on both time-lag and byte-lag; tie to a freshness SLO.
8. **Confusing read replicas with HA standbys.** → Label them in your architecture diagram; set expectations in the on-call runbook.
9. **Running ETL directly against the primary.** → Use dedicated replicas/CDC; protect the OLTP write path.
10. **Ignoring conflict semantics in multi-leader.** → If writes can hit two leaders for the same key, pick a resolution strategy *before* launch (ch. 5).
11. **Schema drift breaks logical replication.** → Version schemas and run migrations in a replication-aware order.
12. **LWW with unsynchronized clocks across regions.** → Use hybrid logical clocks / version vectors, not wall clocks (ch. 5).

---

## 10. Exercises / Interview Questions

> Hints only — no full answers inline.

1. **Quorum math.** With N=5, which `(W, R)` pairs guarantee that any read sees the latest acknowledged write? Which minimize write latency? *(Hint: enumerate `W+R>N`; think about majority and failure tolerance separately.)*
2. **RPO/RTO design.** Design replication for a payments service (RPO=0, RTO<60s) and a metrics service (RPO=5min, RTO=1h). Which modes and topologies? *(Hint: synchronous majority + automated failover vs async + eventual.)*
3. **Read-your-writes.** A user updates their profile and immediately loads the page from a replica that lags 300 ms. Give two client-side and two server-side fixes. *(Hint: sticky sessions, versioned reads, leader-read window, session tokens.)*
4. **Kafka durability.** Given RF=3, `acks=all`, `min.insync.replicas=2`, `unclean.leader.election.enable=false`, can you lose an acknowledged message if one broker dies? Two? *(Hint: walk through ISR shrink and what `acks=all` actually waits for.)*
5. **CDC correctness.** Why does polling `WHERE updated_at > last_run` miss deletes and race with transactions — and how does reading the replication log fix it? *(Hint: deletes have no `updated_at` row to read; concurrent long transactions commit out of timestamp order.)*
6. **Debugging lag.** A follower's byte lag grows without bound while the network is healthy and the leader's write rate is steady. Enumerate the likely causes in order of probability. *(Hint: apply throughput, one huge transaction, single-threaded apply, disk saturation, runaway batch job.)*
7. **Topology hazard.** In an all-to-all topology, explain how message overtaking can cause LWW to discard a causally newer write. What structure fixes it? *(Hint: causal ordering; version vectors.)*
8. **Slot bloat.** A Debezium connector is down for three days. What happens to the Postgres primary, and how do you prevent the outage next time? *(Hint: WAL retention by slots; `max_slot_wal_keep_size`, monitoring, and drop-with-care.)*
9. **Choosing a replica type.** Contrast a hot standby, a read replica, and a logical replica for: (a) failover, (b) read scaling, (c) CDC into a lake. *(Hint: sync/async/filtered; promotability; schema independence.)*
10. **Cross-region writes.** You need active-active writes in `us-east` and `eu-west` for a shopping cart. What conflict resolution will you choose and what can still go wrong? *(Hint: CRDT counters/sets vs LWW; clock skew; availability during partition.)*

---

## References

- Kleppmann, M. — *Designing Data-Intensive Applications*, Chapter 5: "Replication" (1st ed.) / Chapter 6 (2nd ed.). https://dataintensive.net/
- PostgreSQL Documentation — Replication: https://www.postgresql.org/docs/current/runtime-config-replication.html
- PostgreSQL Documentation — Streaming Replication Protocol: https://www.postgresql.org/docs/current/protocol-replication.html
- PostgreSQL Documentation — Logical Replication: https://www.postgresql.org/docs/current/logical-replication.html
- Apache Kafka Documentation — Replication: https://kafka.apache.org/documentation/#replication
- Conduktor — Kafka `min.insync.replicas` explained: https://www.conduktor.io/kafka/kafka-topic-configuration-min-insync-replicas
- DeCandia et al. — "Dynamo: Amazon's Highly Available Key-value Store" (SOSP 2007): https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf
- Amazon Web Services — DynamoDB Read Consistency: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ReadConsistency.html
- Apache Cassandra / DataStax — Configuring consistency: https://docs.datastax.com/en/cassandra-oss/3.0/cassandra/dml/dmlConfigConsistency.html
- MongoDB Documentation — Write Concern (w:majority): https://www.mongodb.com/docs/manual/core/replica-set-write-concern/
- MongoDB Documentation — Replication / oplog: https://www.mongodb.com/docs/manual/replication/
- Karger et al. — "Consistent Hashing and Random Trees" (STOC 1997): https://dl.acm.org/doi/10.1145/258533.258660
- Google Cloud — RDS/Aurora-style Multi-AZ replication (AWS docs): https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Concepts.MultiAZ.html
- Debezium Documentation — CDC from PostgreSQL: https://debezium.io/documentation/reference/stable/connectors/postgresql.html
- Jepsen — Analyses (real-world replication correctness findings): https://jepsen.io/analyses
