# CAP Theorem (+ PACELC)

> CAP is the most cited and most misunderstood theorem in distributed systems. The short version — "you can only pick two of Consistency, Availability, Partition tolerance" — is wrong in a way that causes real architectural mistakes: teams who think they can "choose CA" build systems that fall over on their first network blip, and teams who think "AP means no guarantees" over-provision consistency they never needed. What CAP actually says is narrow and precise: **when a network partition occurs, a distributed system can guarantee either linearizability or availability, but not both.** Everything else is a design conversation. PACELC extends it with the case you hit every single day — no partition — where the real trade-off is **latency versus consistency**. This chapter tells you what the theorem really says, why it's often the wrong tool, and how to map real systems (Spanner, Cassandra, DynamoDB, Postgres, Kafka, Cosmos DB) onto the choices that actually matter.

## Why a Data Engineer Cares

- **You pick consistency levels every day**, whether you call it that or not: Kafka `acks`, DynamoDB `ConsistentRead`, Cassandra `QUORUM`, Cosmos DB consistency levels, BigQuery's "eventual" materialized views, Snowflake's "read-after-write within a session".
- **Your pipeline runs during partitions.** When a cloud region degrades, your Kafka producers, Spark jobs, and CDC connectors face exactly the CAP decision. Knowing whether to fail fast or degrade gracefully is a design choice you must make *before* the incident.
- **CAP explains vendor marketing.** "We're CP" (CockroachDB, Spanner), "we're tunable AP" (Cassandra, Cosmos), "we're CA" (single-node Postgres — and this is fine because there is no partition to tolerate). Understanding the claim lets you evaluate it.
- **PACELC is the one that bites in normal operation.** Choosing strong consistency in a globally distributed database costs you tens to hundreds of milliseconds on every write, forever — not just during failures.

CAP is vocabulary. PACELC is the decision framework. A senior needs both, and needs to know when CAP stops being useful.

## TL;DR

- **CAP (precise):** In a network-partitioning distributed system, you cannot have both **linearizability** (a strong form of consistency) and **total availability**; you must sacrifice at least one *during the partition*.
- **"P" is not a choice.** Networks partition. You either tolerate partitions or you lose availability. So the real choice is **C or A *when* a partition is happening**.
- **CAP is not a menu; it's a spectrum.** "Consistency" (in CAP) means *linearizability* — a very strong guarantee. Most systems offer weaker but useful guarantees (causal, read-your-writes, bounded staleness) that CAP's binary framing ignores.
- **PACELC:** **if Partition → A vs C; Else → Latency vs C.** The "else" branch dominates system design, because partitions are rare and latency is constant.
- **CP systems** (Spanner, etcd, ZooKeeper, HBase, CockroachDB): refuse/fail writes if they can't reach a quorum; keep data correct.
- **AP systems** (Cassandra, DynamoDB default, Riak, DNS): keep answering; accept stale/divergent data and reconcile later.
- **"CA" is a single-node (or effectively single-node) system.** No partition tolerance = not a distributed system in the CAP sense.
- **Tunable systems** (Cassandra, Cosmos DB, DynamoDB per-read) let you choose per operation — the pragmatic answer for most real architectures.
- **Latency is a soft partition.** Cross-region synchronous replication gives you the same trade-off as a partition, without a partition, every request.

---

## 1. Definitions and Core Vocabulary

The theorem only makes sense with Brewer's/Gilbert & Lynch's exact definitions. Loose definitions are where all the confusion comes from.

| Term | Precise meaning (Gilbert & Lynch formalization) |
|---|---|
| **Consistency (C)** | **Linearizability**: there exists a total order of operations consistent with real time, where every read returns the most recent write. Equivalently, the system behaves as if there were a single copy that processes operations atomically. **This is a very strong guarantee.** |
| **Availability (A)** | **Total availability**: *every* request received by a *non-failing* node must return a non-error response. Not "high uptime" — *every* request, on every reachable node. |
| **Partition tolerance (P)** | The system continues to operate (make some response) even when an arbitrary number of messages between nodes are dropped by the network. |
| **Partition** | A network fault that splits nodes into groups that cannot communicate. Messages within a group get through; cross-group messages are lost. |
| **Linearizability** | See C. Often called "atomic consistency" or "strong consistency". |
| **Sequential consistency** | A weaker total order: all nodes agree on *an* order, but it need not respect real time. (See `04-consistency-models.md`.) |
| **CAP-C** | Note: CAP's C is *not* ACID's C (constraint integrity) and *not* "all replicas identical". It is linearizability. |
| **Harvest** (Brewer) | The fraction of requests that can be answered with *useful* (not just any) data during a partition. |
| **Yield** (Brewer) | The fraction of requests that receive a response at all. |
| **PACELC** | Under **P**: choose **A** or **C**. **E**lse (no partition): choose **L**atency or **C**onsistency. Coined by Daniel Abadi (2012). |
| **Latency** | The time to serve a request under a given consistency guarantee. |

> **Senior note:** The colloquial "consistency" (Amazon DynamoDB's "strongly consistent read", DynamoDB's "CAP-C") and "availability" (uptime SLA) are *not* the CAP definitions. When a vendor says "we chose CA", they usually mean "we're a single node" or "we chose high uptime", neither of which is the theorem's statement.

---

## 2. What Problem It Solves / Why It Matters

CAP tells you where the **wall** is. It cannot be avoided, only navigated:

- **It sets the outer bound of what's possible.** You cannot build a system that is simultaneously linearizable, totally available, and partition-tolerant. That is a theorem, not an engineering limit. Every "we solved CAP" claim is really "we chose a weaker C, or a weaker A, in a specific regime".
- **It forces you to state your priorities.** Which matters more for this specific operation: a correct answer, or *any* answer, right now? A payment ledger and a "trending hashtags" widget answer differently.
- **It explains why consistent systems have failure modes.** CP systems fail *closed* (refuse writes) — a deliberate choice, not a bug. A DE who expects Cassandra with `CL=ALL` to "just work" during a node failure is fighting the theorem.
- **It frames budget conversations.** "Always strongly consistent, globally" is a *purchase*, not a design. It costs synchronized clocks + consensus round-trips, or it costs you latency.

**The practical reframing (Brewer, 2012):** stop asking "which two of three?" and ask **"when a partition happens, should this specific operation sacrifice correctness or availability?"** Then ask the PACELC question for the much more common no-partition case: **"how much latency am I willing to pay for consistency?"**

---

## 3. How It Works Internally (Mechanisms, Internals)

### 3.1 The proof sketch (why you can't have all three)

```
Two nodes, G1 and G2, separated by a partition (they can't talk).

Setup: initial value v0. Client writes v1 to G1; client reads from G2.

For A (availability): G2 MUST respond to the read without
   hearing from G1. So it returns the last value it knows: v0.   → not linearizable.

For C (linearizability): G2 MUST return v1 (the latest write) or
   refuse to answer. Refusing violates A. Returning v0 violates C.

∴ During the partition you can keep A or C, but not both.  ∎
```

That's the whole content of CAP. The theorem says nothing about what happens when there *is* no partition (that's PACELC's job), and nothing about weaker consistency models.

### 3.2 Why "P" isn't really optional

If a system is distributed across a network, messages *will* be delayed and dropped (GC pauses, cable cuts, overloaded NICs, AZ failures, misconfigured firewalls). You cannot "not have P". So:

```
Bad framing:   choose 2 of {C, A, P}          ← misleading
Right framing: P happens. When it does, choose {C or A}. ← correct
```

A "CA" choice is only coherent for a system that **cannot** partition: one node, or a system where cross-node communication failure means the whole system is down (so P never manifests as a *choice*).

### 3.3 The lower bound: partitions are not binary, and "never return" is not allowed

The formal proof assumes a partition means messages are *lost*, and that a node must respond "in a bounded time" to be available. Two subtleties:

- **Slow ≠ partitioned, but the choice is identical.** If you enforce a timeout to decide "the other node is unreachable", a slow node triggers exactly the CAP decision. This is why *timeouts* are the real implementation of CAP: they convert "no answer yet" into "treat as partitioned".
- **You can partially satisfy both** via **harvest/yield** (Brewer): return *something* (yield high), but possibly for a subset of data (harvest low), or vice versa. Most real systems do this — a read replica serving *its* data (partial harvest) with low latency (high yield).

### 3.4 PACELC: the part that matters daily

```
        ┌───────────────────────────────────────────────┐
        │            Is there a partition?               │
        └──────────────┬─────────────────┬──────────────┘
                     YES                NO
                      │                 │
              ┌───────▼───────┐  ┌──────▼───────┐
              │ A or C ?      │  │ L or C ?     │
              │ (CAP)         │  │ (latency vs  │
              └───────────────┘  │ consistency) │
                                 └──────────────┘
```

Classification notation: **X/Y** = choice under Partition / choice Else (no partition).

| System | PACELC | Under partition | Otherwise |
|---|---|---|---|
| **Spanner** | PC/EC | Consistency | Consistency (pays latency via TrueTime wait) |
| **CockroachDB** | PC/EC | Consistency | Consistency (follow-the-workload, but sync commit) |
| **DynamoDB (default)** | PA/EL | Availability | Latency (eventually consistent reads by default) |
| **Cassandra (QUORUM)** | PA/EC-ish | Availability (tunable) | Consistency if you set CL=QUORUM, else latency |
| **Riak** | PA/EL | Availability | Latency |
| **Cosmos DB** | PA or PC / EL or EC | Depends on level | Depends on level (5 levels!) |
| **MongoDB (w:1)** | PA/EL | Availability | Latency |
| **MongoDB (w:majority)** | PC/EC | Consistency | Consistency-ish |
| **etcd / ZooKeeper** | PC/EC | Consistency | Consistency |
| **Postgres single node** | (N/A) / EC | N/A (not distributed) | Consistency (it's one node — no latency cost) |
| **Postgres + sync standby** | PC/EC | Consistency | Latency (wait for standby) |
| **Kafka (acks=all, ISR≥2)** | PC-ish / EC-ish | Trades availability for durability | Latency (waits for ISR) |
| **Kafka (acks=1)** | PA/EL | Availability | Latency |
| **BigQuery (DML)** | ~PC/EC for writes, EL for stale reads | — | — |

### 3.5 The overlapping-quorum mechanism (how CP systems actually do it)

```
CP via majority quorum (Raft/Paxos):
  3 nodes. To commit a write, the leader must get ≥2 acks.
  If the leader is partitioned away from the other 2, it CANNOT commit → it
  stops serving writes (fails closed). The majority side elects a new leader.
  Result: linearizable, but the minority side is unavailable for writes.

AP via any-replica (Dynamo):
  Any replica accepts the write (or sloppy-quorum writes).
  During a partition both sides accept writes → divergence.
  Reconciled later (read repair, anti-entropy; see ch. 5).
```

Both are "correct" implementations of *different goals*. The CP version preserves C by sacrificing A *on the minority side*; the AP version preserves A by sacrificing C.

### 3.6 Time and order: why Spanner can be both

Spanner classifies as **PC/EC** (strong consistency always). How? It uses **TrueTime** (bounded-uncertainty atomic clocks + GPS) to assign globally meaningful timestamps, so a read can wait out the uncertainty interval and thus observe all writes that happened-before it. The cost is the **commit-wait** (a few milliseconds per transaction) and the hardware. CockroachDB/Yugabyte approximate this with hybrid logical clocks within a "closed timestamp" interval, trading a small latency penalty instead of custom clocks.

**Key insight:** strong consistency is not magically impossible; it costs **latency, hardware, or availability**, and you have to pay one of them. That's the theorem restated as economics.

---

## 4. How to Implement It (Step-by-Step, with Code / Config)

### 4.1 Decide per operation, not per system

```
Workflow:
1. List your operations (write order, write click, read order status, read feed...).
2. For each, answer:
   a. What happens if this read returns STALE data?        → tolerable? then AP/EL is fine
   b. What happens if this write is REJECTED during P?     → tolerable? then CP is fine
   c. What latency is acceptable?                          → drives the Else branch
3. Assign a consistency level per operation, not per cluster.
```

| Operation | Stale read? | Reject write? | Chosen mode |
|---|---|---|---|
| Balance check at ATM | No | Yes (better than wrong) | CP / strong read |
| Money transfer | No | Yes | CP / majority quorum |
| Social feed | Yes | No | AP / eventual |
| Clickstream ingest | Yes | No | AP / `acks=1` |
| Cart contents | No (own writes) | No | Read-your-writes session consistency |

### 4.2 Tunable consistency in practice

**Cassandra:**
```sql
-- write quorum + read quorum → W+R > N (see ch. 1 & 5)
CONSISTENCY QUORUM;      INSERT INTO payments (id, amt) VALUES (1, 100);
CONSISTENCY QUORUM;      SELECT * FROM payments WHERE id = 1;
-- AP-ish behavior when you use ONE/LOCAL_ONE; CP-ish when you use QUORUM/ALL.
```

**DynamoDB:**
```python
# Eventually consistent read (default, half the cost, may be stale):
table.get_item(Key={"id": 1})
# Strongly consistent read (may fail/slow in a partition; not available on GSIs):
table.get_item(Key={"id": 1}, ConsistentRead=True)
```

**Cosmos DB:** five levels, chosen at the account level (with session-level override):
```jsonc
// Strong (CP / EC): reads see the latest committed write.
// Bounded staleness (PC/EC-ish): reads lag by at most K versions or T time.
// Session (PA/EC-ish): monotonic reads + read-your-writes within a session.
// Consistent prefix (PA/EL): no gaps, but may lag.
// Eventual (PA/EL): any order, fastest.
```

**Kafka:**
```properties
# CP-leaning (durability over availability):
acks=all
min.insync.replicas=2
unclean.leader.election.enable=false

# AP-leaning (availability, may lose data):
acks=1
unclean.leader.election.enable=true
```

### 4.3 Implement a timeout-based partition decision (the real CAP switch)

```python
class QuorumWriter:
    def __init__(self, replicas, quorum, timeout_s=1.0):
        self.replicas, self.quorum, self.timeout_s = replicas, quorum, timeout_s

    async def write(self, key, value):
        # Timeouts ARE the partition detector.
        results = await asyncio.gather(
            *(asyncio.wait_for(r.put(key, value), self.timeout_s) for r in self.replicas),
            return_exceptions=True,
        )
        acks = sum(1 for r in results if not isinstance(r, Exception))
        if acks >= self.quorum:
            return "committed"                 # CP path: succeeded
        # Not enough acks: choose availability or consistency:
        #   AP choice → return "accepted-locally" and reconcile later
        #   CP choice → raise / fail the write
        raise NotEnoughReplicas(acks=acks, quorum=self.quorum)
```

The **timeout value is a policy decision**: too short → false partitions → spurious CP failures and unnecessary AP divergence; too long → slow failover and hung clients.

### 4.4 Design checklists per branch

**Choosing C (CP) — keep correctness, sacrifice availability:**
- Require majority quorum for writes (`Raft`, `Paxos`, `w:majority`, `CL=QUORUM`).
- Fail fast and loudly (`503`), don't silently downgrade.
- Ensure the client retries with backoff and idempotency keys.
- Have a documented **RTO** and an alarm for "no quorum".

**Choosing A (AP) — keep serving, reconcile later:**
- Accept writes anywhere (`acks=1`, `CL=ONE`, async replication).
- Ensure your application can reason about conflicts (or use CRDTs / LWW with well-chosen rules; see ch. 5).
- Provide **read-your-writes** at the session level if users expect it.
- Run **anti-entropy** and **read repair**; monitor divergence.
- Define a **reconciliation SLA** (how long until divergence is acceptable to fix).

### 4.5 The "Else" branch: buying back latency

For every CP choice, ask how to reduce its *cost* without giving up C:

| Technique | What it saves | Cost |
|---|---|---|
| Follower/local reads (`LOCAL_QUORUM`) | Cross-region RTT | Weaker cross-region consistency |
| Asynchronous materialized views | Read latency | Staleness window |
| Bounded staleness (`K` versions / `T` seconds) | Latency | Old-but-bounded data |
| Caching (Redis) with TTL | Read latency & load | Explicit staleness bound |
| Read-your-writes tokens | Correctness for the user | Client must carry a token |
| Lease-based leaders | Coordination latency | Clock assumptions, lease tuning |

---

## 5. Where to Implement It (Systems and When Each Fits)

| System | CAP posture | PACELC | Typical use |
|---|---|---|---|
| **Spanner** | CP (strong) | PC/EC | Global financial, inventory; needs external consistency |
| **CockroachDB / Yugabyte** | CP | PC/EC | Distributed SQL, geo-partitioning |
| **etcd / ZooKeeper / Consul** | CP | PC/EC | Coordination, leader election, config |
| **HBase / Bigtable** | CP (per-row) | PC/EC-ish | Strong single-row, wide-column |
| **MongoDB** | CP-ish (w:majority) or AP (w:1) | EC or EL | Document apps; tunable |
| **Redis (single)** | CA-in-practice (single node) | EC | Cache, ephemeral state |
| **Redis Cluster/Sentinel** | AP-leaning | EL/EC | Cache with failover; risk of loss |
| **DynamoDB** | AP by default, CP per-read | PA/EL (EL default) | Serverless KV, massive scale |
| **Cassandra / ScyllaDB** | AP (tunable toward CP) | PA/EL or PA/EC | Write-heavy, multi-region, time-series |
| **Riak** | AP | PA/EL | High availability, CRDT-friendly |
| **Cosmos DB** | Tunable (5 levels) | PA/EL … PC/EC | Multi-region app data |
| **Postgres (single)** | CA (no partition) | EC | OLTP, no distribution |
| **Postgres (+sync replica)** | CP-ish | PC/EC | HA OLTP |
| **Kafka** | CP/AP per config | PC/EC or PA/EL | Streaming backbone |
| **DNS** | AP | PA/EL | Global name resolution |
| **Snowflake / BigQuery** | Managed (mostly EC for committed writes) | ~PC/EC | Analytics; you don't configure CAP |
| **Elasticsearch** | AP-ish (per-shard, tunable `wait_for_active_shards`) | PA/EL | Search |

**Choosing guide (short):**
- **Correctness is non-negotiable and you can afford latency** → CP (Spanner, Cockroach, etcd, Postgres+sync, MongoDB w:majority).
- **Uptime is non-negotiable and staleness is tolerable** → AP (Cassandra, DynamoDB, Riak, DNS).
- **Both matter, per operation** → tunable (Cassandra, Cosmos, DynamoDB per-read).
- **Not distributed** → don't invoke CAP at all (single-node Postgres, an in-process cache).

---

## 6. Patterns and Anti-Patterns

### Patterns

- **Per-operation consistency.** Strong for money, eventual for metrics. Document the mapping in a table that lives in your repo.
- **Fail closed for writes that must not be wrong; fail open for reads that can be stale.**
- **Bounded staleness as a compromise.** "Reads may lag by ≤ 5 s or ≤ 1000 versions" is a contract you can actually test.
- **Session-level guarantees** (read-your-writes, monotonic reads) for user-facing flows — cheap and covers most UX expectations.
- **Timeout + retry with idempotency.** The pragmatic way to live with partitions without losing data.
- **Read from the primary when correctness matters, from replicas when it doesn't.** Route explicitly.
- **Classify data by value:** ledgers are CP; recommendations are AP; audit logs are append-only with durability but no ordering across nodes.
- **Use consensus-backed coordination for decisions**, not for bulk data (etcd for "who is leader", not for "all events").

### Anti-Patterns

- **Chanting "CAP says pick two".** → Replace with "what do we sacrifice during P, and what latency do we pay otherwise?"
- **Choosing "CA".** → There is no CA among partitionable systems. You chose single-node, or you'll lose A during P.
- **Conflating CAP-C with ACID-C or "replicas are identical".** → CAP-C = linearizability. Write it on the wall.
- **Assuming eventual consistency means "sometimes wrong forever".** → It means *converges*, with a defined reconciliation mechanism. Design the mechanism.
- **Using `CL=ALL` / sync-to-all-replicas "for safety".** → Availability collapses on the first node failure.
- **Applying strong consistency everywhere "to be safe".** → You buy latency and reduced availability you don't need; you also hide which operations truly require it.
- **Ignoring the Else branch.** → Teams obsess over partitions (rare) and ignore the everyday latency/consistency tax (constant).
- **Treating a timeout blip as a partition and flapping the cluster.** → Tune timeouts; add jitter; avoid aggressive failover.
- **No monitoring of divergence or quorum health.** → You discover an AP system's conflicts months later in a report.
- **Using CAP to justify data loss.** → "We're AP" is not a license to drop committed writes; you still owe durability and reconciliation.

---

## 7. Real-World Use Cases and Industry Applications

- **Google Spanner — CP at global scale.** TrueTime-based external consistency for AdWords/Google Cloud; the canonical proof that strong consistency is achievable if you pay latency + hardware. Netflix's open-source equivalent thinking (with CockroachDB/Yugabyte) shows the mainline pattern.
- **Amazon Dynamo (2007) → DynamoDB.** The origin story of AP-by-default: "always writable" shopping carts, vector clocks, application-level conflict resolution. DynamoDB made it managed and added per-read `ConsistentRead`.
- **Apache Cassandra at Netflix/Apple/Discord.** AP multi-DC writes with tunable quorums; chosen when write availability and throughput trump strong consistency.
- **etcd / ZooKeeper inside Kubernetes and Hadoop.** CP consensus used for coordination — exactly where you *want* to refuse progress rather than split.
- **Azure Cosmos DB — five consistency levels as a product.** The best-known commercial expression of the PACELC spectrum: one account can choose Strong, Bounded Staleness, Session, Consistent Prefix, or Eventual, and multi-region writes require relaxing away from Strong.
- **DNS — the world's biggest AP system.** Records propagate eventually; the internet is designed to prefer availability and accept staleness (TTL-bounded).
- **Kafka at LinkedIn/Uber — tunable durability.** `acks` and `min.insync.replicas` let the same cluster serve "never lose a payment event" and "best-effort metrics" workloads differently.
- **Snowflake/BigQuery — CAP abstracted away.** They present *effectively* consistent committed data; you consume it and manage staleness at the transformation layer (materialized views, incremental models).

---

## 8. Senior-Level Deep Dive

### 8.1 CAP is binary; the world isn't

CAP's C = linearizability, A = every-request-answered. Those are extremes. Real systems live on a continuum:

```
strongest ──────────────────────────────────────────► weakest
linearizable │ sequential │ causal │ read-your-writes │ monotonic │ eventual
  (CAP-C)      (agree on     (order   (own writes)      (time only  (any order,
               an order)      preserved)                 forward)    converges)
   Spanner      ZooKeeper     MongoDB   many leaders      session     Cassandra ONE,
   etcd         (some modes)  causal    routing           tokens      DynamoDB default
```

Between CAP-C and eventual lie guarantees that solve 95% of real UX problems at a fraction of the cost (see `04-consistency-models.md`). **A senior frames requirements in this richer ladder, not in CAP's binary.**

### 8.2 Harvest and yield (Brewer's own refinement)

Brewer later argued CAP should be about **harvest** (what fraction of data you can answer for) and **yield** (what fraction of requests you answer at all). This lets you describe real behavior:

```
During a partition:
  - "Answer 100% of requests, but only for the 60% of data whose replicas we can reach"
      → yield = 100%, harvest = 60%   (AP-ish, partial)
  - "Refuse requests when we can't reach a quorum"
      → yield = 0%, harvest = n/a      (CP-ish)
  - "Answer every request, possibly with stale data"
      → yield = 100%, harvest = 100% but C = weak (AP)
```

This is much closer to how you'll actually design a degraded read path.

### 8.3 Latency is the hidden partition

PACELC's "Else" branch exists because latency and consistency trade off *even without failure*:

```
Write to N replicas across regions and wait for majority:
  us-east + eu-west + ap-south
  majority latency ≈ 2nd-fastest RTT ≈ ~90 ms (eu-west at ~80 ms, ap-south at ~200 ms)
  Every single write pays this — forever — not just during incidents.

To avoid it you either:
  a) accept weaker consistency (async, local quorum), or
  b) accept higher latency (sync cross-region), or
  c) restructure (partition data by region so most writes are local).
```

**Option (c) is the senior move:** geo-partitioning so that each user's data lives in their region makes most operations *local* (fast) while preserving strong consistency *within* the region. Cross-region consistency becomes an explicit, rare operation instead of a constant cost.

### 8.4 Quantitative comparison of the branches

| Choice | Write p99 (same AZ) | Write p99 (cross-region) | Availability during P | Data loss risk |
|---|---|---|---|---|
| Async replication | ~1–3 ms | ~1–3 ms | 100% | Yes (tail lost on failover) |
| Majority quorum (local) | ~3–8 ms | ~5–15 ms | Majority side survives | None (RPO=0) |
| Sync to all replicas | ~8–30 ms | ~100–300 ms | Drops if any replica down | None |
| Global strong (Spanner-style) | ~10–20 ms | ~50–300 ms + commit-wait | Majority side survives | None |

Numbers are order-of-magnitude; the *shape* is what matters: global strong consistency is measured in tens to hundreds of milliseconds, and you pay it on every write.

### 8.5 Failure modes and their signatures

| Failure mode | Signature | Which branch | Mitigation |
|---|---|---|---|
| **Lost quorum** | Writes start failing (5xx), reads often still fine | CP losing A | Add replicas/AZs; ensure odd cluster sizes; alert on quorum loss |
| **Split-brain** | Two leaders, divergent writes | Both (bad) | Majority quorum + fencing (see ch. 6) |
| **Stale reads** | Users see old values after writing | AP / EL | Session tokens, read-your-writes routing |
| **Divergence never repaired** | Permanent inconsistent copies | AP mis-designed | Ensure anti-entropy/read repair runs and is monitored |
| **Timeout flapping** | Cluster churns leaders under load | Timeout too tight | Tune timeouts, add jitter, exponential backoff |
| **Silent downgrade** | "Strong" reads quietly served stale | Hidden replication lag | Instrument and assert the guarantee; don't just assume |
| **Latency cliff** | p99 spikes after adding a region | EC choice hitting cross-region sync | Geo-partition; local quorums; async cross-region |

### 8.6 Applying CAP to the modern data stack

```
Kafka:  acks=all + min.insync.replicas>=2 + ULE=false  → CP-leaning (durable, may block)
        acks=1 + ULE=true                              → AP-leaning (fast, may lose)
       Decision is PER TOPIC. Metrics can be AP; payments must be CP.

Spark:  A batch job on a CP source (Spanner) gets consistent snapshots; on an AP
        source (Cassandra ONE) it may read different values for the same key in
        two scans. Reproducibility requires a consistent read level or a snapshot.

Warehouse: Snowflake/BigQuery give effectively-consistent committed data; your
        "eventual consistency" lives at the materialized-view/incremental-model
        layer, where you choose refresh cadence = bounded staleness.

Lakehouse: Iceberg/Delta commits are optimistic-concurrency + atomic metadata swaps.
        Two writers to the same table race → one retries. That is CAP's C vs A
        decision, made by the table format's concurrency protocol.
```

### 8.7 Debugging CAP-shaped incidents: a runbook

1. **Establish which branch you're in.** Is this a *partition* (can some nodes still talk?) or a *latency* event? They have different fixes.
2. **Check quorum health first.** `etcdctl endpoint health`, Raft leader logs, Cassandra `nodetool status` (DN nodes), Kafka ISR shrink, Spanner/PG replica state.
3. **Ask: is the system failing closed or open?** CP systems fail closed by design — a 503 is "correct". Decide whether to *temporarily* relax consistency (if the business allows stale data) rather than page pointlessly.
4. **For AP systems: measure divergence.** Compare replica checksums/Merkle roots; check whether read repair and anti-entropy are running.
5. **Check the Else branch.** If there's no partition, you're looking at latency vs consistency: is a cross-region sync on the hot path? Can you localize it?
6. **Never "fix" availability by disabling durability silently.** Acknowledge the trade-off, document it, and get sign-off.

---

## 9. Common Mistakes and How to Avoid Them

1. **"Pick two of three."** → Reframe as "during a partition, choose C or A; otherwise choose L or C".
2. **Claiming a system is CA.** → Only a non-partitionable system is; everything distributed is CP, AP, or tunable.
3. **Treating CAP-C as ACID-C or replica equality.** → CAP-C is linearizability. Define your terms.
4. **Assuming eventual consistency = broken.** → It means convergence with a defined mechanism; specify and monitor it.
5. **Applying strong consistency uniformly.** → Per-operation choices; write them down.
6. **Using `CL=ALL`/sync-all "for safety".** → It trades your availability away.
7. **Ignoring PACELC.** → The everyday latency tax is the bigger bill.
8. **Letting timeouts silently define your consistency.** → Timeouts are policy; set them deliberately and document the consequences.
9. **No per-region local quorum design.** → Global quorums on the hot path = permanent latency.
10. **Not testing partitions.** → Use `tc netem`, Jepsen, or fault injection in staging; verify the fail-open/fail-closed behavior matches the design.
11. **Confusing uptime SLA with CAP-A.** → CAP-A is every-request-answered, not 99.99% monthly.
12. **Designing AP and never building reconciliation.** → Divergence compounds; add anti-entropy and a reconciliation SLA.

---

## 10. Exercises / Interview Questions

> Hints only — no full answers inline.

1. **Prove it.** State the CAP theorem precisely and give the two-node counterexample. *(Hint: a write to one side, a read on the other.)*
2. **Reframe.** A colleague says "we'll just pick CA". What do they actually mean, and what will happen during a partition? *(Hint: single-node or loss of availability.)*
3. **Map it.** Classify Cassandra, DynamoDB, Spanner, etcd, and single-node Postgres as CP/AP/"CA", with a one-line justification each. *(Hint: default configurations, not marketing.)*
4. **PACELC.** Explain `PA/EL` and `PC/EC` with one real system each, and describe the latency each pays in the *Else* branch. *(Hint: DynamoDB default vs Spanner.)*
5. **Per-operation design.** For a ride-hailing app, choose C/A for: driver location writes, trip state transitions, and rating aggregation. Justify. *(Hint: correctness vs freshness per operation.)*
6. **Harvest/yield.** During a partition, a service answers 100% of requests but only for data it can reach locally. Describe this in harvest/yield terms. *(Hint: yield=1, harvest<1.)*
7. **Latency vs consistency.** Why is a cross-region synchronous write a "latency partition"? Give three ways to avoid the cost. *(Hint: local quorums, geo-partitioning, async cross-region.)*
8. **Bounded staleness.** Design a "read no older than 5 seconds" guarantee. What must the system track, and how do you verify it? *(Hint: timestamps/versions; per-read staleness checks; tests.)*
9. **Tunables.** In Cassandra, you set write `CL=QUORUM` and read `CL=ONE`. Are you CP or AP? What breaks? *(Hint: W+R ≤ N → stale reads even without a partition.)*
10. **Kafka.** Compare two topic configs for a payments topic: `acks=all, min.insync.replicas=2, ULE=false` vs `acks=1, ULE=true`. Which is CP, which is AP, and what does each risk? *(Hint: loss vs availability during broker failure.)*
11. **Debunk.** "Spanner proves CAP wrong." Give the correct rebuttal. *(Hint: latency + hardware, not a violation.)*
12. **Incident.** During a regional outage, your CP database refuses writes (503) while your AP cache serves stale values. Write the decision rule for which is acceptable for which endpoint. *(Hint: fail closed vs fail open per operation; business sign-off.)*

---

## References

- Brewer, E. — "CAP Twelve Years Later: How the 'Rules' Have Changed" (IEEE Computer, 2012): https://www.infoq.com/articles/cap-twelve-years-later-how-the-rules-have-changed/
- Gilbert, S. & Lynch, N. — "Brewer's Conjecture and the Feasibility of Consistent, Available, Partition-Tolerant Web Services" (2002): https://groups.csail.mit.edu/tds/papers/Gilbert/BrewersConjecture.pdf
- Abadi, D. — "Consistency Tradeoffs in Modern Distributed Database System Design" (PACELC, IEEE Computer, 2012): https://www.cs.umd.edu/~abadi/papers/abadi-pacelc.pdf
- Wikipedia — PACELC design principle: https://en.wikipedia.org/wiki/PACELC_design_principle
- Kleppmann, M. — *Designing Data-Intensive Applications*, Chapters 8–9 (The Trouble with Distributed Systems; Consistency and Consensus). https://dataintensive.net/
- Corbett et al. — "Spanner: Google's Globally-Distributed Database" (OSDI 2012): https://research.google/pubs/pub39966/
- Google Cloud — TrueTime and external consistency: https://docs.cloud.google.com/spanner/docs/true-time-external-consistency
- DeCandia et al. — "Dynamo: Amazon's Highly Available Key-value Store" (SOSP 2007): https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf
- Microsoft — Azure Cosmos DB consistency levels: https://learn.microsoft.com/en-us/azure/cosmos-db/consistency-levels
- Apache Cassandra — Consistency levels / tunable consistency: https://docs.datastax.com/en/cassandra-oss/3.0/cassandra/dml/dmlConfigConsistency.html
- Amazon Web Services — DynamoDB read consistency: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ReadConsistency.html
- Apache Kafka — Replication and durability: https://kafka.apache.org/documentation/#replication
- Jepsen — Distributed systems safety research and analyses: https://jepsen.io/analyses
