# Consistency Models

> A consistency model is a contract between a data system and its clients that says exactly which values a read may return, given which writes have happened. "Strong consistency" and "eventual consistency" are the two words people use, but they are the two endpoints of a ladder with about a dozen rungs in between — and the middle rungs (read-your-writes, monotonic reads, causal, bounded staleness) are where almost all correct, fast, real systems live. This chapter gives you the precise definitions, the mechanisms that implement each rung (total order, happens-before, Lamport and vector clocks, hybrid logical clocks, session tokens), the cost of climbing from one rung to the next, and how to pick the *weakest* model that still satisfies your users. Get this right and you can ship a globally distributed system that is both fast and correct; get it wrong and you will spend a quarter chasing "impossible" bugs.

## Why a Data Engineer Cares

- **Kafka gives you per-partition ordering and nothing more.** Understanding that "global ordering" is a different (and expensive) model explains why your exactly-once stream still produced out-of-order events after a rebalance.
- **Delta/Iceberg commits are snapshot-isolated.** Two concurrent writers don't block; one fails and retries. That is a consistency model, and it dictates whether your pipeline must be idempotent.
- **Warehouses give session-scoped or eventually-consistent reads.** Snowflake's "read-after-write within a session", BigQuery's "eventually consistent" materialized views, and Redshift's snapshot isolation are all explicit rungs you must design around.
- **Every dashboard that "shows contradictory numbers" is a consistency-model mismatch** (reading two sources at two different rungs). Debugging it requires naming the model.
- **Your CDC pipeline must produce causal order.** Otherwise a downstream join sees a child row before its parent (the classic referential-integrity flicker).

Consistency models are the vocabulary that turns "the data looks wrong sometimes" into a precise, testable, fixable requirement.

## TL;DR

- **A consistency model = the set of allowable histories of reads/writes.** Stronger model = more intuitive, more expensive; weaker model = cheaper, weirder.
- **The ladder (strongest → weakest):** linearizable → sequential → causal → PRAM/session (read-your-writes, monotonic reads, monotonic writes) → consistent prefix → eventual.
- **Linearizability** = a total order consistent with real time; every read sees every completed write. Needs consensus or single-leader; costs latency/availability.
- **Sequential consistency** = one agreed total order, but it need not match real time (weaker than linearizable).
- **Causal consistency** = if A happens-before B, everyone sees A before B. Concurrent events may be seen in different orders by different nodes. The strongest model that stays available under partitions.
- **Session guarantees** (read-your-writes, monotonic reads, monotonic writes, writes-follow-reads) are cheap, client-side-verifiable, and cover most user-facing needs.
- **Serializability ≠ linearizability.** Serializability is about *transactions* (some equivalent serial order); linearizability is about *operations* (total order respecting real time). Together = **strict serializability** (Spanner).
- **Clocks are the tool:** real time (Spanner TrueTime), Lamport clocks (order), vector clocks (causality), hybrid logical clocks (both, approximately).
- **Pick the weakest model that meets the requirement.** "Strong everywhere" is a choice to pay latency and lose availability you may not need.

---

## 1. Definitions and Core Vocabulary

| Model | Guarantee | Intuition |
|---|---|---|
| **Linearizability** | There is a total order of operations consistent with real time; each read returns the value of the last write before it. Indistinguishable from a single copy. | "It's as if there's one machine." |
| **Sequential consistency** | There is a total order of operations that all nodes agree on, and each node's own operations appear in that order in program order. The order need NOT respect real time. | "Everyone agrees on *an* order, maybe not the real one." |
| **Causal consistency** | Writes that are causally related are seen in the same order by everyone. Concurrent writes may be seen in different orders. | "Nothing appears before its cause." |
| **Read-your-writes (RYW)** | After a client writes a value, that client's subsequent reads see it (or a newer value). | "I can see my own updates." |
| **Monotonic reads** | A client never sees time go backwards: once it reads a value, it never reads an older value. | "I don't un-see things." |
| **Monotonic writes** | A client's writes are applied in the order the client issued them. | "My writes land in order." |
| **Writes-follow-reads** | If a client writes after reading a value, the write is applied "after" that read (causal order across clients). | "My write builds on what I read." |
| **Consistent prefix** | Writes are seen in order, but a reader may lag behind. No gaps/reordering. | "I might be behind, but the story is linear." |
| **Bounded staleness** | Reads may be stale by at most K versions or T time. | "Never more than N old." |
| **Eventual consistency** | If writes stop, all replicas converge to the same value. No ordering guarantee meanwhile. | "It'll agree eventually." |
| **Serializability** | The outcome of concurrent transactions is equivalent to *some* serial execution. (A transaction-isolation model.) | "Transactions behave as if run one at a time." |
| **Strict serializability** | Linearizability + serializability: serial order matches real time. | "One machine, transactions, real time." |
| **Snapshot isolation** | Each transaction reads a consistent snapshot as of its start; write-write conflicts detected (often first-committer-wins). | "I see the world as of t0." |
| **Session guarantee** | Any of the above guaranteed *within a client session*. | "Per-user correctness." |
| **Happens-before (→)** | Lamport's partial order: A → B if A and B are in the same process in order, or A is a send and B its receive, or transitively. | "Causality." |
| **Concurrent** | Neither A → B nor B → A. | "No ordering required." |

> **Senior note:** **Linearizability is a *recency* guarantee (single-object, real-time). Serializability is a *transaction* guarantee (multi-object, order-based).** They answer different questions. A system can be serializable but not linearizable (some snapshot-isolation setups) and linearizable per operation but not serializable across transactions. Spanner is *both* → strict serializable.

---

## 2. What Problem It Solves / Why It Matters

Without a named model, "consistency" degenerates into hand-waving. With a named model, you can:

- **Write a testable requirement.** "Reads must be linearizable" or "reads may be stale by ≤ 5 s" is a spec; "the data should be correct" is not.
- **Choose the cheapest sufficient design.** Most features need only session guarantees — which are nearly free — not global linearizability.
- **Diagnose bugs by category.** "A user saw their comment disappear" is a monotonic-reads violation, and it has known fixes.
- **Reason about failure.** Under a partition, causal consistency is achievable while linearizability forces a choice (the CAP trade-off).

**Business mapping:**

| Requirement | Model needed |
|---|---|
| "After I post, I must see my post" | Read-your-writes |
| "The feed must never go backwards" | Monotonic reads |
| "A comment must never appear before the post it replies to" | Causal |
| "No two withdrawals may exceed the balance, ever" | Strict serializable |
| "Counts will settle within a minute" | Bounded staleness / eventual |
| "Analytics snapshot must be reproducible" | Snapshot isolation |

The cost ladder is real: each rung up buys correctness and costs latency, availability, or coordination. The skill is finding the *minimum* rung.

---

## 3. How It Works Internally (Mechanisms, Internals)

### 3.1 The cost ladder

```
  Cost / coordination ──────────────────────────────────────────►
  Latency        ▲

  linearizable        ← consensus (Raft/Paxos), single leader, or TrueTime
     │                 needs a quorum round-trip; fails closed under partition
  sequential          ← a single agreed order (log), not necessarily real-time
     │
  causal              ← track dependencies (vector clocks / causal tokens);
     │                 available under partition; no consensus needed
  session guarantees  ← client carries a token/version; route accordingly
     │                 (RYW, monotonic reads) — essentially free
  eventually consistent ← any replica answers; reconcile asynchronously
     │
  ──────────────────────────────────────────────────────────────►
```

**Key fact:** everything below causal is achievable with an *available* (AP) system. Causal is the strongest model you can have without giving up availability under partitions (Attiya/Mahajan/Burckhardt results). If your requirement is below causal, you never have to pay the CP tax.

### 3.2 Linearizability: mechanisms

Three ways to get it:

1. **Single leader + reads from the leader.** The leader totally orders everything. Simple; the leader is a bottleneck and a SPOF; reads don't scale.
2. **Consensus per operation (Raft/Paxos).** Writes and (linearizable) reads go through the log; a read may need a "read index" round to confirm leadership. Scales writes across shards, costs a quorum round-trip.
3. **Synchronized clocks (Spanner).** Assign globally meaningful timestamps so a read can wait out uncertainty and observe everything that happened-before. Costs TrueTime hardware + commit-wait.

```
Linearizable read via Raft "read index":
  1. Client asks a follower for key k.
  2. Follower forwards to the leader (or the client asks the leader).
  3. Leader confirms it is still leader (heartbeat quorum) and records the commit index.
  4. Leader waits until its state machine has applied up to that index.
  5. Returns the value.
  ⇒ one extra round-trip, but the read can never be stale.
```

### 3.3 Sequential vs linearizable: the difference, concretely

```
Streams S1 and S2 (sequential consistency: any total order consistent with
program order, NOT necessarily real time):

  real time:  S1 writes x=1 ────────► (time passes) ────────► S1 writes x=2
              S2:                    reads x=2 (?!)
  Sequential consistency ALLOWS S2 to read x=2 first and "x=1" isn't required
  to appear, as long as SOME global order exists. Linearizability forbids this,
  because the x=1 write completed before the read in real time.

  ⇒ Sequential = "an order exists"; linearizable = "the order is real-time".
```

### 3.4 Causal consistency: the sweet spot

Causal consistency preserves happens-before without requiring a global real-time order. Implementation relies on tracking dependencies:

```
Lamport clocks (total order-ish, witness of causality):
  Each process keeps a counter c. On event: c++. On send: attach c.
  On receive: c = max(c, msg.c) + 1.
  Property: A → B  ⇒  lamport(A) < lamport(B).  Converse not true (ties).

Vector clocks (exact causality):
  Each process keeps a vector of counters, one per process.
  A → B  ⇔  VC(A) < VC(B) componentwise. Concurrent ⇔ neither < nor >.
  Cost: O(N) metadata per event. (Used in Dynamo, Riak, some CRDT systems.)

Hybrid logical clocks (HLC): physical time + logical counter.
  HLC ≈ (wall_clock_ms, logical_tiebreak)
  Gives causally-consistent, roughly-real-time timestamps in O(1) space.
  Used by: CockroachDB, MongoDB (cluster time), Yugabyte.
```

```
Causal delivery: a replica must not deliver event B until it has delivered
every event A with A → B.

  A (post) ──▶ B (comment)          Replica 1: A then B   ✅
                                    Replica 2: B then A   ❌ (violates causality)
  ⇒ causal consistency = "no effect before its cause"
```

### 3.5 Session guarantees: cheap and mostly sufficient

Implemented with a **token** the client carries:

```
Read-your-writes (RYW):
  After a write, the client records a version/LSN/timestamp T.
  Subsequent reads must go to a replica whose applied position ≥ T
  (check the token; if the replica is behind, route to the leader or wait).

Monotonic reads:
  The client remembers the version it last saw (T).
  Future reads must be served by a replica with position ≥ T.
  ⇒ never "un-see" data. (Sticky routing to one replica also works.)

Monotonic writes:
  All of a client's writes are applied in issued order (single writer per key,
  or a log per client).

Writes-follow-reads:
  A write carries the version of the read it depends on; the write is applied
  after that version on every replica.
```

```python
# A simple read-your-writes implementation with a version token
class SessionClient:
    def __init__(self, replicas, leader):
        self.replicas, self.leader, self.ver = replicas, leader, 0

    def write(self, key, value):
        self.ver = self.leader.put(key, value)     # returns a version/LSN
        return self.ver

    def read(self, key, strict=False):
        if strict:
            return self.leader.get(key)            # always fresh
        for r in self.replicas:
            if r.applied_version() >= self.ver:    # replica is fresh enough
                return r.get(key)
        return self.leader.get(key)                # fall back to leader
```

This is the single most valuable technique in the chapter: **most "strong consistency" bug reports are read-your-writes bugs, and a version token fixes them without any global coordination.**

### 3.6 Bounded staleness and closed timestamps

```
Bounded staleness: "reads are at most Δ versions or T seconds behind."
  Implementation: each read carries a lower bound on the version it must see;
  the replica either serves it (if its position ≥ bound) or forwards to a fresher one.

Closed timestamps (CockroachDB/Yugabyte/Spanner-ish):
  A shard periodically advertises "all writes before timestamp T are final."
  Follower reads at <= T need no coordination with the leader → fast, and still
  consistent as of T. This is how you get "fast, non-stale-as-of-a-point" reads
  without paying a round-trip for every read.
```

### 3.7 Snapshot isolation (the transactional model you meet most)

```
Each transaction reads a consistent snapshot as of its start time.
Writes conflict only if two transactions write the same key (first-committer-wins).

  T1: BEGIN (t=100)                      T2: BEGIN (t=101)
  T1: read A=10, B=20                    T2: read A=10, B=20
  T1: write A=30 (commit t=102)          T2: write B=40 (commit t=103)  ✅ no conflict
  (both read a snapshot where A=10,B=20; both writes touch different keys)

  Write skew risk: T1 and T2 each "check a constraint then write a different key"
  → both pass their check, both commit, invariant broken.
  ⇒ Snapshot isolation ≠ serializability. Add explicit conflict detection
    (materialize conflicts / SELECT ... FOR UPDATE) where invariants span keys.
```

Snapshot isolation is what Iceberg/Delta commits, Postgres `REPEATABLE READ`, and many warehouses provide. It is **not** serializable unless the system adds serializable conflict detection.

---

## 4. How to Implement It (Step-by-Step, with Code / Config)

### 4.1 Choose the model by requirement (the procedure)

```
1. Write the user-visible invariant.        e.g. "I see my own comment immediately."
2. Classify it:                             RYW? monotonic? causal? linearizable?
3. Find the weakest model that satisfies it (below).
4. Implement the mechanism for that model.
5. Test it: inject replication lag / partitions and assert the invariant holds.
```

| Invariant | Model | Mechanism |
|---|---|---|
| "I see my own writes" | RYW | version token + route to fresh replica/leader |
| "My view never goes backwards" | Monotonic reads | sticky replica or version gate |
| "Comment never precedes its post" | Causal | causal token / dependency tracking |
| "No double-spend" | Linearizable / strict serializable | consensus or leader-bound transaction |
| "Counts settle in ≤ 60 s" | Bounded staleness | version bound + reconciliation |
| "Reproducible analytics" | Snapshot isolation | read at a fixed snapshot/timestamp |

### 4.2 Session tokens in practice

**Cassandra** (drivers do this automatically for the same session/DC):
```python
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra import ConsistencyLevel
profile = ExecutionProfile(consistency_level=ConsistencyLevel.LOCAL_QUORUM,
                           row_factory=dict_factory)
# Driver keeps a "routing key" so the same partition goes to the same replica → RYW-ish.
```

**Spanner:** read-your-writes and stronger via timestamps:
```sql
-- Strong read (default): sees all committed writes
SELECT * FROM Accounts WHERE id = 1;

-- Stale read: read as of a past timestamp (cheap, from any replica, no lock)
SELECT * FROM Accounts AS OF TIMESTAMP TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 15 SECOND)
WHERE id = 1;
```

**MongoDB:** read/write concern + `afterClusterTime` for causal sessions:
```javascript
const session = db.getMongo().startSession({ causalConsistency: true });
// Driver stamps reads with operationTime and reads at >= that cluster time,
// giving read-your-writes + monotonic reads within the session.
session.getDatabase("app").users.updateOne(
  { _id: 1 }, { $set: { name: "ana" } }, { writeConcern: { w: "majority" } }
);
```

**Kafka:** per-partition ordering is your only built-in order; get causality by keying:
```python
# All events for an entity go to one partition → causal order per entity.
producer.send("orders", key=str(order.user_id).encode(), value=event)
# For GLOBAL causal order you'd need a single partition (kills throughput)
# or an external causal-token scheme downstream (e.g., in Flink).
```

### 4.3 Causal ordering in a pipeline (the DE scenario)

```python
# Upstream emits parent/child rows. Downstream must apply parent first.
# Encode causality in the event: each event carries the max version it knows.
import json

def emit(event, seen):
    event["causal_parents"] = sorted(seen)     # the versions this event depends on
    return json.dumps(event), event["version"]

# Downstream buffer: hold an event until all causal parents have been applied.
class CausalBuffer:
    def __init__(self, apply_fn):
        self.applied, self.pending, self.apply_fn = set(), {}, apply_fn

    def receive(self, event):
        self.pending[event["version"]] = event
        progressed = True
        while progressed:
            progressed = False
            for v, e in list(self.pending.items()):
                if all(p in self.applied for p in e["causal_parents"]):
                    self.apply_fn(e); self.applied.add(v)
                    del self.pending[v]; progressed = True
```

### 4.4 Achieving snapshot isolation in a lakehouse

```sql
-- Delta Lake: write a snapshot, read with time travel for reproducibility
INSERT INTO silver.orders SELECT ... FROM bronze.orders WHERE _commit_version > 42;
SELECT * FROM silver.orders VERSION AS OF 42;      -- reproducible read
-- Both writers use optimistic concurrency; a conflict → retry (see 09 Common Mistakes).

-- Iceberg: snapshot isolation via snapshot IDs
SELECT * FROM catalog.db.orders FOR SYSTEM_VERSION AS OF <snapshot-id>;
```

**Rule:** analytical correctness requires reading *all* inputs at one snapshot. Mixing "current dimension" with "yesterday's fact" produces numbers that are individually right and jointly wrong. Data-warehouse batch windows are partly a consistency-model artifact.

### 4.5 Testing consistency in CI

```python
# Property test: after a write, a read must never return an older value.
async def test_read_your_writes(client, key):
    v = client.write(key, "v1")
    for _ in range(100):
        assert client.read(key) >= v     # monotonic + RYW
        await asyncio.sleep(0.01)

# Fault-injection variant: add 300ms of lag to one replica and repeat.
```

Tools: **Jepsen** (linearizability checking), **Porcupine** (Go linearizability checker), **knossos/elle** (Jepsen libraries), and `tc netem` for latency/partition simulation in staging.

---

## 5. Where to Implement It (Systems and When Each Fits)

| System | Default model | Tunable? | When to choose |
|---|---|---|---|
| **Postgres (single node)** | Linearizable (serializable optional) | Isolation levels, `synchronous_commit` | OLTP correctness; no distribution to pay for |
| **Postgres (sync replica)** | Linearizable-ish reads from leader | `synchronous_standby_names` | HA OLTP; correctness important |
| **MySQL Group Replication / Galera** | Certified/linearizable-ish | Sync mode | Multi-primary HA |
| **MongoDB** | Causal sessions + `readConcern`/`writeConcern` | `majority`, `linearizable`, `snapshot` | Document apps needing tunable guarantees |
| **Cassandra** | Tunable (ONE … QUORUM … ALL) | Per query | AP at scale with per-op consistency |
| **DynamoDB** | Eventually consistent by default | `ConsistentRead`; `TransactWriteItems` | Serverless KV; strong read on demand |
| **Spanner** | Strict serializable | Stale reads with bounds | Global strong SQL |
| **CockroachDB / Yugabyte** | Serializable; follower reads w/ closed timestamps | `AS OF SYSTEM TIME` | Distributed SQL, geo |
| **etcd / Consul / ZooKeeper** | Linearizable | (fixed) | Coordination, locks, leader election |
| **Kafka** | Per-partition total order (log order) | `acks`, `min.insync.replicas`, transactions | Streams; ordering per key |
| **Snowflake** | Snapshot-ish; RYW within a session | — | Analytics; batch snapshot reads |
| **BigQuery** | At-least-once DML + eventually consistent MVs | `ConsistentRead` not applicable | Serverless analytics |
| **Iceberg / Delta / Hudi** | Snapshot isolation (OCC commit) | Conflict resolution | Lakehouse concurrency |
| **Redis** | Linearizable per key on a single node; async to replicas | `WAIT` | Cache/in-memory structures |

**Choosing guide (short):**
- **Single-node OLTP** → you already have linearizability; don't over-engineer.
- **Multi-node OLTP that must be correct** → majority quorum / serializable (Spanner, Cockroach, Mongo `majority`+`linearizable`).
- **User-facing app data at scale** → session guarantees (RYW + monotonic) + eventual replication. Almost always sufficient.
- **Causal-heavy data (comments, edits, IoT)** → causal consistency (HLC/vector clocks).
- **Analytics** → snapshot isolation + a fixed batch window; reproducibility over freshness.

---

## 6. Patterns and Anti-Patterns

### Patterns

- **Version tokens for RYW.** The cheapest fix for the most common complaint.
- **Sticky reads per user/session.** Route one session to one replica; get monotonic reads for free.
- **Read-the-leader-after-write window.** For ~1 s after a write, route that user's reads to the leader.
- **Causal tokens in events.** Every event carries a "depends on" vector; consumers buffer until parents arrive.
- **Closed-timestamp follower reads.** Fast reads that are consistent as of a known point.
- **Snapshot everything for analytics.** Read all inputs at one snapshot/timestamp for reproducibility.
- **Bounded-staleness contracts.** "Reads lag ≤ 5 s" is testable; "eventually consistent" is not.
- **Idempotent, commutative writes.** When the model is weak, make operations safe to replay and reorder (CRDTs, upserts, `max` merges).

### Anti-Patterns

- **Assuming "the database is consistent" globally.** → Name the model per operation and document it.
- **Applying linearizability everywhere.** → You pay quorum latency and lose availability for guarantees nobody uses.
- **Reading your own writes from a lagging replica.** → The classic bug; fix with tokens/routing.
- **Mixing sources at different models in one query.** → Contradictory dashboards; align snapshots or label staleness.
- **Confusing snapshot isolation with serializability.** → Write skew bites you (two transactions each validate then write different keys).
- **Using wall clocks for ordering across nodes.** → Clock skew reorders events; use Lamport/HLC/TrueTime.
- **Relying on Kafka for global ordering.** → Ordering is per partition; keying is the tool; global order needs one partition or a causal layer.
- **Treating `w:majority` as "linearizable".** → It's not (no real-time total order without read concern `linearizable`).
- **No consistency tests.** → Without fault injection, you never learn which model you actually have.
- **Ignoring the analytics snapshot.** → Numbers "right but jointly wrong" when inputs come from different moments.

---

## 7. Real-World Use Cases and Industry Applications

- **Google Spanner — strict serializability with TrueTime.** External consistency for global transactions; reads at explicit timestamps (including stale reads for cheap replicas). The reference implementation of "strong consistency at scale, if you pay".
- **CockroachDB / Yugabyte — serializable by default, follower reads via closed timestamps.** Shows how to keep serializability while making reads fast and local.
- **MongoDB — causal consistency sessions.** The `causalConsistency` session flag plus `readConcern`/`writeConcern` gives application-level session guarantees without a global consensus read for every query. A widely copied pattern.
- **Facebook/Meta — read-your-writes in the social graph.** TAO and friends route a user's reads to a cache/replica fresh enough for that user, precisely the session-token pattern.
- **Amazon DynamoDB — per-read consistency choice, and `TransactWriteItems` for atomic multi-item writes.** Blends AP by default with opt-in transactions.
- **Cassandra at Netflix/Apple/Discord — tunable per query.** `LOCAL_QUORUM` for correctness-sensitive reads, `ONE` for background analytics.
- **Kafka at LinkedIn — per-key ordering + transactions.** The log's total order *per partition* is the consistency model; everything downstream inherits it.
- **Delta Lake / Iceberg — snapshot isolation for lakehouse writers.** Optimistic concurrency; conflicts retried; readers see a consistent table version.
- **Snowflake — read-after-write within a session, snapshot isolation for concurrent DML.** Analytics engineers rely on it implicitly all day.

---

## 8. Senior-Level Deep Dive

### 8.1 Serializability vs linearizability (a table you should memorize)

| | **Linearizability** | **Serializability** |
|---|---|---|
| Unit | Single operation (read/write of one object) | Transaction (multiple operations) |
| Guarantee | Total order matches real time | Some serial order equivalent |
| Multi-object? | Not necessarily | Yes |
| Real-time? | Yes | No |
| Typical use | Registers, KV, leader election | Transactions, databases |
| Example systems | etcd, ZooKeeper | PostgreSQL `SERIALIZABLE`, SQL Server |
| Both together | **Strict serializability** — Spanner, CockroachDB | |

### 8.2 The model-selection decision tree

```
Does the user need to see their own write immediately?
   no  ──▶ eventual / bounded staleness (cheapest)
   yes ──▶ Does anyone else need to see it, or does it need to respect real time?
             no (only the same user)  ──▶ read-your-writes (session token) ✅ cheap
             yes ──▶ Do reads need to agree with real time globally?
                        no  ──▶ causal consistency (HLC / vector clocks)
                        yes ──▶ Is the operation only one key?
                                   yes ──▶ linearizable (single leader / consensus)
                                   no  ──▶ strict serializable (Spanner / Cockroach)
```

### 8.3 The latency cost of each rung (order-of-magnitude, same region)

| Model | Added latency vs local read | Availability under partition |
|---|---|---|
| Eventual | 0 (local) | Full |
| Session (RYW/monotonic) | +0–2 ms (token check) | Full |
| Bounded staleness | +1–5 ms | Full (bounded) |
| Causal | +2–10 ms (dependency tracking) | Full |
| Sequential | +5–20 ms (log ordering) | Majority side only |
| Linearizable | +5–20 ms (quorum round) | Majority side only |
| Strict serializable | +10–20 ms local, +100s cross-region | Majority side only |

These are shapes, not laws: the point is that the jump from **causal → linearizable** is where you start paying availability, and the jump to **cross-region strict** is where you pay latency on every write.

### 8.4 Consistency in the modern data stack (the part people skip)

```
Source (OLTP)          → CDC (Kafka)              → Lakehouse (Delta/Iceberg)
  linearizable writes      per-partition order        snapshot isolation commits
                                                      + idempotent merges

Consistency requirements flow DOWN the pipeline:

1. Bronze faithfulness: the CDC log must preserve per-key order (same partition).
2. Silver correctness: merges must be idempotent and use LSN/version, not wall time.
3. Gold reproducibility: reads must target one snapshot; materialized views create
   explicit bounded staleness.

Failure to align models shows up as:
  - "The fact table references a dimension key that didn't exist yet" (causal violation)
  - "Re-running the job yields different numbers" (no snapshot isolation)
  - "The dashboard and the API disagree" (two different consistency models)
```

### 8.5 Why "session guarantees" deserve to be first-class

Session guarantees cover the vast majority of user-visible expectations at essentially zero cost, because they are enforced *client-side with a token*. The typical architecture:

```
        ┌────────────┐  write (gets token T)
user ──▶│  API       │──────────────────────────▶ leader
        │  (session) │
        └─────┬──────┘  read (sends token T)
              │
              ▼
      replica if applied_version >= T   ── yes ──▶ serve (fast, correct)
                                        ── no  ──▶ forward to leader
```

The leader is only touched in the narrow window right after a write. You get RYW + monotonic reads + full read scalability. **This is what most "strongly consistent" user-facing systems actually do.**

### 8.6 Failure modes and their signatures

| Symptom | Violated model | Likely cause | Fix |
|---|---|---|---|
| "I posted, then it vanished, then reappeared" | Monotonic reads | Reads bouncing between replicas of different freshness | Sticky replica or version gate |
| "My comment shows before the post" | Causal | Events applied out of dependency order | Causal tokens / per-entity keying |
| "Balance went negative" | Linearizable / serializable | Stale read then write (no conflict check) | Consensus transaction or `SELECT FOR UPDATE` |
| "Two dashboards disagree" | Snapshot alignment | Inputs read at different times | Single snapshot / fixed window |
| "Re-running the job changes results" | Snapshot isolation | Reading live data mid-job | Read one version (`VERSION AS OF`) |
| "Conflicting writes lost one update" | Conflict resolution | LWW with unsynced clocks | Version vectors / CRDT (ch. 5) |
| "Reads are slow only in write bursts" | Linearizable | Quorum round-trip under contention | Session tokens or closed timestamps |

### 8.7 Debugging consistency bugs: a runbook

1. **Name the model you intended.** If you can't, that's the first bug.
2. **Reproduce with a small history.** Build the minimal interleaving (write → lag → read) and check which model it violates.
3. **Instrument versions.** Log `applied_version`/LSN/timestamp on every read path; a mismatch is immediately visible.
4. **Check routing.** Which replica served the read, and how fresh was it? Random load balancing is the #1 cause of monotonic-read violations.
5. **Check clock usage.** Any ordering by wall clock across nodes is suspect; look for LWW and `updated_at` comparisons.
6. **Check the snapshot.** For analytics, confirm all inputs came from one version/timestamp.
7. **Write a regression test.** Encode the invariant as a property test with injected lag; run it in CI.

---

## 9. Common Mistakes and How to Avoid Them

1. **Using "consistency" without a model name.** → Always specify: linearizable, sequential, causal, RYW, monotonic, snapshot, eventual.
2. **Over-applying linearizability.** → Start from the weakest model that meets the invariant; climb only as needed.
3. **Random read routing with user-visible writes.** → Sticky routing / version tokens.
4. **Confusing snapshot isolation with serializability.** → Detect cross-key conflicts explicitly where invariants span rows.
5. **Ordering by wall clock across nodes.** → Lamport/HLC/TrueTime.
6. **Assuming Kafka gives global order.** → Per-partition order; key for causality.
7. **Ignoring causal dependencies in CDC.** → Encode dependencies; buffer until parents apply.
8. **Reading different sources at different snapshots in a join.** → Align versions or state the staleness.
9. **Treating `w:majority` as linearizable.** → Add read concern `linearizable` if you truly need it.
10. **No consistency tests or fault injection.** → You don't know your model; you only know your hope.
11. **Silent fallbacks to weaker models.** → If a code path degrades to stale reads, log and alarm it.
12. **Not documenting the model per table/endpoint.** → New engineers reintroduce the bugs you fixed.

---

## 10. Exercises / Interview Questions

> Hints only — no full answers inline.

1. **Define.** Contrast linearizability, sequential consistency, and causal consistency with one interleaving each. *(Hint: real-time order vs agreed order vs dependency order.)*
2. **Transactions.** Give a history that is serializable but not linearizable, and one that is linearizable per-object but not serializable. *(Hint: multi-object transactions vs real time.)*
3. **RYW.** Implement read-your-writes for a mobile app with replicas in three regions. What does the client carry? *(Hint: version/LSN token; route to a fresh-enough replica.)*
4. **Monotonic reads.** A user's feed randomly shows old and new posts. Diagnose and fix. *(Hint: load-balanced reads across lagging replicas; sticky routing or version gate.)*
5. **Causal.** Blog comments sometimes appear before the post. Design a fix in a CDC pipeline. *(Hint: causal_parents set; buffer until applied.)*
6. **Vector vs Lamport.** When do you need vector clocks instead of Lamport clocks, and what do they cost? *(Hint: detecting concurrency vs determining causality; O(N) metadata.)*
7. **HLC.** Why do CockroachDB/MongoDB use hybrid logical clocks instead of pure logical or pure physical time? *(Hint: causality + approximate real time in O(1).)*
8. **Snapshot.** Two pipelines join a fact table to a dimension table and get "impossible" counts. What consistency model is violated and how do you fix it? *(Hint: different snapshots; read one version.)*
9. **Bounded staleness.** Specify and test "reads are at most 5 seconds old". *(Hint: timestamp bounds; per-read staleness check; lag injection tests.)*
10. **Closed timestamps.** Explain how follower reads can be both fast and non-stale-as-of-T. *(Hint: a shard advertises all writes ≤ T are final.)*
11. **Write skew.** Give a snapshot-isolation anomaly and a fix that keeps snapshot isolation. *(Hint: two transactions validate then write different keys; materialize the conflict.)*
12. **Model mapping.** For a bank (ledger), a social feed, and a metrics warehouse, pick the minimum sufficient model and justify each. *(Hint: strict serializable; RYW/monotonic/causal; snapshot + bounded staleness.)*

---

## References

- Kleppmann, M. — *Designing Data-Intensive Applications*, Chapter 9: "Consistency and Consensus". https://dataintensive.net/
- Viotti, P. & Vukolić, M. — "Consistency in Non-Transactional Distributed Storage Systems" (ACM CSUR 2016): https://arxiv.org/abs/1512.00168
- Lamport, L. — "Time, Clocks, and the Ordering of Events in a Distributed System" (1978): https://lamport.azurewebsites.net/pubs/time-clocks.pdf
- Attiya, H., Bar-Noy, A., Dolev, D. — "Sharing Memory Robustly in Message-Passing Systems" (quorum systems): https://dl.acm.org/doi/10.1145/200836.200869
- Terry, D. — "Replicated Data Consistency Explained Through Baseball" (session/consistency intuition): https://www.microsoft.com/en-us/research/publication/replicated-data-consistency-explained-through-baseball/
- Google Cloud — Spanner TrueTime and external consistency: https://docs.cloud.google.com/spanner/docs/true-time-external-consistency
- Google Cloud — Spanner stale reads / timestamps: https://cloud.google.com/spanner/docs/timestamp-bounds
- MongoDB Documentation — Causal consistency and read/write concerns: https://www.mongodb.com/docs/manual/core/read-isolation-consistency-recency/
- MongoDB Documentation — Read Concern "linearizable": https://www.mongodb.com/docs/manual/reference/read-concern-linearizable/
- CockroachDB — Consistency model and closed timestamps: https://www.cockroachlabs.com/docs/stable/architecture/transaction-layer
- Apache Cassandra — Tunable consistency: https://docs.datastax.com/en/cassandra-oss/3.0/cassandra/dml/dmlConfigConsistency.html
- Berenson et al. — "A Critique of ANSI SQL Isolation Levels" (write skew, snapshot isolation): https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/tr-95-51.pdf
- Herlihy, M. & Wing, J. — "Linearizability: A Correctness Condition for Concurrent Objects" (1990): https://dl.acm.org/doi/10.1145/78969.78972
- Jepsen — Consistency models and analyses: https://jepsen.io/consistency
- Bailis, P. — "Linearizability versus Serializability": http://www.bailis.org/blog/linearizability-versus-serializability/
