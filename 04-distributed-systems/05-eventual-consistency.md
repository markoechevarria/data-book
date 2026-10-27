# Eventual Consistency

> Eventual consistency is the guarantee that if you stop writing, all replicas will *converge* to the same value — but it says nothing about *when* or *what order* they get there. It is the price of high availability (`03-cap-theorem.md`) and the reason your shopping cart, likes, presence indicators, and collaborative document don't lose data when a region goes dark. The hard part is not the word "eventual"; it is the word "consistency": *how* do two replicas that accepted conflicting writes decide on one value? This chapter covers the conflict-resolution toolbox — last-write-wins, version vectors, vector clocks, and CRDTs (the mathematically guaranteed version) — plus the repair machinery (read repair, hinted handoff, Merkle-tree anti-entropy, gossip) that closes the gap after the writes stop. It also maps all of this onto the tools you use daily: Cassandra, DynamoDB, Cosmos DB, Kafka, and the lakehouse merge layer.

## Why a Data Engineer Cares

- **Your multi-region ingestion is eventually consistent.** Kafka cross-region replication is asynchronous; S3/GCS replication is asynchronous; Global Tables / Cosmos multi-region writes are asynchronous by design. If you don't design for convergence, you get duplicates, lost updates, and resurrected deletes.
- **A lakehouse MERGE is a conflict-resolution policy in disguise.** Every `MERGE INTO` with a window function is "pick a winner" — the same problem as LWW, and the same traps.
- **CDC pipelines are eventual.** Two source changes written milliseconds apart can arrive out of order; you must be idempotent and order-tolerant.
- **Deletes are the hardest case** (tombstones, re-appearing rows) and show up as "we deleted a customer and their orders came back" after a backfill.
- **Counters and aggregates must be merge-safe.** A `sum()` recomputed from divergent partial sums is wrong; a CRDT count is right.

Eventual consistency is where distributed-systems theory becomes your daily merge logic.

## TL;DR

- **Eventual consistency = convergence**: once writes stop, all replicas reach the same state. No timing or ordering guarantee during the window.
- **Strong eventual consistency (SEC)** = eventual consistency + *same set of updates → same state, deterministically, regardless of order/duplication*. This is what CRDTs provide.
- **Conflict resolution strategies, weakest to strongest:**
  1. **Last-write-wins (LWW)** — pick by timestamp. Simple, lossy, exposes clock skew.
  2. **LWW with hybrid logical clocks** — same idea, causality-aware timestamps.
  3. **Version vectors / vector clocks** — detect concurrency; push the choice to the application.
  4. **CRDTs** — data types that merge deterministically and always (counters, sets, registers, maps, sequences).
  5. **Application-specific resolvers** — domain logic decides (merge carts, sum likes, union sets).
- **Repair machinery:** **read repair** (fix on read), **hinted handoff** (park writes for absent nodes), **anti-entropy with Merkle trees** (background comparison), **gossip** (propagate membership and state).
- **CRDT types you'll actually use:** `G-Counter`, `PN-Counter`, `LWW-Register`, `MV-Register`, `G-Set`, `OR-Set` (add/remove), `LWW-Map`, RGA/sequence CRDTs for text.
- **Costs:** metadata growth (vector clocks are O(N)), tombstones (delete markers that must be garbage-collected), and the operational complexity of running repair.
- **Rule:** don't use eventual consistency where correctness is non-negotiable (money, uniqueness); where you use it, *specify the resolution rule and the convergence mean-time*.

---

## 1. Definitions and Core Vocabulary

| Term | Meaning |
|---|---|
| **Convergence** | All replicas reach the same state after writes stop. |
| **Strong eventual consistency (SEC)** | Convergence for any delivery order/duplication: replicas that received the same set of updates have the same state. |
| **Conflict** | Two concurrent (causally independent) updates to the same logical datum. |
| **Last-write-wins (LWW)** | Resolve conflicts by a timestamp; the "newest" wins, the rest are discarded. |
| **Version vector / vector clock** | Per-replica counter vector used to detect causality vs concurrency between versions. |
| **CRDT** | Conflict-free Replicated Data Type: a type whose merge is commutative, associative, and idempotent → guaranteed convergence. |
| **State-based CRDT (CvRDT)** | Replicas exchange full/partial states; merge = join (least upper bound) on a semilattice. |
| **Operation-based CRDT (CmRDT)** | Replicas exchange operations; requires causal (or exactly-once) delivery. |
| **Delta-state CRDT** | State-based but ships only the delta since last sync (bandwidth-efficient → used in practice). |
| **Read repair** | On read, detect divergent replicas and push the winning version back. |
| **Anti-entropy** | Background process comparing replicas to repair divergence not caught by read repair. |
| **Merkle tree** | Hash tree over key ranges for cheap divergence detection (compare roots, descend on mismatch). |
| **Hinted handoff** | A neighbor stores a write destined for an unreachable node and forwards it when it returns. |
| **Sloppy quorum** | Accept writes on the first N healthy nodes, even if not the home replicas. |
| **Tombstone** | A marker recording a deletion so the delete propagates and doesn't get resurrected. |
| **Gossip protocol** | Epidemic-style peer-to-peer state propagation (membership, versions, health). |
| **Operational transformation (OT)** | Alternative to CRDTs for collaborative editing; needs a central server to transform ops. |
| **Semilattice** | A set with a commutative, associative, idempotent join operation; the algebraic basis of state-based CRDTs. |
| **Divergence** | The state where replicas hold different values for the same key. |

> **Senior note:** "Eventually consistent" and "converged" are not the same as "correct". A system that converges to *a* value may converge to the *wrong* value if your merge rule is lossy (LWW). Convergence is a *liveness* property; correctness is your resolution policy's job.

---

## 2. What Problem It Solves / Why It Matters

Eventual consistency is what you get when you refuse to make availability contingent on coordination:

| Requirement | Why eventual consistency fits |
|---|---|
| "The cart must accept adds even during a partition" | Both sides accept writes; merge later |
| "Likes/clicks must never be rejected" | Counters merge associatively |
| "Active-active multi-region writes" | Local writes, async replication, CRDT merge |
| "Offline-first mobile clients" | Clients write locally, sync later |
| "Presence indicators" | TTL-based LWW is fine; nobody needs linearizable presence |
| "Cheap cross-region reads" | Replicas answer locally, reconcile in the background |

**The cost:** two replicas can disagree, users can see different values, and you must (a) guarantee convergence and (b) decide what "the right answer" means when two writes race. The entire rest of this chapter is about (a) and (b).

**Why "eventually" is a real guarantee and not a cop-out:** under a partition, you *cannot* have linearizability and availability simultaneously (ch. 3). Eventual consistency is the only model that keeps writes flowing, so for a large class of workloads it is not a compromise — it is the correct engineering choice.

---

## 3. How It Works Internally (Mechanisms, Internals)

### 3.1 What convergence requires

For two replicas that received the same set of updates (in any order, possibly with duplicates) to reach the same state, the merge function `⊔` (join) must satisfy:

```
Commutative:  a ⊔ b = b ⊔ a
Associative:  (a ⊔ b) ⊔ c = a ⊔ (b ⊔ c)
Idempotent:   a ⊔ a = a
```

These three properties define a **join-semilattice**. If your data type's merge obeys them, you get Strong Eventual Consistency for free — no consensus, no ordering, no coordination.

```
  a ──┐
      ├──▶ a ⊔ b  ──▶  a ⊔ b ⊔ c        (same result no matter the order or duplicates)
  b ──┘                 ▲
  c ────────────────────┘
```

**This is the entire CRDT idea.** Everything else is designing useful types whose merge has these properties.

### 3.2 Conflict detection: version vectors

```
Replica A: v = {A:2, B:1}
Replica B: v = {A:2, B:3}

Compare componentwise:
  {A:2,B:1} < {A:2,B:3} because B-A < B-B and A is equal
  ⇒ A's version is dominated by B's ⇒ B is newer (no conflict)
     winner = the dominating version.

Now:
  Replica A: v = {A:3, B:1}
  Replica B: v = {A:2, B:3}
  Neither dominates (A has more of A, B has more of B) ⇒ CONCURRENT
  ⇒ a real conflict; hand both versions to the application (or a CRDT/rule).
```

Vector clocks are conceptually identical (they track events, not just versions). The practical cost: **O(N) per item** where N is the number of replicas. Systems compress this (per-DC summaries, "dotted" version vectors, HLCs) because per-key vectors don't scale to hundreds of nodes.

> **Senior note:** Dynamo's original vector-clock approach pushed conflicts to *applications*, and most applications did the worst possible thing: `LWW` by wall clock. Modern systems (Cassandra, DynamoDB) mostly dropped vector clocks in favor of timestamps (LWW) precisely because apps mishandled conflicts — trading correctness for predictability. Know which trade your system makes.

### 3.3 Last-write-wins (LWW): simple, lossy, clock-sensitive

```
Replica A at 12:00:00.100 writes name="Ana"   (clock fast by 300 ms)
Replica B at 12:00:00.200 writes name="Anna"  (clock correct)
Wall-clock LWW: B wins ("Anna").  But A's write was causally LATER
(issued after seeing B's value). If A's clock is fast, LWW may have picked
the wrong winner and DIVERGED FROM INTENT while still converging to a value.

⇒ LWW converges (any total order on timestamps works) but can silently drop
  a causally newer update due to clock skew.
```

Improvements:
- **Hybrid logical clocks (HLC):** timestamps that respect causality and stay close to wall time → LWW becomes causality-respecting (CockroachDB, MongoDB cluster time).
- **Server-assigned timestamps:** a single authority stamps writes (or a per-region authority) to reduce skew.
- **Tie-break deterministically:** `(timestamp, replica_id, value_hash)` to ensure convergence even on equal timestamps.

```
LWW rule (convergent):
  key: pick the (ts, node_id, value) tuple that is greatest lexicographically
  ⇒ total order ⇒ all replicas agree ⇒ convergence guaranteed.
```

### 3.4 CRDT catalog (what to use for what)

**Counters**
```
G-Counter (grow-only):  vector of per-replica counts. merge = componentwise max.
  value = sum(vector).  +1 on replica i: vector[i] += 1.
  For "likes", "views": never negative → G-Counter is perfect.

PN-Counter:  two G-Counters, P (increments) and N (decrements).
  value = sum(P) - sum(N).   Handles negative counts.
```

```python
class GCounter:
    def __init__(self, replica_id, n_replicas):
        self.id, self.state = replica_id, [0] * n_replicas
    def increment(self, by=1):
        self.state[self.id] += by
    def merge(self, other):                          # commutative, assoc, idempotent
        self.state = [max(a, b) for a, b in zip(self.state, other.state)]
    def value(self):
        return sum(self.state)

class PNCounter:
    def __init__(self, rid, n):
        self.p, self.n = GCounter(rid, n), GCounter(rid, n)
    def increment(self, by=1): self.p.increment(by)
    def decrement(self, by=1): self.n.increment(by)
    def merge(self, o): self.p.merge(o.p); self.n.merge(o.n)
    def value(self): return self.p.value() - self.n.value()
```

**Registers**
```
LWW-Register:  one value + a timestamp; merge = newer wins. (Convergent, lossy.)
MV-Register:   keeps ALL concurrent values from a version vector; merge = union of
               concurrent values, untangling any that are dominated.
               Forces the application to observe conflicts instead of losing them.
```

**Sets**
```
G-Set (grow-only): merge = union. Simple, can't remove.
2P-Set: add-set (G-Set) + remove-set (G-Set) + "removed" flag.
        Elements can be added then removed ONCE (a removed element can't be re-added).
OR-Set (Observed-Remove, "AWSet"): each add carries a unique tag; remove removes the
        tags OBSERVED so far. Concurrent add+remove → add wins (a "resurrection"
        that is actually correct: the add had not been observed by the remover).
LWW-Element-Set: each element has an add and a remove timestamp; LWW per element.
```

**Maps and sequences**
```
LWW-Map: a map whose keys are LWW registers (e.g., a user profile document).
OR-Map:  a map whose keys can be added/removed with OR-Set semantics.

Sequence CRDTs (for text): RGA, Yjs/YATA, Logoot, Treedoc.
  Collaborative editing without a central authority. Used by Yjs, Automerge,
  and (in a variant) Figma's multiplayer engine.
```

### 3.5 Repair machinery: how divergence actually gets fixed

```
Three layers, from fast/small to slow/comprehensive:

1. READ REPAIR (on the read path)
   Coordinator reads from R replicas; if they disagree, return the winning value
   AND write it back to the stale replicas.
   + Latency: the repair is piggybacked on a read you were doing anyway.
   − Only fixes data that is READ. Cold keys stay divergent.

2. HINTED HANDOFF (on the write path)
   If a home replica is unreachable, a neighbor stores a "hint" (the write +
   target) and delivers it when the node returns.
   + Keeps writes available and eventually delivered.
   − Hints expire; if the node stays down too long, the hint is dropped and you
     rely on anti-entropy.

3. ANTI-ENTROPY (background)
   Periodically compare replicas over key ranges and reconcile ALL differences.
   + Catches cold data and dropped hints. The only layer that guarantees eventual
     convergence for keys nobody reads.
   − Expensive; schedule it (Cassandra: nodetool repair; needs to run within
     gc_grace_seconds to avoid resurrecting deleted data).
```

```
Merkle-tree anti-entropy:

   Replica A                 Replica B
   root:A_hash              root:B_hash   → equal? done.
        |                         |
   compare level by level; descend only into subtrees whose hashes differ.
   At the leaves, exchange the actual values and merge.
   O(log N) work when data mostly agrees — the reason it's feasible at scale.
```

### 3.6 Gossip, membership, and failure detection

```
Cassandra/Dynamo use gossip to:
  - disseminate membership and topology
  - spread version/health information
  - detect failures (phi-accrual failure detector)

Every second, each node exchanges state with a few random peers; information
spreads in O(log N) rounds. No central coordinator → no SPOF.
```

### 3.7 Deletes, tombstones, and the resurrection problem

```
Delete is a WRITE (a tombstone), because "absence" is not a state you can replicate.

  t0: node A has key k=1
  t1: node B (partitioned) still has k=1
  t2: delete k on A → tombstone with timestamp T_del
  t3: partition heals; B's value k=1 has timestamp T_val < T_del → tombstone wins ✅

Danger:
  - If you GC the tombstone BEFORE all replicas have seen it, a late replica
    can re-introduce k=1 → "the delete was resurrected".
  - Cassandra guards this with gc_grace_seconds (default 10 days) and requires
    repair within that window.
  - Backfills in a lakehouse have the same hazard: re-inserting an old row after
    a delete unless the delete is represented as a tombstone/higher version.
```

---

## 4. How to Implement It (Step-by-Step, with Code / Config)

### 4.1 Choose the resolution strategy

```
1. Is the datum commutative/mergeable by nature?
     counter, set, union, max  ──▶ use a CRDT. Done.
2. Is a single scalar value with "latest wins" acceptable?
     profile field, status, last_seen ──▶ LWW (prefer HLC timestamps).
3. Must concurrent values be preserved for a human/app decision?
     ──▶ MV-Register / conflict surfacing (merge UI, review queue).
4. Does the domain have a merge rule?
     shopping cart → union of items (OR-Set / item-wise LWW with max quantity)
     ──▶ application-specific merge (a "custom CRDT").
```

### 4.2 Cassandra: last-write-wins by cell timestamp

```sql
-- Cassandra resolves conflicts per CELL using a server-assigned timestamp (µs).
CREATE TABLE user_profile (
  user_id uuid PRIMARY KEY,
  name text,
  updated_at timestamp
);
-- You can explicitly set the timestamp (useful for idempotent replay / CDC):
INSERT INTO user_profile (user_id, name) VALUES (?, ?) USING TIMESTAMP 1735689600000000;
```

```
Implications:
  + Simple, fast, convergent.
  − Two concurrent writes → highest timestamp wins, the other is LOST silently.
  − Clock skew across nodes affects the winner.
  − If you re-insert an old CDC event with an old timestamp, it's ignored ✅
    (this is why replaying CDC with source timestamps is idempotent).
```

### 4.3 Read repair and consistency levels

```sql
-- Cassandra performs read repair automatically based on the read CL:
CONSISTENCY QUORUM;  SELECT * FROM user_profile WHERE user_id = ?;
-- The coordinator compares replicas, returns the newest, and repairs stale ones.
```

```python
# Manual read-repair-style merge in application code (Dynamo-style)
def read_with_repair(replicas, key, r):
    versions = [(rep.get(key), rep) for rep in replicas]
    live = [(v, rep) for v, rep in versions if v is not None]
    if len(live) < r:
        raise RuntimeError("read quorum not met")
    winner = max(live, key=lambda vr: (vr[0].ts, vr[0].node_id))
    for v, rep in live:
        if (v.ts, v.node_id) < (winner.ts, winner.node_id):
            rep.put(key, winner)            # repair stale replica
    return winner
```

### 4.4 Anti-entropy / Merkle-tree delta sync

```python
import hashlib

def merkle_root(d, depth=0, max_depth=8):
    keys = sorted(d)
    if not keys or depth == max_depth:
        return hashlib.sha256(repr({k: d[k] for k in keys}).encode()).hexdigest()
    mid = len(keys) // 2
    left = merkle_root({k: d[k] for k in keys[:mid]}, depth + 1, max_depth)
    right = merkle_root({k: d[k] for k in keys[mid:]}, depth + 1, max_depth)
    return hashlib.sha256((left + right).encode()).hexdigest()

def reconcile(a, b):
    # Compare roots; if equal, nothing to do. Otherwise, descend into the ranges
    # whose hashes differ and merge the actual keys (naive version here):
    if merkle_root(a) == merkle_root(b):
        return {}
    diffs = {}
    for k in set(a) | set(b):
        if a.get(k) != b.get(k):
            diffs[k] = max([v for v in (a.get(k), b.get(k)) if v is not None],
                           key=lambda v: v["ts"])
    return diffs     # apply diffs to both sides
```

### 4.5 Lakehouse merge: the DE's version of conflict resolution

```sql
-- Delta Lake: idempotent, order-tolerant MERGE using a version/source-of-truth key
MERGE INTO silver.customers AS tgt
USING (
  -- one row per customer, newest by (logical_version, updated_at, op)
  SELECT * EXCEPT(rn) FROM (
    SELECT *, ROW_NUMBER() OVER (
      PARTITION BY customer_id
      ORDER BY log_version DESC, updated_at DESC,
               CASE WHEN op = 'delete' THEN 1 ELSE 0 END DESC
    ) AS rn
    FROM staging.customer_events
  ) WHERE rn = 1
) AS src
ON tgt.customer_id = src.customer_id
    AND tgt.log_version <= src.log_version          -- ← LWW guarded by version
WHEN MATCHED AND src.op = 'delete' THEN DELETE
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED AND src.op <> 'delete' THEN INSERT *;
```

```
Key DE patterns:
  - Version/sequence column is the LWW key, NOT wall-clock time.
  - Deletes are events (tombstones), not "row disappeared".
  - The MERGE is idempotent: re-running with the same events is a no-op.
  - Full history for late-arriving events (a bigger window of events at the source).
```

### 4.6 Kafka: making an eventual pipeline convergent

```properties
# Exactly-once-ish producer + transactional writes (dedup on producer retries)
enable.idempotence = true
transactional.id = cdc-writer-1
```

```python
# Consumer: dedupe + upsert by (key, version) instead of append
def apply(events, table):
    # events may arrive out of order or duplicated
    for e in dedupe_by(events, key=lambda e: (e["entity_id"], e["version"])):
        if e["version"] > table.get_version(e["entity_id"]):
            table.upsert(e["entity_id"], e["payload"], e["version"])
```

```
Convergence for streaming pipelines:
  1. Dedupe by a stable event id (or by (key, version)).
  2. Apply with a monotonic guard (`version > current`), never blind overwrite.
  3. Represent deletes explicitly.
  4. Make the sink idempotent (MERGE/upsert, not append).
  Then: duplicate delivery, reordering, and replay all converge to the same state.
```

### 4.7 Implementing a CRDT-backed feature (worked example: "likes")

```python
# Multi-region likes with no coordination.
# Each region keeps a G-Counter per post: {region: count}.
# Merge is componentwise max; value is the sum.

class LikesStore:
    def __init__(self):
        self.counters = {}      # post_id -> {region: count}

    def like(self, post_id, region):
        self.counters.setdefault(post_id, {})
        self.counters[post_id][region] = self.counters[post_id].get(region, 0) + 1

    def merge(self, other):
        for post, vec in other.counters.items():
            mine = self.counters.setdefault(post, {})
            for region, c in vec.items():
                mine[region] = max(mine.get(region, 0), c)     # join
    def total(self, post_id):
        return sum(self.counters.get(post_id, {}).values())
```

Why this is right: `max`+`sum` is commutative, associative, idempotent. Duplicate syncs are harmless. Two regions liking simultaneously both count. No locks, no coordination, always available.

**What you cannot do this way:** enforce "at most one like per user" — that is a *set membership* constraint, so model it as an OR-Set of `(user_id)` and count the set, not a counter.

---

## 5. Where to Implement It (Systems and When Each Fits)

| System | Eventual-consistency mechanism | When to choose |
|---|---|---|
| **Apache Cassandra** | LWW per cell (server timestamps), read repair, hinted handoff, anti-entropy, gossip | Write-heavy, multi-region, AP workloads |
| **Amazon DynamoDB** | Global tables (async multi-region), eventually consistent reads; `TransactWriteItems` for atomic multi-item | Serverless KV; active-active |
| **Azure Cosmos DB** | 5 levels (Eventual, Consistent Prefix, Session, Bounded Staleness, Strong); multi-region writes | Multi-region app data with a tunable knob |
| **Riak** | Dotted version vectors, CRDTs (counters, sets, maps, registers) built-in | True AP, CRDT-first |
| **Redis (Enterprise / CRDT module)** | Active-active geo with CRDT types | Caches/state that must merge across regions |
| **Automerge / Yjs** | CRDT libraries for JSON/text docs | Local-first apps, collaborative editing |
| **CouchDB / PouchDB** | MVCC + deterministic revision resolution (app-level, historically) | Offline-first sync |
| **MongoDB** | Async replica sets, `readPreference` secondary, causal sessions | Documents; use sessions for RYW |
| **Kafka → lakehouse** | Async replication + idempotent MERGE with version guard | Streaming into analytical stores |
| **Delta Lake / Iceberg** | Optimistic concurrency; snapshot isolation; MERGE semantics | Lakehouse convergence layer |
| **S3 / GCS cross-region replication** | Asynchronous object replication (RTC for S3) | DR and multi-region data access |
| **DNS** | TTL-based eventual propagation | Global name resolution |

**Choosing guide (short):**
- **Counters/likes/presence/views** → CRDT counters (or DynamoDB atomic counters with merge-by-max across regions).
- **Sets with add/remove** → OR-Set (Riak, or implement; or an LWW-per-element set).
- **Documents/profiles that race** → LWW with HLC timestamps, or MV-Register + conflict UI.
- **Carts/collaborative state** → domain-specific merge (union + max rules).
- **Text/collaborative editing** → sequence CRDT (Yjs/Automerge) or OT with a server.
- **Analytics** → idempotent MERGE with version guard + tombstone events.

---

## 6. Patterns and Anti-Patterns

### Patterns

- **Model the datum as the merge-able type it wants to be.** Counter → counter; set → set; profile → LWW register.
- **Use version/sequence numbers, not wall clocks.** Guard every write with `if new_version > current_version`.
- **Make every write idempotent.** Idempotency is the practical guarantee that makes eventual consistency safe.
- **Represent deletes explicitly** (tombstones or `op='delete'` events) and GC them only after full propagation.
- **Run anti-entropy on a schedule** and monitor how much it repairs (a rising trend = a real problem).
- **Surface conflicts when they matter.** MV-Register + a merge/review UI is honest; silent LWW is not.
- **Bound the convergence window** and alert on it ("p99 convergence ≤ 60 s").
- **Merge in the sink, not in the source.** Keep the log faithful; converge in the serving/analytical layer.
- **Design the conflict rule before launch**, and write it in the data contract.

### Anti-Patterns

- **Blind LWW with skewed clocks** on data where the "wrong" winner loses money. → HLC or version numbers, or a stronger model.
- **Assuming "eventually" means "in a few milliseconds".** → Measure; set and monitor a convergence SLO.
- **Using a plain counter across regions with independent increments.** → Use a G-Counter/PN-Counter; a last-writer-wins counter loses increments.
- **Resurrecting deleted rows via backfill.** → Backfills must respect tombstone versions or target a range that excludes deleted keys.
- **GC-ing tombstones too early.** → Honor `gc_grace_seconds`; run repair within the window.
- **Never running repair.** → Cold data stays divergent forever; the "eventual" in eventual consistency silently becomes "never".
- **Reconciling in application code ad hoc.** → Inconsistent rules per service → data that converges differently in each consumer.
- **Append-only sinks for mutable data.** → Duplicates and stale rows accumulate; use MERGE/upsert.
- **Treating read repair as sufficient.** → It only fixes what is read; you still need anti-entropy.
- **Using eventual consistency for uniqueness/invariants.** → "Unique usernames" needs coordination, not merge.
- **Mixing CRDTs and LWW on the same field.** → The rules fight; pick one per datum.

---

## 7. Real-World Use Cases and Industry Applications

- **Amazon Dynamo (2007) — the origin.** Shopping carts: always allow "add to cart", merge conflicts with vector clocks + application resolution. The design that seeded a generation of AP databases.
- **Riak — CRDT-first.** Ships G-Counter, PN-Counter, OR-Set, LWW-Register, and maps as first-class types so applications never hand-roll merge logic.
- **Cassandra at Netflix/Apple/Discord — LWW everywhere.** Timestamp-per-cell resolution plus read repair and `nodetool repair`; operational discipline is what keeps it convergent.
- **DynamoDB Global Tables — active-active multi-region.** Asynchronous replication with LWW per item (and `TransactWriteItems` for atomic multi-item). Multi-region strong consistency is explicitly not offered.
- **Azure Cosmos DB — tunable eventual consistency.** Consistent Prefix and Eventual levels for multi-region writes; Session for read-your-writes; Strong for the cases that need it.
- **Figma / Google Docs — collaborative editing.** Sequence structures plus a server-authoritative model (Figma) or OT/CRDT (Docs, Yjs, Automerge) to merge concurrent edits.
- **Offline-first mobile (CouchDB/PouchDB, Realm, Automerge)** — clients write locally and sync; convergence is the entire product.
- **Data lakehouse ingestion — the DE case.** CDC events → idempotent MERGE into silver with a version guard; the "conflict resolution" is your merge policy, and it must be documented.
- **DNS — global eventual consistency at internet scale.** The system the world runs on tolerates staleness by design (TTL).

---

## 8. Senior-Level Deep Dive

### 8.1 LWW vs CRDT vs OT: when to use which

| Approach | Guarantees | Metadata cost | When to use |
|---|---|---|---|
| **LWW** | Converges; may lose concurrent writes | O(1) (timestamp) | Scalars where "latest wins" is genuinely OK (status, last_seen) |
| **Version vectors + app resolution** | Converges; surfaces conflicts | O(N) per item | Data where conflicts are rare but must not be lost silently |
| **CRDT (counter/set/register)** | Strong eventual consistency, never loses increments/ops | O(replicas) for counters; O(elements) for sets (with tombstones) | Counters, sets, presence, carts, maps |
| **CRDT (sequence: Yjs/RGA)** | Converges for text ordering | Grows with edit history (needs GC/compaction) | Collaborative editing, rich text |
| **OT with central server** | Converges; needs a server | O(1) client | When you accept a coordination point |

### 8.2 Metadata growth and garbage collection (the real operational cost)

```
A G-Set/OR-Set never forgets:
  - tombstones accumulate (one per removal)
  - each add carries a unique tag
  → state grows monotonically unless you GC.

Mitigations:
  - GC tombstones after a causal-stability window ("all replicas have seen it").
  - Stabilization: ask all replicas for their minimum version; anything dominated
    and older than that can be dropped.
  - Delta CRDTs: sync only deltas → less network, same GC problem.
  - Sequence CRDTs: periodic "snapshot + rewrite" (like a log compaction).

Rule of thumb: any CRDT that supports removal needs a GC/compaction story,
or its state grows forever.
```

### 8.3 Convergence vs correctness: the distinction that saves you

```
Convergence (liveness):  all replicas end up equal.  Guaranteed by CRDT algebra.
Correctness (safety):    the common value matches intent.  Your policy's job.

Example: LWW on a bank balance "converges" to one of the two racing values —
that's convergence but NOT correctness. Money needs linearizability, not LWW.

Example: G-Counter of likes "converges" AND is correct (each like counts once),
because the domain semantics are commutative.
```

**This is the senior judgment call:** match the algebra of your *domain* to the algebra of your *merge*. If the domain operation is not commutative, a commutative merge cannot preserve it — you need coordination (2PC/consensus) or a different data model.

### 8.4 Measuring and monitoring eventual consistency

```
Metrics that matter:
  1. Convergence lag:      time from last write to all replicas equal.
                           Track per-DC; alert on p99.
  2. Divergence count:     number of keys where replicas disagree (sample-based).
  3. Repair volume:        keys/bytes repaired by anti-entropy per run.
                           A rising trend = a systemic problem, not a hiccup.
  4. Conflict rate:        concurrent-write rate per key. High → redesign the key.
  5. Read-repair hit rate: how often reads find divergent data.
  6. Hint backlog:         hinted handoff queue depth and age.
  7. Tombstone count / GC: size and age of tombstones (GC health).

A single-number SLO like "99% of keys converge within 30 s" forces the whole
team to care about what "eventual" actually costs them.
```

### 8.5 Failure modes and their signatures

| Symptom | Likely cause | Fix |
|---|---|---|
| Deleted row reappears | Tombstone GC'd/expired before propagation; backfill re-inserted | Repair within `gc_grace_seconds`; version-guard backfills |
| Counter too low | Non-merge-safe counter overwritten by LWW | Use G/PN-Counter |
| "My update vanished" | LWW with skewed clock, or blind overwrite | HLC/version guard; MV-Register |
| Divergence never heals for cold keys | Anti-entropy not running or not covering the range | Schedule full repair; verify coverage |
| Convergence time spikes | Repair overload, hint queue backlog, network partition | Throttle repair; monitor hints; add capacity |
| Two regions disagree indefinitely | Ring/key ownership mismatch, blocked gossip port | Check topology/gossip connectivity |
| Edit cursor jumps in a doc | Sequence CRDT without proper causal context | Use a tested library (Yjs), not a hand-roll |
| Duplicate rows in the lake | Append-only sink + at-least-once delivery | Idempotent MERGE with version guard |

### 8.6 Debugging divergence: a runbook

1. **Prove there is divergence.** Compare checksums/Merkle roots per key range across replicas. Don't trust "it looked wrong once".
2. **Classify the divergence.** Same-timestamp different-value? Different-timestamp? Missing key? Each has a different cause.
3. **Check the timestamps.** Skew? Duplicate timestamps with no deterministic tie-break?
4. **Check delivery.** Are writes reaching all replicas at all (gossip/hints/replication lag)?
5. **Check repair.** Has anti-entropy run over this range in the last cycle? Is it inside the tombstone GC window?
6. **Check the application.** Is something writing without a version guard (blind overwrite)? Is a backfill resurrecting deleted rows?
7. **Fix forward, then prevent.** Repair the data; add the invariant as a monitor (conflict rate, divergence count) and a test.

### 8.7 Eventual consistency across the data stack

```
Kafka (async cross-region)  →  CDC consumers  →  Lakehouse MERGE  →  BI

Each hop introduces its own "eventual" window:
  - Kafka MirrorMaker: seconds of cross-region lag.
  - Consumer reprocessing: can emit duplicate/stale events.
  - MERGE: resolves by version.
  - BI caches/materialized views: bounded staleness.

Your end-to-end "freshness" is the SUM of these windows.
Design decision: make every hop idempotent and version-guarded so the total
result is convergent, then publish ONE end-to-end freshness SLO.
```

---

## 9. Common Mistakes and How to Avoid Them

1. **Using LWW for values where losing a write is unacceptable.** → Version vectors + app merge, or a stronger model.
2. **Trusting wall clocks for conflict resolution.** → HLCs or server-assigned, causality-respecting timestamps with deterministic tie-breaks.
3. **Non-merge-safe counters/aggregates.** → G-Counter / PN-Counter; sum per-region then merge.
4. **Forgetting tombstone lifecycle.** → Represent deletes, GC only after full propagation, and run repair inside `gc_grace_seconds`.
5. **Never running anti-entropy.** → Schedule repair; monitor repair volume; verify range coverage.
6. **Blind overwrites in the sink.** → Always guard with a version/sequence.
7. **No convergence SLO.** → Define and monitor "converged within X".
8. **Reconciling differently in each consumer.** → Put the conflict rule in the data contract; implement once.
9. **Hand-rolling a sequence CRDT for text.** → Use Yjs/Automerge; the edge cases are brutal.
10. **Assuming a managed service is "strongly consistent" because it's managed.** → Read the docs: DynamoDB default reads and GSIs are eventual; Global Tables are active-active LWW.
11. **Backfills that bypass the version guard.** → Backfills must write with true versions and respect tombstones.
12. **Ignoring metadata growth.** → Measure CRDT state/tombstone size; add GC; alert on unbounded growth.

---

## 10. Exercises / Interview Questions

> Hints only — no full answers inline.

1. **Algebra.** Show that `max` on integers is commutative, associative, and idempotent. Why does that make a G-Counter converge? *(Hint: join-semilattice; same updates → same state.)*
2. **LWW failure.** Construct a history where wall-clock LWW converges to the causally *older* value. What fixes it? *(Hint: clock skew; HLC or version numbers.)*
3. **Counter design.** Design a "views" counter that must be correct across three regions with no coordination. What type, and what is the merge function? *(Hint: G-Counter; componentwise max; sum.)*
4. **Sets with removal.** Why can't a 2P-Set re-add a removed element, and how does an OR-Set fix it? What's the cost? *(Hint: unique tags; tombstones.)*
5. **Conflict surfacing.** You must never silently lose a concurrent edit to a profile. Which register type, and what does the app do with it? *(Hint: MV-Register; merge UI / review queue.)*
6. **Shopify-style cart.** Design a multi-region cart merge: adds, removes, quantity changes. Which rules are commutative, and which need care? *(Hint: OR-Set for items; PN-Counter or max for quantity; deletion should win or LWW by item.)*
7. **Tombstones.** A customer was deleted, then a 6-month backfill re-created them. Explain why and give two fixes. *(Hint: tombstone GC window; version-guarded backfills; legal holds.)*
8. **Anti-entropy.** Why is read repair insufficient, and where do Merkle trees fit in? *(Hint: cold keys; log-scale divergence detection.)*
9. **Lakehouse merge.** Write the guard that makes a `MERGE` idempotent and order-tolerant under duplicate CDC events. *(Hint: version/sequence comparison; delete as an event.)*
10. **Convergence SLO.** Define "p99 convergence ≤ 30 s" and describe how you would measure it in a Cassandra multi-DC cluster. *(Hint: divergence sampling; repair lag; DC replication stats.)*
11. **Metadata growth.** An OR-Set grows without bound. Describe stabilization and what invariant it depends on. *(Hint: causal stability — every replica has seen the update.)*
12. **Choosing a model.** For each of: likes, a bank balance, a shopping cart, a text editor, and a user status field — pick LWW / CRDT / linearizable, with a one-line reason. *(Hint: match the domain's algebra to the merge.)*

---

## References

- Shapiro, M., Preguiça, N., Baquero, C., Zawirski, M. — "Conflict-Free Replicated Data Types" (SSS 2011): https://www.lip6.fr/Marc.Shapiro/papers/2011/CRDTs_SSS-2011.pdf
- Shapiro et al. — "A Comprehensive Study of Convergent and Commutative Replicated Data Types" (INRIA RR-7506): https://hal.inria.fr/inria-00555588
- Kleppmann, M. & Beresford, A. — "A Conflict-Free Replicated JSON Datatype": https://arxiv.org/abs/1608.03960
- Kleppmann, M. — *Designing Data-Intensive Applications*, Chapters 5 and 9 (replication, conflict resolution, eventual consistency). https://dataintensive.net/
- DeCandia et al. — "Dynamo: Amazon's Highly Available Key-value Store" (vector clocks, hinted handoff, Merkle trees): https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf
- Terry et al. — "Managing Update Conflicts in Bayou, a Weakly Connected Replicated Storage System" (SOSP 1995): https://dl.acm.org/doi/10.1145/224056.224070
- Vogels, W. — "Eventually Consistent" (ACM Queue 2008): https://dl.acm.org/doi/10.1145/1466443.1466448
- Riak Documentation — Data types / CRDTs: https://docs.riak.com/riak/kv/latest/developing/data-types/index.html
- Apache Cassandra — Architecture (read repair, hinted handoff, gossip, anti-entropy): https://cassandra.apache.org/doc/latest/cassandra/architecture/index.html
- Amazon DynamoDB — Global tables (multi-region active-active): https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GlobalTables.html
- Microsoft — Azure Cosmos DB consistency levels: https://learn.microsoft.com/en-us/azure/cosmos-db/consistency-levels
- Yjs Documentation — Shared data types and YATA: https://docs.yjs.dev/
- Automerge — CRDT library for JSON: https://automerge.org/
- Delta Lake — MERGE and idempotent upserts: https://docs.delta.io/latest/delta-update.html
- Delta Lake — Concurrency control (optimistic concurrency): https://docs.delta.io/concurrency-control/
