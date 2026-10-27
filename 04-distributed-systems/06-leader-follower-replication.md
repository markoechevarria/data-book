# Leader/Follower Replication

> Leader/follower replication is the default architecture of almost every database you touch: one node accepts writes and serializes them into an ordered log, others replay that log to stay in sync. It is simple in principle and brutally subtle in practice, because all the difficulty is concentrated into one question: **when the leader dies, who becomes the new leader, and how do we guarantee the old one stops writing?** Botch that question and you get *split-brain* — two leaders, two divergent histories, silent data loss that surfaces weeks later as an audit failure. This chapter is the end-to-end story: the replication log, leader election (Raft, Paxos, ZAB in the abstract), failover mechanics, and the fencing and quorum rules that make failover safe. It also gives you the concrete operational recipes for Postgres, MySQL, MongoDB, Kafka, etcd, and RDS/Aurora, plus the runbooks and failure modes a senior is expected to know cold.

## Why a Data Engineer Cares

- **Kafka is a leader/follower system.** Every partition has a leader and followers; a controller elects new leaders when brokers fail; ISR (in-sync replicas) defines who is eligible. `unclean.leader.election.enable` is literally a split-brain/consistency policy.
- **Your CDC source is a leader/follower system.** Postgres streaming replication, MySQL binlog replication, MongoDB replica sets. When the primary fails over, your Debezium connectors must reconnect, re-resolve the primary, and handle a new timeline/log position.
- **Failover changes your pipeline's assumptions.** After failover, replication slots may be gone, binlog positions may reset (GTID helps), and a promoted replica may be missing the last few seconds of writes. Your downstream must tolerate a slightly different history.
- **RPO/RTO are failover properties.** "We have replicas" is not a failover plan; the plan is the election + fencing + promotion procedure, and its measured RTO.
- **Metadata stores are leader/follower.** Hive Metastore, Iceberg catalogs backed by Postgres, Airflow's DB, Kubernetes' etcd — a split-brain there corrupts orchestration, not just data.

Leader/follower replication is where the theory you read in chapters 1–5 becomes an on-call runbook.

## TL;DR

- **Leader/follower (a.k.a. primary/replica, master/slave):** one node accepts writes, orders them in a log, and followers replay the log. Reads can go anywhere (with staleness caveats).
- **The log is the replication.** WAL, binlog, oplog, Raft log — same idea, different encodings.
- **Failover = detect failure → elect a new leader from an eligible set → promote → redirect clients → fence the old leader.**
- **Leader election needs a quorum** to be safe. Raft/Paxos/ZAB solve it with terms/epochs, majority votes, and a log-matching rule that prevents a stale candidate from winning.
- **Split-brain is the failure to avoid:** without quorum + fencing, the old leader may keep accepting writes. Fix with **majority quorum**, **fencing tokens**, **STONITH**, or **witness nodes**.
- **Fencing** is not optional: a "fenced" old leader must be unable to write to shared storage/other nodes. Tokens, epochs, and revocable leases are the tools.
- **Synchronous replication** gives RPO=0 but ties availability to replicas; **asynchronous** gives fast, available writes but can lose the tail on failover. Semi-synchronous (`ANY 1`, `w:majority`) is the usual compromise.
- **Automatic failover is a product decision.** Postgres core has none (use Patroni/repmgr); MySQL needs MHA/Orchestrator/Group Replication; MongoDB, Kafka, RDS, Aurora have it built in.
- **Test failover or you don't have it.** Scheduled, automated, measured, and rehearsed — with a documented RTO/RPO.

---

## 1. Definitions and Core Vocabulary

| Term | Meaning |
|---|---|
| **Leader (primary, master)** | The node that accepts writes and drives the log. |
| **Follower (replica, standby, secondary, slave)** | A node that applies the leader's log. May serve reads. |
| **Replication log** | The ordered stream of changes (WAL, binlog, oplog, Raft log). |
| **LSN / binlog position / GTID / oplog ts** | A position in the log used to track and resume replication. |
| **Timeline** | Postgres concept: an incrementing ID bumped on each promotion, so replicas can refuse to follow a divergent history. |
| **Failover** | Promoting a follower to leader after the leader fails. |
| **Failback** | Returning leadership to the original node (after it recovers). |
| **Leader election** | The process of choosing a new leader; safe versions require consensus. |
| **Quorum** | Strict majority (⌊N/2⌋+1) of voters; required to elect/commit in Raft/Paxos/ZAB. |
| **Term / epoch / view** | A monotonically increasing number identifying a leadership period; used to reject stale leaders/messages. |
| **Commit index** | The log position that a majority has replicated and that is therefore durable. |
| **Log matching property** | Raft/Paxos invariant: if two logs contain an entry with the same index and term, all preceding entries are identical. Prevents divergent history. |
| **Split-brain** | Two nodes simultaneously believing they are leader, and both accepting writes. |
| **Fencing** | Making a stale leader incapable of writing (tokens, epochs, STONITH/power-off, revocable leases). |
| **Fencing token** | A monotonically increasing number attached to each leadership term; storage rejects writes with an older token. |
| **STONITH** | "Shoot The Other Node In The Head" — forcibly power off/reboot the other node to break a split. |
| **Witness / arbiter** | A lightweight quorum participant that votes but holds no data (MongoDB arbiter, Galera garbd). |
| **Sentinel** | Redis's election/monitoring component (Redis Sentinel). |
| **ISR (Kafka)** | In-Sync Replicas: the set of followers caught up to the leader, eligible for election. |
| **Unclean leader election** | Kafka: allow an out-of-sync replica to become leader → availability at the cost of data loss. |
| **Failstop / crash-stop** | Fault model assuming a failed node simply stops (vs. Byzantine, where it may lie). |
| **Phi-accrual failure detector** | Probabilistic heartbeat-liveness estimator used by Cassandra/Akka etc. |
| **Promotion** | The act of turning a follower into the leader (stop recovery, open for writes, bump term/timeline). |

> **Senior note:** Leader/follower replication and **consensus (Raft/Paxos)** are not the same thing, though they compose. Plain leader/follower with manual failover is just log shipping; adding *automatic, safe* election turns the log into a consensus log. Kafka KRaft and etcd are consensus-based; classic Postgres streaming replication is not (the *operator* provides the election via Patroni).

---

## 2. What Problem It Solves / Why It Matters

Leader/follower replication answers four questions with one architecture:

| Question | Answer provided |
|---|---|
| How do I get durability across machine failures? | Followers hold copies; leader commits to a quorum |
| How do I scale reads? | Followers serve read-only queries |
| How do I order writes globally (for a shard)? | The single leader totally orders them |
| How do I survive a leader crash with minimal downtime? | Elect a follower and promote it |

**Business mapping:**
- "We must not lose a committed order" → synchronous/semi-sync follower + safe election (RPO=0).
- "We must be back in under a minute" → automated election with a pre-warmed standby (RTO < 60 s).
- "Analytics must not disturb OLTP" → route reads to followers (accepting lag).
- "We need one consistent ordering for CDC" → the leader's log is that ordering; downstream keys off LSN/offset.

**The cost:** a leader is a write bottleneck and a coordination point. You trade write scalability for write *order*, and you inherit failover complexity. That is why sharded systems (ch. 2) put a leader/follower group **per shard** — scaling writes by sharding, keeping order by having one leader per shard.

---

## 3. How It Works Internally (Mechanisms, Internals)

### 3.1 The base architecture

```
                     ┌──────────────┐
     writes ────────▶│    LEADER    │◀── clients (reads too, if freshly-consistent)
                     │  appends log │
                     └──────┬───────┘
       replication log      │
        ┌───────────────────┼───────────────────┐
        ▼                   ▼                   ▼
  ┌──────────┐        ┌──────────┐        ┌──────────┐
  │ FOLLOWER │        │ FOLLOWER │        │ FOLLOWER │   ◀── read-only traffic
  │ (replay) │        │ (replay) │        │ (replay) │
  └──────────┘        └──────────┘        └──────────┘

  Async:  leader acks after local append.
  Sync:   leader acks after ≥K followers confirm (quorum).
```

### 3.2 Log shipping mechanics per system

| System | Log | Position | Notes |
|---|---|---|---|
| **PostgreSQL (physical)** | WAL segments | LSN + timeline | Byte-identical replica; replication slots retain WAL |
| **PostgreSQL (logical)** | Decoded row changes | LSN | Can filter tables; subscriber can differ in version |
| **MySQL** | Binary log (row/statement) | binlog filename+pos, GTID | GTID makes failover position-agnostic |
| **MongoDB** | Oplog | oplog timestamp (TS) | Idempotent replay of `applyOps` |
| **Kafka** | Partition log | offset | Followers *fetch* from leader like consumers |
| **etcd/Consul/ZooKeeper** | Raft/ZAB log | index + term | The log *is* the consensus protocol |
| **SQL Server** | Transaction log | LSN | Log shipping / Always On |

```
Postgres timeline example (why promotion is tricky):

  timeline 1:  L0 ── L100 ── L200  (original primary dies at L200)
  promotion of standby B:  timeline 2 branches at L200
                 timeline 2:  L200 ── L210 ── ...
  If standby A (still on timeline 1) reconnects, it must NOT blindly follow a
  leader whose log diverged. The timeline ID makes this detectable.
  ⇒ pg_rewind repairs a diverged old primary before it can rejoin.
```

### 3.3 Replica catch-up: how a follower joins or rejoins

```
Cold join (new replica):
  1. base backup @ position P  (pg_basebackup; MySQL clone plugin; Mongo initial sync)
  2. ship the snapshot
  3. stream WAL/binlog/oplog from P onward
  4. when replay catches up → "streaming" state

Rejoin after short outage:
  - Stream from where it left off (needs the log still retained).
  - If the log is gone (truncated/GC'd) → full re-sync.

Rejoin after a *diverged* history (old primary coming back):
  - pg_rewind (Postgres), reseed, or re-clone.
  - Never allow it to "catch up" by replaying the wrong history.
```

### 3.4 Leader election: Raft, step by step (the model to internalize)

```
States: FOLLOWER, CANDIDATE, LEADER. Persistent: currentTerm, votedFor, log[].

Raft in five moves (per term):
  1. Heartbeats: the leader sends AppendEntries (empty = heartbeat) every ~50–150ms.
  2. Timeout: if a follower hears nothing for a randomized election timeout,
     it becomes a CANDIDATE, increments currentTerm, votes for itself.
  3. RequestVote: the candidate asks all peers for votes (same term).
     A peer grants a vote if (a) it hasn't voted this term and
     (b) the candidate's log is at least as up-to-date (lastTerm, lastIndex).
  4. Majority: on getting >N/2 votes → LEADER. It appends a no-op/current entry
     and replicates it to establish the commit index for its term.
  5. Safety: entries are committed only when replicated to a majority;
     the "log matching" + "leader completeness" rules ensure no committed entry
     is ever lost, even across elections.
```

```
    ┌────────────┐  timeout   ┌────────────┐  majority votes  ┌────────────┐
    │  FOLLOWER  │───────────▶│ CANDIDATE  │─────────────────▶│   LEADER   │
    └─────┬──────┘            └─────┬──────┘                  └─────┬──────┘
          ▲                         │ higher term seen              │
          │                         ▼                               │
          └──────────────────────────────────────────────────────────┘
                       step down (append from a newer leader)

Randomized timeouts prevent split votes (two candidates perpetually tied).
Terms prevent stale candidates/leaders from being accepted.
```

**Why this matters for a DE:** etcd (Kubernetes, Kafka's older ZK, many catalogs), Consul, CockroachDB, TiKV, and Kafka KRaft all implement (variants of) Raft. Understanding the moves lets you diagnose "election storm" incidents (`election timeout too low`, network flapping, overloaded nodes failing heartbeats).

### 3.5 Failover: the exact sequence, and where it goes wrong

```
1. DETECT       — heartbeat timeout, health check failure, quorum loss.
                  (Phi-accrual, Raft timeout, Patroni ttl, Mongo heartbeat.)
2. ELECT        — a quorum of voters chooses the new leader.
                  Only nodes with sufficiently up-to-date logs are eligible.
3. PROMOTE      — the chosen follower stops recovery, opens for writes,
                  bumps term/timeline, optionally waits for remaining LSN.
4. REDIRECT     — clients/proxies/DNS/service discovery point to the new leader.
                  (Patroni rewrites a distributed config; Cockroach has gateways.)
5. FENCE        — the old leader must be prevented from writing:
                  revoke its lease, bump the fencing token, STONITH it,
                  or ensure it cannot reach quorum for commits.
6. RECONCILE    — on recovery, the old leader rejoins as a follower
                  (possibly needing pg_rewind/re-clone if it had diverged).
```

**Where it goes wrong:**
- Step 1 fires on a *slow but alive* node → unnecessary failover (flapping).
- Step 3 promotes a lagging replica → data loss.
- Step 4 is slow or partial → some clients keep talking to the old leader.
- **Step 5 is skipped** → split-brain: two leaders, both accepting writes.
- Step 6 is mishandled → the old leader's divergent writes are replayed or lost incoherently.

### 3.6 Split-brain: the failure mode

```
              ┌──────── partition ────────┐
   ┌──────────┴─────────┐        ┌────────┴─────────┐
   │  OLD LEADER (A)    │   ✂    │  NEW LEADER (B)  │
   │  still accepting   │        │  elected by      │
   │  writes ✍          │        │  majority ✍      │
   └────────────────────┘        └──────────────────┘
        Both write → divergent histories → last one to resync "wins" or
        writes are silently discarded. Catastrophic for ledgers.
```

**Prevention (all three, not one):**

1. **Quorum for writes.** A leader can only commit with a majority. The minority side therefore cannot make progress → it *should* refuse writes. (Raft, Galera, MongoDB `w:majority`, etcd.)
2. **Fencing.** Storage/clients reject writes from an older term/token. Fencing tokens are monotonic; the old leader's token is lower, so its writes are refused.
3. **Physical isolation (STONITH).** Power off / fence the old node so it *cannot* write. The blunt but effective final backstop.

```
Fencing-token example:
  term 1 leader A → token 1 → writes to storage with token 1
  partition; A is isolated; term 2 leader B → token 2
  storage rule: reject writes whose token < the max token seen.
  A's writes (token 1) are rejected even though A still "thinks" it's leader. ✅
```

> **Senior note:** Fencing must be enforced **on the resource**, not in the leader's own code. A leader that politely checks "am I still leader?" before writing is not fenced — the check itself may be stale. The *storage* must reject the old token.

### 3.7 PostgreSQL failover in depth (Patroni)

Postgres core ships streaming replication but **no automatic failover**. Production setups bolt on one of:

- **Patroni** — distributed config in etcd/Consul/ZooKeeper; a leader lease; automatic promotion; `pg_rewind` for rejoin; HAProxy/consul-template for client routing. The de-facto standard.
- **repmgr** — simpler, quorum-based, less flexible.
- **pg_auto_failover** — monitor + keeper nodes.
- **Cloud (RDS/Aurora/Cloud SQL)** — managed, hidden automation.

```
Patroni election:
  - Every node holds a lease key in etcd with a TTL.
  - The leader renews the lease; if it dies, the TTL expires.
  - A healthy replica takes the lease (compare-and-swap), then promotes.
  - The old leader, on return, sees the lease is taken → demotes, pg_rewind, rejoins.
  - Client routing (HAProxy) points at the node holding the leader key.
```

```yaml
# patroni.yml (abridged)
scope: pg-cluster
namespace: /service/
etcd3:
  hosts: ["etcd1:2379", "etcd2:2379", "etcd3:2379"]
bootstrap:
  dcs:
    ttl: 30                 # lease TTL
    loop_wait: 10           # how often we check
    retry_timeout: 10
    synchronous_mode: true  # optional: only promote sync standbys
    synchronous_mode_strict: false
    postgresql:
      parameters:
        wal_level: replica
        synchronous_commit: "on"
        max_wal_senders: 10
        hot_standby: "on"
postgresql:
  callbacks:
    on_start: /etc/patroni/on_start.sh      # e.g. repoint HAProxy / PgBouncer
    on_role_change: /etc/patroni/on_role.sh
```

`synchronous_mode: true` makes Patroni prefer promoting a *synchronous* standby → RPO=0 failover. The trade: if no sync standby is available, failover may be blocked (availability for correctness).

### 3.8 Kafka's leader election (controller + ISR)

```
Kafka partition:  leader L + followers F1..Fk   (replication factor k+1)
ISR = {L, F1} if F2 is lagging beyond replica.lag.time.max.ms

Broker failure:
  1. Controllers (KRaft quorum or ZooKeeper) detect the broker is gone.
  2. For each partition it led, pick a NEW leader from the ISR.
     - ISR non-empty & ULE=false  → choose from ISR (no data loss) ✅
     - ISR empty & ULE=true       → choose any replica (data loss possible) ⚠️
  3. Consumers reconnect; offsets are preserved.

Key configs:
  unclean.leader.election.enable = false   # never elect out-of-sync → no loss
  min.insync.replicas = 2                  # producers with acks=all need 2 in ISR
  replica.lag.time.max.ms = 30000          # how long before a follower leaves ISR
```

```
"ISR empty" scenario:
  RF=3, min.insync.replicas=2. Two brokers fail → ISR = {leader} only.
  Producers with acks=all now BLOCK (cannot meet min.insync.replicas).
  - With ULE=false: the partition is unavailable for writes until a broker returns.
    (Availability sacrificed; no data loss.)
  - With ULE=true: promote a stale replica → writes resume, but messages that
    only existed on the dead leader are LOST.
  ⇒ This is a pure CAP decision, per topic (ch. 3).
```

### 3.9 MongoDB replica set election

```
Replica set: 1 primary + N secondaries (+ optional arbiter, no data).
  - Secondaries replicate the oplog from the primary.
  - Heartbeats every 2s; a primary is "unreachable" after electionTimeoutMillis.
  - An eligible secondary calls an election, needs a MAJORITY of votes.
  - The most up-to-date secondary (highest oplog term/ts) is the best candidate.
  - Write concern w:majority + read concern majority/linearizable control guarantees.
  - Priority/`nodes[i].priority` influences who wins; `hidden`/`delayed` members
    are non-voting or intentionally lagging.
```

```javascript
// Promote an eligible secondary by priority, and require majority writes
cfg = rs.conf()
cfg.members[1].priority = 10        // prefers member 1 as primary
rs.reconfig(cfg)
db.adminCommand({ setDefaultRWConcern: 1, defaultWriteConcern: { w: "majority" }})
```

### 3.10 Managed failover (RDS / Aurora / Cloud SQL)

```
RDS Multi-AZ (instance):
  - Synchronous standby in another AZ; automatic CNAME flip on failure.
  - RTO typically ~60–120 s; RPO ≈ 0 (sync).
  - Standby does not serve reads.

RDS Multi-AZ (cluster):
  - Semi-synchronous to two readable standbys; faster failover (~35 s).

Aurora:
  - Storage replicated 6 ways across 3 AZs; redo log applied by replicas.
  - Failover typically < 30 s; reader endpoints survive.
  - "Fast failover"/"cluster cache management" reduces warm-up time.

Cloud SQL (MySQL/Postgres): regional HA with a standby; automatic failover.
```

The point for a DE: **the platform owns steps 1–5**; you own step 6 (reconnect, re-resolve the primary, and not lose your offsets/slots).

---

## 4. How to Implement It (Step-by-Step, with Code / Config)

### 4.1 Postgres HA with Patroni + etcd + HAProxy (the standard stack)

```bash
# 1) etcd (3 or 5 nodes — odd, for quorum)
etcd --name etcd1 --initial-cluster etcd1=...,etcd2=...,etcd3=...
# 2) Patroni on each Postgres node (see patroni.yml above); one becomes leader
patroni /etc/patroni/patroni.yml
# 3) HAProxy in front; route 5432 read-write to the leader, 5433 read-only to replicas
```

```haproxy
# haproxy.cfg (abridged): leader vs replicas via Patroni's REST health check
frontend pg_rw
  bind *:5432
  default_backend pg_leader
backend pg_leader
  option httpchk GET /primary            # Patroni returns 200 only on the leader
  server pg1 pg1:5432 check port 8008
  server pg2 pg2:5432 check port 8008
frontend pg_ro
  bind *:5433
  default_backend pg_replicas
backend pg_replicas
  option httpchk GET /replica
  server pg1 pg1:5432 check port 8008
  server pg2 pg2:5432 check port 8008
  server pg3 pg3:5432 check port 8008
```

**Failover test (do this in staging, on a schedule):**
```bash
# Simulate a primary crash and time the recovery:
patronictl -c /etc/patroni/patroni.yml switchover --force --candidate pg2
# or hard-kill the leader process and watch:
watch -n1 'patronictl -c /etc/patroni/patroni.yml list'
psql "host=10.0.0.10 port=5432 dbname=app" -c "select pg_is_in_recovery();"
```

### 4.2 Fencing a stale leader (the invariant)

```python
# Storage-side fencing: the resource enforces monotonic tokens.
class FencedVolume:
    def __init__(self):
        self.max_token = 0
    def write(self, token, data):
        if token < self.max_token:
            raise PermissionError(f"stale leader token {token} < {self.max_token}")
        self.max_token = max(self.max_token, token)
        self._apply(data)
        return True

# Each leadership term issues a strictly increasing token:
class Leader:
    def __init__(self, volume, token):
        self.volume, self.token = volume, token
    def write(self, data):
        return self.volume.write(self.token, data)   # rejected if superseded
```

**Checklist for real deployments:**
- [ ] Majority quorum required for commit (Raft/Paxos/Galera/`w:majority`).
- [ ] Monotonic term/epoch; storage rejects older terms.
- [ ] STONITH / power fencing available (IPMI, cloud API) as a backstop.
- [ ] Network fencing: the old leader cannot reach the shared resource/other nodes.
- [ ] Revocable leases with bounded clock assumptions (or no leases at all).

### 4.3 Kafka failover configuration and validation

```bash
# Ensure no data loss on broker failure:
kafka-configs.sh --alter --entity-type topics --entity-name payments \
  --add-config min.insync.replicas=2,unclean.leader.election.enable=false

# Producers must use acks=all to benefit:
#   acks=all + min.insync.replicas=2 + RF=3  ⇒ tolerate 1 broker down, no loss

# Validate your failover behavior:
kafka-topics.sh --describe --topic payments      # see leader + ISR per partition
# Kill a broker that leads a partition; confirm a new leader appears from the ISR.
```

### 4.4 MongoDB replica set with safe failover

```javascript
// 3 voting members across 3 AZs; majority write concern; causal sessions
rs.initiate({
  _id: "rs0",
  members: [
    { _id: 0, host: "m0:27017", priority: 2 },
    { _id: 1, host: "m1:27017", priority: 1 },
    { _id: 2, host: "m2:27017", priority: 1 }
  ]
});

// Application:
//   write: { w: "majority", j: true }        → durable across majority
//   read:  { readConcern: { level: "majority" } } or "linearizable" when needed
//   session: causalConsistency: true         → read-your-writes + monotonic reads
```

```
Election safety rules:
  - A primary must be able to reach a MAJORITY of voting members to stay primary.
  - Only the most up-to-date eligible secondary should win (rollback protection).
  - `w:majority` writes are guaranteed to survive failover.
  - Use 3 or 5 voting members; an arbiter can break a tie but adds no durability.
```

### 4.5 Detecting and preventing split-brain (ops layer)

```yaml
# Kubernetes example: a StatefulSet with a leader-election sidecar (Lease API)
# and a PodDisruptionBudget so you don't lose quorum during maintenance.
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata: { name: pg-cluster }
spec:
  minAvailable: 2        # keep a majority of 3 alive during node drains
  selector: { matchLabels: { app: postgres } }
```

```bash
# Fencing a node that is likely the "old leader" after a partition:
#  1) confirm quorum has moved on (new leader exists with a higher term)
#  2) fence the old node before letting it rejoin writes
#     - cloud: detach/power-off the instance, or revoke its IAM access
#     - bare metal: IPMI power-off (STONITH)
#  3) repair divergence: pg_rewind / stripe the diverged WAL / re-clone
```

### 4.6 Manual failover runbook (for systems without automation)

```
Preconditions:  a replica within your RPO window is healthy; you know the RPO.
1. Announce + freeze writers if possible (drain the app).
2. Verify the chosen replica's replay position. Compare to the last known
   primary LSN (if recoverable). Record the expected data loss (how many seconds).
3. Promote:
     pg_ctl promote -D /var/lib/postgresql/data      # or: SELECT pg_promote();
4. Redirect clients:
     - update DNS/service discovery, or
     - flip the proxy backend (HAProxy/pgbouncer), or
     - update the application's connection string source (config service).
5. FENCE the old primary (power off / revoke access) before it can accept writes.
6. Verify:  SELECT pg_is_in_recovery();  -- must be false on the new primary
7. Rebuild the old node as a follower (pg_rewind or re-clone).
8. Unfreeze writers; watch error rates and lag.
9. Postmortem: record actual RTO/RPO; fix the gap.
```

---

## 5. Where to Implement It (Systems and When Each Fits)

| System | Leader/follower style | Auto-failover | When to choose |
|---|---|---|---|
| **PostgreSQL + Patroni** | Streaming (physical) + logical | Yes (Patroni) | Self-managed HA OLTP; full control |
| **PostgreSQL + RDS/Aurora** | Managed, sync/semi-sync | Yes | Managed HA; less control |
| **MySQL + Orchestrator/MHA/Group Replication** | Binlog, GTID | Yes (with tooling) | Cost-sensitive OLTP |
| **MongoDB replica set** | Oplog | Yes (built-in) | Document apps needing HA |
| **Kafka** | Per-partition leader + ISR | Yes (controller) | Streaming; tunable durability |
| **etcd / Consul / ZooKeeper** | Raft/ZAB consensus | Yes | Coordination, config, leader election |
| **CockroachDB / TiKV / Spanner** | Raft/Paxos per shard (multi-leader *group*) | Yes | Distributed SQL with strong consistency |
| **Redis (Sentinel/Cluster)** | Async replica + sentinel election | Yes | Cache HA; accepts tail loss |
| **SQL Server Always On** | Log shipping / AG | Yes | Microsoft shops |
| **Galera / Percona XtraDB (MySQL)** | Synchronous certification-based multi-primary | Yes | Multi-primary MySQL, low-latency LAN |
| **Aurora / Cloud SQL / Azure SQL** | Managed storage-level | Yes | Cloud-managed relational HA |

**Choosing guide (short):**
- **Self-managed Postgres** → Patroni with an odd etcd cluster; HAProxy for routing.
- **Managed relational** → RDS Multi-AZ / Aurora; accept the platform's RTO.
- **Streaming** → Kafka RF=3, `min.insync.replicas=2`, `acks=all`, ULE off for critical topics.
- **Coordination** → etcd/Consul/ZooKeeper with 3 or 5 nodes.
- **Distributed SQL** → Cockroach/Yugabyte/TiDB (Raft per range, automatic).
- **Cache** → Redis Sentinel/Cluster; tolerate tail loss.

---

## 6. Patterns and Anti-Patterns

### Patterns

- **Odd quorum sizes (3, 5, 7).** Never 2 or 4 — they don't add failure tolerance and can tie.
- **Fencing tokens on every leadership change.** Storage enforces monotonicity.
- **Delayed replicas for oops-recovery.** A replica intentionally N minutes behind protects against `DROP TABLE`.
- **Synchronous mode restricted to a small critical set.** Sync only the writes that must not be lost.
- **Automated, scheduled failover drills.** Prove RTO/RPO monthly.
- **Pre-warmed standbys.** Restore caches/buffer pools so failover isn't slow for the first 10 minutes ("fast failover" / cache warm-up).
- **Client retry with idempotency keys.** Every failover produces a burst of retries; make them safe.
- **Separate read replicas from failover standbys.** Don't let analytics starve the promotion candidate.
- **Watchdog + witness.** A witness breaks ties without storing data.
- **Log-based client redirection.** Clients discover the leader (Patroni REST, `mongos`, broker metadata) rather than hardcoding.

### Anti-Patterns

- **Two-node clusters for HA.** No majority; a partition can't be resolved safely.
- **"We have replicas, so we have HA."** Async replicas are not a failover plan.
- **No fencing.** Guaranteed split-brain under partition.
- **Automatic failover with a too-short timeout.** Flapping; a slow-but-alive primary gets demoted.
- **Promoting the *newest* replica with `unclean.leader.election.enable=true`.** Silent data loss.
- **Hardcoded primary IPs in configs/connectors.** Breaks on every failover.
- **Running `pg_rewind`/rejoin on a still-live old primary.** Divergence isn't resolved; it's duplicated.
- **Ignoring the write burst after failover.** Retries + cold caches can re-crash the new leader.
- **Arbiters in MongoDB as a substitute for real replicas.** They vote but don't store data (no extra durability).
- **No failover test until the real incident.** Your runbook is a hypothesis until tested.
- **Fencing implemented in the leader itself.** The stale leader is exactly the node you can't trust.
- **Delayed replica promoted accidentally.** You just lost N minutes of writes backward — check eligibility.

---

## 7. Real-World Use Cases and Industry Applications

- **GitHub PostgreSQL HA.** Well-documented use of orchestrator-style tooling, replicas, and carefully controlled failovers; also a public example of a split-brain-adjacent incident and the fencing lessons learned.
- **GitLab PostgreSQL at scale.** Patroni + Consul, documented failover procedures and RTO, and extensive incident write-ups on failover and replication lags.
- **Kafka at LinkedIn/Uber.** Controller-driven leader election per partition; `ISR` semantics tuned per topic; unclean election disabled for critical pipelines.
- **MongoDB at many SaaS companies.** Replica sets with `w:majority` + causal sessions; elections tuned with priorities and hidden members.
- **etcd in Kubernetes.** Raft quorum for the entire control plane; the canonical example of "lose quorum, lose the cluster's ability to change".
- **CockroachDB / Spanner.** Raft/Paxos *per range*: each range is a leader/follower group. Failover is per-range, sub-second, and invisible to applications. The logical endpoint of this chapter.
- **AWS RDS/Aurora.** Multi-AZ failover with a CNAME flip; engineered RTO numbers published in docs; the "HA as a checkbox" model most teams use.
- **Redis Sentinel/Cluster.** Sentinel election among sentinels; async replicas mean tail loss is possible — chosen for cache workloads where it's acceptable.
- **Financial exchanges / trading systems.** Often deliberately avoid automatic failover on the write path, preferring manual control with strong fencing, because a wrong automatic promotion is worse than a short outage.

---

## 8. Senior-Level Deep Dive

### 8.1 RPO/RTO math and the failure tolerance table

| Cluster size | Quorum | Failures tolerated | Notes |
|---|---|---|---|
| 1 | 1 | 0 | No HA |
| 2 | 2 | 0 | **Anti-pattern**: any failure loses quorum |
| 3 | 2 | 1 | The minimum sane HA |
| 4 | 3 | 1 | No better than 3, more cost |
| 5 | 3 | 2 | Good balance for multi-AZ |
| 7 | 4 | 3 | Multi-region; higher coordination cost |

```
RPO (data loss) as a function of replication mode:
  async:            RPO = replication lag at failure time (can be seconds/minutes)
  semi-sync (k of N): RPO = 0 if ≥1 in-sync replica counted; else = lag of the promoted node
  sync (all):       RPO = 0 always (if you can write at all)

RTO (downtime) as a function of automation:
  manual runbook:            tens of minutes (human detection + action)
  automated (Patroni/Mongo): seconds to ~1 minute (detect + elect + promote + redirect)
  managed (RDS/Aurora):      ~30–120 s (platform-dependent), plus client reconnect time
  consensus-based (Cockroach/Spanner): sub-second per range, but client retries dominate
```

### 8.2 Consistency of failover: what a promoted replica can and cannot promise

```
An async-promoted primary:
  + has a consistent prefix of the log
  + can immediately accept writes on a new timeline
  − is missing the tail the old leader had → committed-but-lost writes
  − cannot serve reads that "saw" those writes (they're gone)

A sync-promoted primary:
  + has everything that was acknowledged to clients (RPO=0)
  − might not have *unacknowledged* writes in flight (still lost, but they were
    never acknowledged → no contract broken)

⇒ Define your contract in terms of *acknowledged* writes. "We lose nothing that
  we told the client we accepted" is the honest, achievable guarantee for
  semi-sync/sync; async cannot say even that.
```

### 8.3 The leader bottleneck and how to break it

```
A single leader per shard serializes writes. Ways to scale beyond one leader:

1. SHARD MORE          — more leader/follower groups (ch. 2). The standard answer.
2. MULTI-LEADER        — several leaders accept writes → conflicts (ch. 5).
3. CONSENSUS GROUP     — the "leader" is a per-range Raft group; leadership is
                         spread across many groups, so the write bottleneck is
                         a range, not a cluster (Cockroach/Spanner).
4. FOLLOWER READS      — offload reads (with staleness), keeping one writer.
5. WRITE BATCHING      — amortize the commit round-trip over many rows.
```

Null Hypothesis: for most OLTP systems, **sharding + follower reads** beats multi-leader. Multi-leader is usually a geo/latency decision, not a throughput one.

### 8.4 Failure modes and their signatures

| Failure | Signature | Root cause | Fix |
|---|---|---|---|
| **Split-brain** | Two writers, divergent rows, duplicate/lost transactions | No quorum/fencing | Majority commit + fencing tokens + STONITH |
| **Election storm** | Repeated leadership changes, latency spikes | Timeouts too low, network flapping, overloaded nodes | Tune timeouts, separate heartbeat networks, reduce load |
| **Lost tail on failover** | Recent rows missing after promotion | Async promoted | Semi-sync for critical data; document RPO |
| **Old primary rejoins as-is** | Duplicated/contradictory data | No pg_rewind/re-clone; fencing skipped | Rewind/re-clone; enforce fencing |
| **No quorum** | Writes fail; reads may still work | Too many nodes down, 2-node cluster | Add nodes; restore majority; avoid even sizes |
| **Failover succeeds but apps fail** | Connection errors, stale primary cache | Hardcoded IPs; no reconnect logic | Service discovery; retry with backoff; idempotency |
| **Failover slow (minutes)** | RTO far above target | Cold caches, slow promotion, no client redirect | Pre-warmed standbys; automated routing; fast discovery |
| **Replica promotes with wrong data** | Data appears to "roll back" | Delayed replica promoted | Exclude delayed/hidden members from election |
| **Cascading failure post-failover** | New leader overloaded | Retry storm + cold cache | Backoff/jitter; rate-limit retries; warm caches |
| **Zombie leader writing** | Writes appear then vanish after heal | Fencing bypassed | Enforce at the storage layer |

### 8.5 Testing failover (the only way to know your RTO)

```
A real failover test suite:

  1. Planned switchover     — graceful promotion; measures best-case RTO.
  2. Unplanned leader kill  — SIGKILL the leader process; measures detection+promotion.
  3. Network partition      — isolate the leader (iptables/tc netem);
                              verify the majority side promotes AND the old leader
                              is fenced (cannot commit writes).
  4. Slow-leader test       — add 5s latency; verify no spurious failover (flapping).
  5. Overload test          — saturate the leader; ensure heartbeats still get through
                              (or that the system degrades predictably).
  6. Client-behavior test   — verify apps reconnect, retry idempotently, and do not
                              write to the stale leader.
  7. Repeat under load      — failover at p99 write rate, not on an idle cluster.

Record for each: detection time, election time, promotion time, redirect time,
total RTO, and observed write loss (RPO).
```

### 8.6 Debugging leader/follower incidents: a runbook

1. **Who is the leader?** `patronictl list`, `rs.status()`, `kafka-topics --describe`, `etcdctl endpoint status`. Never assume.
2. **Is there a quorum?** Check etcd/ZK/Raft peer health; a healthy minority is still dead.
3. **Is anyone fenced?** Look for a demoted-but-alive old leader that could still write.
4. **How far behind is the promoted node?** Compare last applied position with the last acknowledged write.
5. **Are clients pointed at the right node?** DNS TTLs, proxy backends, service discovery, connection pools.
6. **Is the log retained?** Slots/retention determine whether failed followers can rejoin without a full resync.
7. **Is the new leader overloaded?** Check CPU/IO/latency; failover bursts are common.
8. **Write the timeline.** Every failover incident is a timeline problem; reconstruct detect→elect→promote→redirect→fence.

### 8.7 Leader/follower in the data-engineering stack

```
Postgres/MySQL (leader) ──CDC──▶ Kafka ──▶ lakehouse
   failover: new primary, new timeline/LSN
   your connector must: re-resolve the primary, resume from a valid position,
   and avoid double-applying the overlapping tail (idempotent merges!)

Kafka leader election ──▶ downstream consumers
   a partition-leader change can cause consumer group rebalances;
   ensure offsets are committed and processing is idempotent.

Warehouse/catalog metadata DB (Postgres/HMS/Glue) ──▶ orchestration
   a split-brain here corrupts job state; treat catalogs as critical HA systems.
```

**The DE rule:** *every* failover produces duplicates or gaps at the boundary. Design all sinks to be idempotent and order-tolerant (ch. 5), and store a source log position with each batch so you can detect and repair gaps.

---

## 9. Common Mistakes and How to Avoid Them

1. **Two-node HA clusters.** → Use odd sizes (3 or 5) for a real quorum.
2. **No fencing.** → Enforce fencing tokens at the resource; add STONITH as a backstop.
3. **Trusting a leader's self-check.** → The storage/clients must reject stale tokens.
4. **Automatic failover with aggressive timeouts.** → Tune timeouts; add jitter; separate heartbeat networks.
5. **Promoting an async replica without knowing the RPO.** → Use sync/semi-sync for critical writes; document the loss window.
6. **Hardcoded leader addresses.** → Service discovery + proxies; verify TTLs and pool reconnection.
7. **Forgetting client retries after failover.** → Idempotency keys, jittered backoff, retry budgets.
8. **Not excluding delayed/hidden replicas from election.** → They'd roll data backward; mark them ineligible.
9. **Skipping the rejoin procedure for the old leader.** → `pg_rewind`/re-clone; never let it replay the wrong history.
10. **No failover drills.** → Test monthly, under load, with a recorded RTO/RPO.
11. **Assuming Kafka is safe by default.** → Set `min.insync.replicas` and `acks=all` for critical topics; disable unclean election.
12. **Not preserving replication slots/log retention through failover.** → Followers/connectors may need a full resync; budget for it.
13. **Ignoring the post-failover load spike.** → Warm caches; rate-limit retries; scale before you fail over.
14. **Treating the promoted node's clock/state as authoritative for CDC.** → Use the log position (LSN/GTID/oplog ts), not wall-clock time.

---

## 10. Exercises / Interview Questions

> Hints only — no full answers inline.

1. **Election safety.** In Raft, why must a candidate's log be at least as up-to-date as the voter's before it gets a vote? *(Hint: prevents a stale leader from erasing committed entries.)*
2. **Split-brain anatomy.** Describe a two-node cluster with no fencing under a partition. How many leaders can exist, and what data ends up where? *(Hint: both, divergent; reconciliation nightmare.)*
3. **Fencing tokens.** Why must the *resource* enforce the token rather than the leader? Give an example of a "polite" check that fails. *(Hint: stale self-assessment; storage-side monotonicity.)*
4. **RPO/RTO.** For a payments service, choose a replication mode, election mechanism, and fencing, and state the resulting RPO/RTO. *(Hint: sync semi-quorum + automated election + tokens.)*
5. **Postgres promotion.** After promoting a standby, the old primary comes back with 5 seconds of extra writes. What do you do, and what tool repairs it? *(Hint: timeline divergence; pg_rewind.)*
6. **Patroni.** Explain how a lease in etcd prevents two primaries, and what happens when the old primary returns. *(Hint: TTL, compare-and-swap, demote + rewind.)*
7. **Kafka.** With RF=3, `min.insync.replicas=2`, two brokers down: what happens with `acks=all`, and how does `unclean.leader.election.enable` change it? *(Hint: producers block vs stale promotion and loss.)*
8. **Quorum sizes.** Why is a 4-node cluster no better than a 3-node cluster for failure tolerance? *(Hint: majority of 4 is 3; tolerate 1 either way.)*
9. **Election storm.** A cluster flaps leadership every few seconds under load. Enumerate causes and fixes. *(Hint: timeouts vs GC/heartbeat starvation; separate networks; throttle.)*
10. **Client behavior.** After a failover, your CDC connector keeps reading from the old primary. Why, and how do you fix it permanently? *(Hint: cached connection/DNS; service discovery; re-resolve on error.)*
11. **Exactly-once after failover.** Your stream processor wrote offsets and results in separate systems. A failover occurs between the two. Design a fix. *(Hint: transactional/atomic offset storage; idempotent upserts; at-least-once + dedupe.)*
12. **Distributed SQL.** How do CockroachDB/Spanner make failover per-range and nearly invisible, and what did they give up? *(Hint: Raft per range, leases, clocks; latency and complexity.)*
13. **Fencing vs leases.** Compare fencing tokens and revocable leases. What clock assumptions does each make? *(Hint: lease correctness depends on bounded clock drift; tokens don't.)*

---

## References

- Kleppmann, M. — *Designing Data-Intensive Applications*, Chapter 5 & 9 (Leader/follower replication, consensus, failover). https://dataintensive.net/
- Ongaro, D. & Ousterhout, J. — "In Search of an Understandable Consensus Algorithm (Raft)" (extended version): https://raft.github.io/raft.pdf
- Raft website (visualizations, implementations): https://raft.github.io/
- Lamport, L. — "The Part-Time Parliament" / "Paxos Made Simple": https://lamport.azurewebsites.net/pubs/paxos-simple.pdf
- Reed, B. & Junqueira, F. — "A Simple Totally Ordered Broadcast Protocol" (Zab): https://www.datadoghq.com/pdf/zab-web.pdf
- PostgreSQL Documentation — High availability, load balancing, and replication: https://www.postgresql.org/docs/current/high-availability.html
- PostgreSQL Documentation — `pg_rewind`: https://www.postgresql.org/docs/current/app-pgrewind.html
- Patroni Documentation — HA with etcd/Consul/ZooKeeper: https://patroni.readthedocs.io/
- Apache Kafka Documentation — Replication and the controller: https://kafka.apache.org/documentation/#replication
- Conduktor — Kafka `min.insync.replicas` and `unclean.leader.election.enable`: https://www.conduktor.io/kafka/kafka-topic-configuration-min-insync-replicas
- MongoDB Documentation — Replica set elections: https://www.mongodb.com/docs/manual/core/replica-set-elections/
- MongoDB Documentation — Write concern `w:majority`: https://www.mongodb.com/docs/manual/core/replica-set-write-concern/
- etcd Documentation — Raft and quorum: https://etcd.io/docs/latest/learning/
- AWS Documentation — RDS Multi-AZ deployments: https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Concepts.MultiAZ.html
- AWS Documentation — Aurora high availability and failover: https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/Concepts.AuroraHighAvailability.html
- Martin Kleppmann — "How to do distributed locking" (fencing tokens, Redlock critique): https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html
- Jepsen — Analyses (leader election, split-brain, data loss case studies): https://jepsen.io/analyses
