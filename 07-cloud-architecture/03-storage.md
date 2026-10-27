# Storage for Data Systems

> Storage is the foundation of every data platform and, paradoxically, the layer most
> engineers understand least. They know "S3" the way they know "the internet" — it just
> works — until a 5 TB full-table scan costs $25, a million tiny files collapse a job's
> throughput, a cache read returns nothing because replication lagged, or an egress bill
> arrives at 10× the storage bill. A senior engineer knows storage as a **spectrum of
> durability, availability, consistency, performance, and cost trade-offs**, and chooses
> the point on that spectrum per data zone.

## TL;DR

- **Three storage models:** block (a virtual disk, one writer, low latency), file (a shared
  POSIX hierarchy, many writers), object (flat keys + metadata over HTTP, effectively
  infinite, the backbone of data lakes).
- **Object storage is the system of record for data platforms** — because it is cheap,
  durable, and infinitely scalable — but it is not a filesystem, and treating it as one
  (millions of small files, no partitioning) is the #1 cause of slow, expensive pipelines.
- **Durability ≠ availability.** Durability is "will I lose the bytes" (S3 Standard: 99.999999999%
  designed durability). Availability is "can I read them right now" (SLA, e.g. 99.9%).
- **Storage classes/tiers are a cost dial.** Hot → cool → cold → archive trades retrieval
  latency and retrieval cost for a lower storage price. Lifecycle policies automate the slide.
- **Consistency is no longer the trap it was:** S3, GCS, and Azure Blob are all **strongly
  consistent** for reads-after-writes now. The remaining traps are *list* consistency patterns,
  eventual consistency in caches/DNS/replicas, and cross-region replication lag.
- **Data transfer is where surprises live.** Ingress is (usually) free; egress, cross-AZ,
  cross-region, and NAT throughput are billed. Design to keep bytes where they are consumed.
- **Formats beat systems.** Parquet/ORC + open table formats (Iceberg/Delta/Hudi) give you
  portability across engines and eliminate vendor lock-in at the storage layer.

## 1. Definitions and core vocabulary

### 1.1 The three storage models

| Model | Abstraction | Access | Latency | Typical use | Cloud service |
|---|---|---|---|---|---|
| **Block** | Raw fixed-size blocks on a volume | Attached to one VM (mostly) | µs–ms | Databases, boot volumes, high-IOPS | EBS, Managed Disks, Persistent Disk |
| **File** | POSIX directory tree, shared | Many clients, NFS/SMB | ms | Shared config, HPC, lift-and-shift apps | EFS, FSx, Azure Files, Filestore |
| **Object** | Flat namespace, key → bytes + metadata | HTTP REST, any client | ms–100 ms | Data lakes, backups, media, logs | S3, Azure Blob, GCS |

### 1.2 Core vocabulary

- **Bucket/container** — the top-level namespace in object storage (globally unique name).
- **Object key** — the full string name; "directories" are just prefixes (`logs/2024/03/…`).
- **Prefix** — a leading substring of the key; the unit of partitioning and request scaling.
- **Durability** — probability that stored data is not lost over a year (e.g. 11 nines).
- **Availability** — fraction of time the service answers requests (SLA, e.g. 99.9%).
- **Durability vs availability in practice:** 11 nines durability means you will essentially
  never lose data; it says nothing about whether you can read it *now*.
- **Storage class / tier / access tier** — a per-object cost/performance profile (S3 Standard,
  S3 Intelligent-Tiering, Glacier; Blob Hot/Cool/Cold/Archive; GCS Standard/Nearline/Coldline/Archive).
- **Lifecycle policy** — rules that transition or expire objects based on age/prefix/tags.
- **Multipart upload** — uploads split into parts, required for large objects, resumable.
- **Versioning** — keeps all versions of an object; the basis for recovery and for locking.
- **Consistency model** — what a read guarantees after a write. Strong = read reflects the
  latest write; eventual = may be stale briefly.
- **Erasure coding / replication** — how durability is achieved (multiple copies or k-of-n
  fragments).
- **Open table format** — a metadata layer over object storage providing ACID, schema
  evolution, time travel (Iceberg, Delta Lake, Hudi).
- **Data gravity** — data accumulates and attracts compute; the cost of moving it is the
  inertia.

## 2. What problem it solves / why it matters

### 2.1 Why object storage ate the data lake

Before object storage, a data lake was HDFS on a fixed cluster: capacity was a purchase, and
compute and storage were welded together. Object storage **separates storage from compute**:

- Storage is effectively infinite, elastic, and cheap. You never "run out of disk."
- Compute can be created and destroyed independently, in any size, per job.
- Durability is someone else's SLO (11 nines), so you stop running RAID across three racks.
- One copy can be read by Spark, a warehouse, a BI tool, and a Python notebook simultaneously.

### 2.2 Why a senior must know it

Three reasons:

1. **Cost.** Storage design (class, lifecycle, layout, file sizes) is often the biggest
   controllable line item after compute — and the scan cost of a bad layout dwarfs the
   storage cost of a good one.
2. **Performance.** Object stores have per-request and per-prefix limits; small files and
   unpartitioned scans are the most common cause of slow pipelines.
3. **Correctness and compliance.** Consistency, versioning, object lock, and encryption
   settings decide whether you can satisfy a regulator, recover from a bad deploy, or
   accidentally expose PII.

## 3. How it works internally (mechanisms, internals)

### 3.1 Block storage internals

A block volume is presented to an OS as a device (`/dev/nvme1n1`). The provider replicates it
**within an AZ** (e.g. EBS volumes are replicated across multiple servers within one AZ) for
durability. Key internals:

- **Zone-scoped.** An EBS volume lives in one AZ; you cannot attach it to an instance in
  another AZ (you snapshot and recreate).
- **Performance is provisioned.** IOPS and throughput are separate knobs (gp3 lets you set
  them independently of size). Burst credits on smaller volumes.
- **Snapshots are incremental** and stored in object storage; the first restore is slow
  (lazy-loading from snapshot), which matters for DR times.

```text
Instance ──attach──► Volume (AZ-scoped, replicated within AZ)
                          └── snapshot ──► S3 (regional object store)
```

### 3.2 File storage internals

File storage is a distributed filesystem: metadata servers + data servers, POSIX semantics
(locking, atomic rename, permissions). EFS is elastic (grows/shrinks with usage); FSx offers
Windows/Lustre/NetApp/OpenZFS flavours for specific workloads. Internals to know:

- **Lustre/NetApp/FSx for HPC** = high throughput, parallel access — used for ML training and
  HPC, not for a general data lake.
- **POSIX is expensive.** Atomic rename and locking require coordination; this is why object
  stores don't have it natively.
- **Performance modes** (e.g. EFS burst vs. provisioned throughput) must be matched to load.

### 3.3 Object storage internals

Object storage is a **distributed key-value store** with an HTTP front door. The mental model:

```text
PutObject("2024/03/01/part-00000.parquet", bytes)
  │  key ──hash──► partition ──► replicated/coded across devices & AZs (≥3 AZs)
  │  metadata: size, etag, content-type, storage-class, user metadata
  ▼
  durable, strongly consistent (read-after-write), no rename, no append
```

Consequences of this design:

- **No rename, no append.** "Move" = copy + delete (silently slow for large objects). "Update"
  = rewrite the whole object. This is *the* reason table formats (Iceberg/Delta) exist.
- **Prefix throughput scales, but only if you spread keys.** Historically, a single prefix
  had a request-per-second limit; modern S3 partitions automatically, but **hot key ranges**
  and sequential-prefix access patterns remain a real throughput risk. Randomize/partition
  keys for high-rate writes.
- **List operations are the slow path.** `ListObjects` with millions of objects is fine-ish,
  but using listing as a discovery mechanism in a hot path is an anti-pattern; use a catalog.
- **Multipart upload** splits large objects into parts, allowing parallelism and resume; a
  failed multipart leaves orphaned parts that still cost money unless aborted.
- **Erasure coding vs replication.** Replication (3+ copies) is simple but stores 3×. Erasure
  coding (e.g. 1.5× overhead) is how providers achieve 11 nines cheaply.

### 3.4 Durability and availability, precisely

```text
Durability: P(no data loss over a year)  → S3 Standard: 99.999999999% (11 nines)
Availability: P(service answers)         → S3 Standard SLA: 99.9%
             → S3 One Zone-IA: 99.5%, and data is lost if the AZ is destroyed
```

Design rule: **11 nines of durability does not protect you from yourself.** Deletion, bad
writes, and ransomware are the real threats. Mitigations: **versioning**, **MFA delete**,
**Object Lock (WORM)**, and **cross-region replication**.

### 3.5 Consistency: the modern reality

Since December 2020, **S3 provides strong read-after-write consistency** for all
operations, including list and overwrite. GCS and Azure Blob are likewise strongly consistent
for object reads after writes. So what still bites?

- **Caches and read replicas** (CloudFront, database replicas, Snowflake result cache) are
  eventually consistent by nature.
- **Cross-region replication (CRR)** lags — seconds to minutes. A reader in the DR region may
  not see the newest object.
- **List-after-write in *other* services** (Glue/Hive catalog updates, partition discovery)
  can lag; "write a partition then immediately query by list" may miss it.
- **Distributed system semantics in table formats** (Iceberg commits) rely on atomic pointer
  swaps; correctness depends on the store's strong consistency — which we now have.

> Senior note: the old advice "S3 is eventually consistent" is **stale**. Do not repeat it.
> The current advice is "the object store is strongly consistent, but everything *around* it
> — caches, replicas, catalogs, CDNs — may not be."

### 3.6 Storage classes and the cost/latency curve

```text
                    Cheaper storage  ──────────────────────────►
 S3:        Standard  →  Standard-IA  →  Glacier IR  →  Glacier Flexible → Glacier Deep Archive
 Retrieval: ms        →  ms           →  ms          →  min–hours       →  12 h
 Min dur.:  none      →  30 d         →  90 d        →  90 d            →  180 d
 Use:       hot data  →  infrequent   →  archive-now →  backup          →  compliance/compliance
```

Key internals:

- **Minimum storage duration** means you are billed for the minimum even if you delete
  earlier — a lifecycle rule that archives too aggressively can cost *more*.
- **Retrieval fees** can dominate: pulling a big dataset back from Archive is expensive and
  slow. Archive is for cold, rarely-read data.
- **Intelligent-Tiering** monitors access and moves objects automatically for a small
  monitoring fee — the right default when access patterns are unknown.
- **Small objects** in cold tiers are inefficient (per-object overhead); aggregate them.

## 4. How to implement it (step-by-step, with IaC/CLI/config examples)

### 4.1 Provision a hardened, lifecycle-managed bucket (Terraform)

```hcl
resource "aws_s3_bucket" "lake" {
  bucket = "acme-lake-prod"
}

resource "aws_s3_bucket_versioning" "lake" {
  bucket = aws_s3_bucket.lake.id
  versioning_configuration { status = "Enabled" }   # recovery from bad deletes/overwrites
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.data.arn
    }
    bucket_key_enabled = true          # cuts KMS request cost dramatically
  }
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id

  # raw zone → infrequent → glacier, then expire
  rule {
    id     = "raw-zone-tiering"
    status = "Enabled"
    filter { prefix = "raw/" }
    transition { days = 30  storage_class = "STANDARD_IA" }
    transition { days = 90  storage_class = "GLACIER_IR" }
    expiration { days = 730 }
    noncurrent_version_expiration { noncurrent_days = 30 }
  }

  # abort incomplete multipart uploads — invisible cost sink
  rule {
    id     = "abort-mpu"
    status = "Enabled"
    filter {}
    abort_incomplete_multipart_upload { days_after_initiation = 7 }
  }
}

resource "aws_s3_bucket_policy" "tls_only" {
  bucket = aws_s3_bucket.lake.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "DenyInsecure"
      Effect    = "Deny"
      Principal = "*"
      Action    = "s3:*"
      Resource  = [aws_s3_bucket.lake.arn, "${aws_s3_bucket.lake.arn}/*"]
      Condition = { Bool = { "aws:SecureTransport" = "false" } }
    }]
  })
}
```

### 4.2 The data-lake zone layout (layout *is* architecture)

```text
s3://acme-lake-prod/
├── raw/            # immutable landing, partitioned by ingest date + source
│   └── source=orders/dt=2024-03-01/hour=13/part-*.parquet
├── bronze/         # cleaned, deduplicated, still append-only
│   └── orders/dt=2024-03-01/...
├── silver/         # conformed, joined, Iceberg/Delta tables
│   └── orders/ (Iceberg table)
├── gold/           # aggregates / marts for BI
│   └── daily_sales/ (Iceberg table)
└── sandbox/        # scratch, aggressively lifecycle-expired
```

Two things to internalize: **partition by the column you filter on** (`dt`), and **keep files
100–500 MB** (see anti-patterns).

### 4.3 CLI operations you will actually use

```bash
# Efficient parallel sync (multi-threaded) — NOT a rename, it's copy
aws s3 sync ./data s3://acme-lake-prod/raw/source=orders/dt=2024-03-01/ \
  --only-show-errors

# Large-file upload with parallelism (multipart)
aws s3 cp big.parquet s3://acme-lake-prod/raw/ \
  --expected-size 8000000000

# Cost/usage visibility
aws s3api list-objects-v2 --bucket acme-lake-prod --prefix raw/ \
  --query 'Contents[].[Key,Size,StorageClass]' --output table

# Enforce object lock for compliance (WORM)
aws s3api put-object-lock-configuration --bucket acme-lake-prod \
  --object-lock-configuration 'ObjectLockEnabled=Enabled,Rule={DefaultRetention={Mode=COMPLIANCE,Days=3650}}'

# Observe at-rest encryption/config status
aws s3api get-bucket-encryption --bucket acme-lake-prod
```

```bash
# GCP equivalents
gcloud storage buckets create gs://acme-lake-prod --location=europe-west1 --uniform-bucket-level-access
gcloud storage cp -r ./data gs://acme-lake-prod/raw/ --gzip-in-flight

# Azure equivalents
az storage container create --name raw --account-name acmelake
az storage blob upload-batch --destination raw --source ./data --account-name acmelake
```

### 4.4 Iceberg table on object storage (the modern lakehouse unit)

```sql
CREATE TABLE silver.orders (
  order_id      BIGINT,
  customer_id   BIGINT,
  amount        DECIMAL(12,2),
  order_ts      TIMESTAMP,
  country       STRING
)
USING iceberg
PARTITIONED BY (days(order_ts), bucket(16, customer_id))
TBLPROPERTIES (
  'write.target-file-size-bytes' = '268435456',   -- 256 MB target file
  'write.upsert.enabled' = 'true'
);
```

Partitioning choice matters: **time partition for pruning + hash bucket for join/skew control.**

### 4.5 Lifecycle automation as code (GCS example)

```bash
gcloud storage buckets update gs://acme-lake-prod --lifecycle-file=lifecycle.json
```

```json
{
  "rule": [
    { "action": {"type": "SetStorageClass", "storageClass": "NEARLINE"},
      "condition": {"age": 30, "matchesPrefix": ["raw/"]} },
    { "action": {"type": "SetStorageClass", "storageClass": "COLDLINE"},
      "condition": {"age": 90, "matchesPrefix": ["raw/"]} },
    { "action": {"type": "Delete"}, "condition": {"age": 730, "matchesPrefix": ["sandbox/"]} }
  ]
}
```

## 5. Where to implement it (AWS / Azure / GCP equivalents)

### 5.1 Storage service map

| Capability | AWS | Azure | GCP |
|---|---|---|---|
| Object storage | S3 | Blob Storage | Cloud Storage |
| Hierarchical namespace | S3 (prefixes; S3 Express) | **ADLS Gen2 (HNS)** | GCS (prefixes) |
| Hot tier | S3 Standard | Blob Hot | GCS Standard |
| Infrequent/cool | Standard-IA | Blob Cool | Nearline |
| Cold | Glacier Instant Retrieval | Blob Cold | Coldline |
| Archive | Glacier Flexible / Deep Archive | Blob Archive | Archive |
| Auto-tiering | Intelligent-Tiering | Lifecycle + auto-tier | Autoclass |
| Block storage | EBS | Managed Disks | Persistent Disk / Hyperdisk |
| Fast local disk | Instance store NVMe | Local SSD / temp disk | Local SSD |
| Shared file (NFS) | EFS | Azure Files | Filestore |
| HPC file | FSx for Lustre | Managed Lustre / NetApp | Filestore / NetApp |
| Windows file | FSx for Windows | Azure Files (SMB) | — (use NetApp/Filestore) |
| Object lock/WORM | S3 Object Lock | Immutable Blob Storage | Bucket Lock |
| Cross-region replication | S3 CRR / SRR | GRS / RA-GRS / object replication | Dual-region / Transfer Service |
| Snapshot/backup | EBS Snapshots / Backup | Snapshots / Backup Vault | Snapshots / Backup & DR |

### 5.2 Blob/object API compatibility

S3's API is the de-facto standard; GCS and Azure both expose S3-compatible XML APIs, and
there are S3-compatible gateways for on-prem. This means **compute is portable** if your
storage interface is S3-compatible — but semantics (consistency of lists, Object Lock
behaviour, multipart details) differ subtly. Test conformance before assuming drop-in parity.

### 5.3 ADLS Gen2's hierarchical namespace (the notable Azure difference)

ADLS Gen2 adds a **real directory namespace** on top of Blob, which enables **atomic
directory rename** — a genuine advantage for Spark jobs that do `_temporary` → final rename.
On AWS/GCP, that pattern causes a slow copy. If you run Spark on Azure, enable HNS.

## 6. Patterns and anti-patterns

### 6.1 Patterns

- **Medallion zones** (raw/bronze/silver/gold) with different lifecycle policies per zone.
- **Compaction** of small files on a schedule (or automatic in Iceberg/Delta/Hudi).
- **Partition by filter column; cluster/order by a high-cardinality column.**
- **Target file size 100–500 MB**; aim for ≤ a few hundred files per partition.
- **Versioning + Object Lock** for compliance and ransomware resilience.
- **Intelligent-tiering / Autoclass** when access patterns are unknown.
- **Bucket keys (S3)/CMEK batching** to cut KMS cost.
- **Multipart cleanup rules** to abort orphaned uploads.
- **Cross-account, cross-region replication** for DR, with documented RPO.

### 6.2 Anti-patterns

- **The small-file problem.** Writing thousands of tiny files per partition → per-file
  overhead, listing storms, and a driver that spills.
- **Unpartitioned scans.** `SELECT * FROM huge_table` with no partition filter = scanning
  everything (and paying for every byte).
- **Treating object storage as a filesystem.** Frequent renames/appends fail or cost 10×.
- **Putting hot data in Archive.** Retrieval fees + hours of latency.
- **Lifecycle transitions that cost more than they save** (ignoring minimum duration).
- **One giant prefix** for high-throughput writes (`prefix=events/` for millions of objects/s).
- **A bucket per tenant with no tag/account discipline** → unallocatable cost.
- **Reading data across regions to run compute** → egress + latency.
- **Relying on eventual consistency folklore** to gate designs (it's strongly consistent now).
- **No versioning** → a bad `overwrite` is unrecoverable.
- **Unencrypted or publicly accessible buckets.** See chapter 6.

## 7. Real-world use cases and industry applications

- **Data lakehouse (most enterprises):** raw → curated → marts on object storage with Iceberg;
  one copy read by Spark, warehouse, and BI.
- **Media archives:** petabytes in Glacier/Archive, restored on request for compliance or
  re-monetization; WORM for contractual retention.
- **Backups and DR:** incremental snapshots + cross-region replication; object lock to defeat
  ransomware.
- **IoT/telemetry:** high-rate time-series writes into time-partitioned prefixes, then
  downsample and tier.
- **ML feature stores:** features written as Parquet to object storage, read by training jobs
  and served (via a key-value layer) online.
- **Financial tick data:** immutable append-only Parquet, partition-pruned for backtests;
  archive tiers after N months.

## 8. Senior-level deep dive: trade-offs, architecture decisions, performance, failure modes, debugging, cost

### 8.1 Trade-off tables

**Durability vs. availability vs. cost (S3 example, conceptual):**

| Class | Designed durability | Availability SLA | Relative storage cost | Retrieval |
|---|---|---|---|---|
| Standard | 11 nines | 99.9% | $$$ | free |
| One Zone-IA | 11 nines | 99.5% | $$ | per-GB |
| Standard-IA | 11 nines | 99.9% | $$ | per-GB |
| Glacier IR | 11 nines | 99.9% | $ | per-GB |
| Glacier Flexible | 11 nines | 99.99% (after restore) | ¢ | per-GB + restore |
| Deep Archive | 11 nines | 99.99% | ¢ | per-GB + restore, 12 h |

**Block vs file vs object, for data workloads:**

| Need | Choose |
|---|---|
| A database data directory | Block |
| Shared config/scripts across nodes | File |
| The data lake, backups, media | Object |
| ML training shuffle over huge training set | File (Lustre) or object + caching |
| Boot volume | Block |

### 8.2 The economics you must be able to compute

```text
Monthly storage cost  = Σ (GB in class C × price(C)) + requests + retrieval + transfer
Scan cost (Athena/BQ) = bytes_scanned × price_per_TB
Egress cost           = GB_out × price_per_GB
Request cost          = (PUT + GET + LIST + ...) × price_per_1000
```

Two worked intuitions (illustrative prices — always check current):

- Storing 100 TB in S3 Standard ~ $2,300/month; the same data in Glacier Deep Archive
  ~ $100/month — a 20×+ difference that makes archival trivially worth automating.
- Scanning 100 TB once in Athena at ~$5/TB = **$500 for one query**. Partitioning so you scan
  1 TB instead = $5. **The layout decision is worth more than the storage decision.**

### 8.3 Performance internals

- **Request rate per prefix.** Even with automatic partitioning, extremely hot prefixes and
  sequential (monotonically increasing) key names can hotspot. Add randomness or use
  time-bucketed prefixes.
- **Concurrency limits.** A single Spark read of one file is single-threaded; parallelism
  comes from many files/byte-ranges. This is why one 50 GB file is *worse* than 200 × 250 MB.
- **List performance.** Data catalogs exist precisely so jobs don't `LIST` enormous prefixes.
- **Throughput per instance.** A single VM can saturate its network long before the store
  does; scale readers horizontally.

### 8.4 Failure modes and debugging

| Symptom | Likely cause | Debug |
|---|---|---|
| Spark job stuck at 99%, one task slow | Skew / one huge file | `spark.sql.files.maxPartitionBytes`; repartition |
| Job slower after "optimizing" | Too many small files / listing | Check file count per partition; compaction |
| `SlowDown` / 503 from object store | Request rate hotspot | Spread prefixes; backoff |
| "Data is missing" after a write | Reading a replica/cache/catalog that lags | Read from primary; refresh catalog |
| Recover a deleted/overwritten object | No versioning | Enable versioning; restore via versions |
| Restore from Glacier slow | Retrieval tier | Use Expedited/Standard restore; plan RTO |
| Egress bill blowup | Cross-region reads, NAT, CDN miss | Flow logs, VPC endpoints, co-locate |

Debugging approach: **count first, then size.** Most storage-related pipeline problems are
"how many files" and "how many bytes," not "is the store down." `aws s3api list-objects-v2
--prefix ... | wc -l` and per-partition size histograms answer 80% of questions.

### 8.5 Architecture decisions (the ones that matter)

1. **Object storage as the system of record?** Almost always yes for analytics. Exception:
   sub-millisecond OLTP (block-backed DB) and specialized HPC.
2. **Table format?** Yes for anything mutable — Iceberg (interoperable) or Delta (Spark-native).
3. **Partitioning strategy?** Driven by the dominant filter; validate with query logs.
4. **Lifecycle policy?** Tier aggressively for cold data, but price-check minimum durations.
5. **Replication?** Single-region until compliance/RPO requires cross-region.
6. **Encryption?** SSE-KMS with bucket key by default; CMEK if regulation demands key custody.

## 9. Common mistakes and how to avoid them

| Mistake | Impact | Fix |
|---|---|---|
| Millions of small files | Slow lists, slow jobs, high request cost | Compact to 100–500 MB; run compaction |
| No partitioning | Full scans, huge scan bills | Partition by date/filter column |
| Over-partitioning (too many tiny partitions) | Metadata explosion, scheduler overhead | Partition wisely; use clustering for the rest |
| Hot single prefix | Throttling | Spread keys / time-bucket prefixes |
| Archive for warm data | Restore fees + latency | Match class to access frequency |
| No versioning | Unrecoverable overwrite/delete | Enable versioning + MFA delete |
| No multipart abort rule | Silent storage cost | Lifecycle abort rule |
| Ignoring minimum duration | Transition costs exceed savings | Model the lifecycle before applying it |
| Cross-region reads | Egress + latency | Co-locate compute; replicate only what's needed |
| Public bucket | Breach | Block public access; policy conditions |
| One bucket for everything | Unallocatable cost, blobby IAM | Bucket-per-domain; tag + policy |
| Assuming eventual consistency | Wrong design conclusions | Trust current strong-consistency docs |

## 10. Exercises / interview questions (hints only)

1. **You store 500 TB of logs, 2% read per day.** Design storage classes + lifecycle and
   estimate the savings. *(Hint: hot tier for the window read; IA/Glacier beyond; minimum
   durations.)*
2. **A nightly job writes 200,000 files of 1 MB.** What breaks and how do you fix it?
   *(Hint: listing, per-file overhead, compaction, target file size.)*
3. **Why does `mv` on S3 take time for a 50 GB object?** *(Hint: no rename; copy + delete.)*
4. **Explain why an unpartitioned Athena query is expensive** and how partitioning fixes it.
   *(Hint: bytes scanned pricing.)*
5. **Design a DR strategy with RPO 15 min / RTO 1 h** for an S3-based lake. *(Hint: CRR,
   replication time control, restore drills.)*
6. **When would you choose FSx for Lustre over S3?** *(Hint: HPC/ML shuffle, POSIX, throughput.)*
7. **Your egress bill tripled.** List five investigations. *(Hint: cross-region reads, NAT,
   CDN, replication, client downloads.)*
8. **Iceberg vs Delta vs Hudi:** name one workload where each wins. *(Hint: interoperability,
   Spark-centric, streaming upserts.)*

## References

- Amazon S3 storage classes — https://docs.aws.amazon.com/AmazonS3/latest/userguide/storage-class-intro.html
- Amazon S3 consistency model — https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html#ConsistencyModel
- Amazon S3 lifecycle configuration — https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lifecycle-mgmt.html
- Amazon S3 pricing — https://aws.amazon.com/s3/pricing/
- Amazon EBS volume types — https://docs.aws.amazon.com/ebs/latest/userguide/ebs-volume-types.html
- Amazon EFS — https://docs.aws.amazon.com/efs/latest/ug/whatisefs.html
- FSx for Lustre — https://docs.aws.amazon.com/fsx/latest/LustreGuide/what-is.html
- AWS S3 durability and the shared responsibility model — https://docs.aws.amazon.com/AmazonS3/latest/userguide/DataDurability.html
- Azure Blob access tiers — https://learn.microsoft.com/en-us/azure/storage/blobs/access-tiers-overview
- ADLS Gen2 hierarchical namespace — https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-namespace
- Azure managed disks — https://learn.microsoft.com/en-us/azure/virtual-machines/managed-disks-overview
- Google Cloud Storage classes — https://cloud.google.com/storage/docs/storage-classes
- GCS consistency — https://cloud.google.com/storage/docs/consistency
- GCS lifecycle management — https://cloud.google.com/storage/docs/lifecycle
- Apache Iceberg — https://iceberg.apache.org/docs/latest/
- Delta Lake — https://docs.delta.io/latest/index.html
- Apache Hudi — https://hudi.apache.org/docs/overview/
- AWS Prescriptive Guidance: S3 performance — https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance.html
