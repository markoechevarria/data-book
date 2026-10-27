# Managed Data Services: Comparison and Selection

> The cloud's core value proposition for data engineers is not "rented servers" — it is the
> catalog of **managed data services**: warehouses, lake catalogs, streaming buses, ETL
> engines, and Spark platforms that come with someone else's SRE team attached. Knowing the
> names is not the skill; knowing **which one to pick, when, and why** is. A senior engineer
> can look at a requirement (a 2 s streaming SLA, a 40 TB nightly aggregate, 500 BI users,
> GDPR residency) and land on the right service — and justify it against two alternatives —
> in a few minutes.

## TL;DR

- **Separate the layers:** ingest/streaming, storage/catalog, processing/query, orchestration.
  Each layer has a distinct set of managed services, and you should pick per layer, not per
  vendor marketing.
- **Warehouses and query engines differ in cost model, not just SQL:** provisioned cluster
  (Redshift, Synapse Dedicated), credit/time-based (Snowflake), slot/scan-based (BigQuery),
  scan-based serverless (Athena). The pricing model *is* the architecture.
- **The lakehouse has converged:** object storage + open table format (Iceberg/Delta/Hudi) +
  a catalog + any of several compute engines. This is now the default for analytics, and it
  decouples compute choice from data location.
- **Streaming buses:** Kinesis, Event Hubs (Kafka-compatible), Pub/Sub for managed ingestion;
  MSK / Confluent Cloud / Event Hubs / Pub/Sub for Kafka-compatible ecosystems.
- **Managed Spark:** EMR (Serverless), Dataproc (Serverless), Databricks, Synapse/HDInsight —
  choose by engine affinity, runtime cost, and ecosystem, not by "which is fastest" folklore.
- **Selection is driven by five questions:** workload shape, concurrency, latency SLA, cost
  model fit, and ecosystem/lock-in tolerance.
- **The catalog is the keystone.** Glue Data Catalog / Dataplex / Unity Catalog decide whether
  your engines can share data. Ignore it and you build silos.
- **Serverless is the default, provisioned is the exception** — provision only for steady,
  high-utilization, latency-sensitive, or cost-predictable workloads.

## 1. Definitions and core vocabulary

- **Managed service** — a provider-operated service exposing APIs; you configure, they run.
  The antithesis is self-managed (Kafka on VMs, Postgres on EC2).
- **OLTP database** — row-oriented, transactional, low-latency point operations (RDS, Aurora,
  Cloud SQL, Azure SQL, AlloyDB).
- **OLAP warehouse** — columnar, massive parallel processing, analytic queries (Redshift,
  BigQuery, Snowflake, Synapse Dedicated).
- **Query engine / serverless SQL** — query a lake without loading it (Athena, Trino/Presto,
  BigQuery external tables, Synapse serverless, Fabric).
- **Catalog / metastore** — table + schema + partition metadata (Glue Data Catalog, Hive
  Metastore, Dataplex, Unity Catalog, Purview).
- **Lakehouse** — object storage + open table format + catalog + multiple engines.
- **Streaming bus / log** — durable, ordered, partitioned append-only event store (Kinesis,
  MSK, Event Hubs, Pub/Sub).
- **Managed Spark** — a managed runtime for distributed data processing (EMR, Dataproc,
  Databricks, Synapse Spark).
- **ETL/orchestration service** — managed pipeline execution (Glue jobs, Data Factory, Dataflow,
  Composer/MWAA/ADF).
- **Serverless** — capacity is provisioned per request/job; scaling to zero is available.
- **Concurrency scaling / slots / warehouses** — the mechanisms each warehouse uses to add
  capacity: extra clusters (Redshift), slots (BigQuery), warehouses (Snowflake).
- **Compute-storage separation** — the architectural shift enabling independent scaling and
  pricing of compute and storage (Redshift RA3, Aurora, Snowflake, BigQuery).
- **DBU / DPU / credit / slot-hour** — the vendor-specific units of billed work.

## 2. What problem it solves / why it matters

### 2.1 Build vs. buy

Self-managing a data service means owning: provisioning, patching, backups, replication,
failover, scaling, security hardening, upgrades, and 24/7 paging. For most organizations this
is **undifferentiated heavy lifting** — it does not create product value. Managed services
convert these costs into configuration and an API call.

The counter-argument (legitimate) is:
- **Cost at scale:** managed services carry a premium that grows with volume; at extreme
  scale, self-managed can be cheaper.
- **Control:** engine versions, tuning knobs, extensions, and internals may be gated.
- **Portability:** deep use of a managed service's quirks creates lock-in.

### 2.2 Why a senior must know the service landscape

Because **service selection is architecture**. Choosing Snowflake vs. BigQuery vs. Redshift
determines cost model, scaling behavior, concurrency handling, failure modes, and how you
write SQL. Choosing Kinesis vs. MSK determines your consumer semantics and your ops burden.
Choosing Databricks vs. EMR determines your development model. These are not cosmetic
choices; they are multi-year commitments, so being able to reason about them is a core
senior competence.

## 3. How it works internally (mechanisms, internals)

### 3.1 OLTP databases (RDS / Aurora / Cloud SQL)

Internals: a primary instance with synchronous **Multi-AZ standby** (block-level replication
to a second AZ) or, in Aurora, **shared, log-structured storage replicated 6-way across 3
AZs** with multiple compute endpoints.

```text
RDS Multi-AZ:   App ──► Primary (AZ a) ──sync block replication──► Standby (AZ b)
                          └── async read replica (optional)

Aurora:         App ──► Writer instance ─┐
                  App ──► Reader instance ├──► Shared distributed log+storage
                  App ──► Reader instance │     (6 copies / 3 AZs)
                                         ┘     only redo log is shipped to storage
```

Aurora's key internal trick: **only the redo log is written to storage**, and storage nodes
apply it. This makes writes cheaper and replicas faster (low replica lag), and the storage
layer *is* the replication. This is why Aurora failover is fast and why its storage grows
automatically.

### 3.2 Warehouses: three fundamentally different internal designs

**(a) Redshift — provisioned MPP cluster with RA3.**
A leader node parses/plans; compute nodes store columnar blocks locally and/or on Redshift
Managed Storage (S3) in the RA3 architecture. Data is distributed by a dist key and sorted by
a sort key; the query optimizer exploits both. Internals that matter: **distribution style**
(KEY/ALL/EVEN/AUTO), **sort keys**, **zone maps** (block-level min/max → skip), **materialized
views**, **result cache**, and **Concurrency Scaling** (transient extra clusters for burst
reads). RA3 separates compute and storage, so scaling compute doesn't rescale storage.

**(b) BigQuery — serverless, disaggregated, Dremel.**
BigQuery is a multi-tenant, serverless engine: a **slot** is a unit of compute. Queries run on
a tree of mixers/workers executing over **Capacitor** (columnar) storage in **Colossus**
(distributed filesystem). Internals: **column-clustered storage**, **dynamic query plan
resizing**, **shuffle tiers**, **materialized views**, **BI Engine** (in-memory cache), and
**reservations** (dedicated slots for isolation). The user never sizes a cluster; they buy
slots (on-demand or committed) and can assign reservations to projects.

**(c) Snowflake — separated storage/compute with virtual warehouses.**
Storage is a single micro-partitioned columnar store on object storage; **virtual warehouses**
are independent compute clusters that cache micro-partitions locally. Internals: **micro-partition
pruning** (min/max metadata), **clustering keys**, **result cache**, **warehouse cache**,
**multi-cluster warehouses** (auto-scale for concurrency), **zero-copy clone**, **time travel**,
and **Snowpark**/native app runtime. You pay credits per second the warehouse runs, plus
storage per TB-month.

**Comparison of internals:**

| Aspect | Redshift | BigQuery | Snowflake |
|---|---|---|---|
| Compute unit | provisioned nodes / serverless RPU | slot | virtual warehouse (credit/sec) |
| Storage unit | managed storage (S3) + local | Colossus (managed) | micro-partitions on object storage |
| Cluster model | per-cluster (RA3), concurrency scaling | multi-tenant + reservations | per-warehouse clusters + multi-cluster |
| Isolation knob | separate cluster / workload mgmt | reservations + slot assignments | separate warehouse |
| Concurrency handling | concurrency scaling | slot autoscaling | multi-cluster warehouse |
| Scaling granularity | node type + count | slots | warehouse size + clusters |
| Predicate skipping | sort keys + zone maps | clustering + partitions | micro-partition pruning + clustering |

### 3.3 Query engines / serverless SQL

**Athena** executes Presto/Trino (engine v3) over data in S3 (Parquet/ORC/JSON + Iceberg),
using the Glue Data Catalog. You pay **per byte scanned**. It is serverless and stateless:
no cluster, but a query start latency and a per-query concurrency limit. **Athena for Apache
Spark** adds managed Spark notebooks. Key internal facts: partition pruning and column
projection directly cut bytes scanned → directly cut cost; small files hurt; a workgroup
configures output location, encryption, and cost-control per-query limits.

**BigQuery external tables / BigLake** and **Synapse serverless** are the analogues: query
object storage without loading, paying per scanned byte, with the same "layout is cost"
logic. **Trino/Presto** self-managed or via a managed offering (Starburst, EMR on EKS) gives
the same model with more control.

### 3.4 Catalogs

Glue Data Catalog (Hive-compatible, multi-engine), Dataplex (GCP), Unity Catalog
(Databricks, cross-cloud), Microsoft Purview (governance + metadata). The catalog stores
databases, tables, schemas, partition metadata, and increasingly **fine-grained access
control** and **lineage**. Internally it's a metadata service with an API; **the catalog is
what makes one copy of data readable by many engines** — this is the whole point of the
lakehouse.

### 3.5 Streaming buses

**Kinesis Data Streams** — shards, ordered per shard, 24 h–365 d retention, per-shard
throughput limits; KCL/KDA for consumption; enhanced fan-out for dedicated throughput.
**MSK** — real Apache Kafka (broker == Kafka broker), full Kafka API, topics/partitions/
offsets, broker sizing, storage, and version management; MSK Serverless removes broker ops.
**Event Hubs** — Kafka-protocol-compatible premium/basic tiers, partitions, throughput units
or processing units, capture to Blob. **Pub/Sub** — global, serverless, at-least-once
messages with ordering keys and no partitions to manage (a different mental model from Kafka).

```text
Kafka mental model:   partitions (ordering unit) + offsets (consumer position) + replication
Kinesis model:        shards (ordering unit) + sequence numbers + fan-out consumers
Pub/Sub model:        topics + subscriptions (per-consumer) + at-least-once delivery
```

### 3.6 Managed Spark platforms

- **EMR / EMR Serverless** — managed Hadoop/Spark/Hive/Presto/Trino/Flink; instance fleets,
  managed scaling, EMR Studio. Serverless removes cluster sizing (worker sizes + pre-init).
- **Dataproc / Dataproc Serverless** — GCP's managed Spark/Hadoop; integrates with BigQuery
  and GCS; serverless removes cluster ops.
- **Databricks** — optimized Spark (Photon vectorized engine), notebooks, Delta Lake, Unity
  Catalog, Delta Live Tables, jobs, SQL warehouses; runs on all three clouds.
- **Synapse Spark / HDInsight / Fabric** — Azure's managed Spark; Fabric unifies lakehouse +
  warehouse + notebooks.

### 3.7 ELT/orchestration services

Glue (serverless Spark + Python shell + Data Catalog + crawlers), Data Factory (Azure,
connector-rich orchestration with mapping data flows), Dataflow (GCP, Apache Beam, unified
batch/stream), Cloud Composer / MWAA / ADF / Airflow-managed (DAG orchestration), dbt (SQL
transformations), Step Functions/Logic Apps/Workflows (state machines).

## 4. How to implement it (step-by-step, with IaC/CLI/config examples)

### 4.1 A reference lakehouse on AWS (Terraform + SQL)

```hcl
# 1. Object storage zones (see ch. 3) — data at rest
# 2. Glue Data Catalog database for the lake
resource "aws_glue_catalog_database" "lake" {
  name = "lake_silver"
}

# 3. Crawler to register schema for an S3 prefix
resource "aws_glue_crawler" "orders" {
  name          = "orders_crawler"
  database_name = aws_glue_catalog_database.lake.name
  role          = aws_iam_role.glue.arn
  s3_target { path = "s3://acme-lake-prod/silver/orders/" }
  schema_change_policy { update_behavior = "UPDATE_IN_DATABASE" }
}

# 4. Athena workgroup with a per-query byte limit (cost guardrail)
resource "aws_athena_workgroup" "analytics" {
  name = "analytics"
  configuration {
    enforce_workgroup_configuration    = true
    bytes_scanned_cutoff_per_query     = 10_737_418_240  # 10 GB — kills accidental full scans
    result_configuration {
      output_location    = "s3://acme-athena-results/"
      encryption_configuration { encryption_option = "SSE_S3" }
    }
  }
}
```

```sql
-- Athena over Iceberg in the Glue Catalog
CREATE TABLE lake_silver.orders (
  order_id BIGINT, customer_id BIGINT, amount DECIMAL(12,2), order_ts TIMESTAMP
)
LOCATION 's3://acme-lake-prod/silver/orders'
TBLPROPERTIES ('table_type'='ICEBERG','format'='parquet','write_compression'='zstd');

-- A partitioned scan that prunes (cheap) — note the dt filter
SELECT date_trunc('day', order_ts) AS d, sum(amount)
FROM   lake_silver.orders
WHERE  dt BETWEEN DATE '2024-03-01' AND DATE '2024-03-07'   -- pruning
GROUP  BY 1 ORDER BY 1;
```

### 4.2 Snowflake: warehouses, resource monitors, auto-suspend

```sql
CREATE WAREHOUSE wh_etl WITH
  WAREHOUSE_SIZE = 'LARGE'
  AUTO_SUSPEND = 60          -- seconds idle before suspend → the #1 cost lever
  AUTO_RESUME  = TRUE
  INITIALLY_SUSPENDED = TRUE
  MAX_CLUSTER_COUNT = 4      -- multi-cluster for concurrency
  SCALING_POLICY = 'ECONOMY';

CREATE RESOURCE MONITOR rm_etl WITH CREDIT_QUOTA = 2000
  TRIGGERS ON 80 PERCENT DO NOTIFY
           ON 100 PERCENT DO SUSPEND_IMMEDIATE;
ALTER WAREHOUSE wh_etl SET RESOURCE_MONITOR = rm_etl;

-- Separate warehouses per workload = the isolation pattern
CREATE WAREHOUSE wh_bi  WAREHOUSE_SIZE='SMALL'  AUTO_SUSPEND=120 MAX_CLUSTER_COUNT=10;
CREATE WAREHOUSE wh_adhoc WAREHOUSE_SIZE='XSMALL' AUTO_SUSPEND=60;
```

### 4.3 BigQuery: reservations and slot isolation

```bash
# Purchase a commitment, create a reservation, assign a project (capacity isolation)
bq mk --reservation --location=EU --slots=500 --commitment_plan=ANNUAL etl_reservation
bq mk --reservation_assignment --reservation_id=etl_reservation \
  --job_type=QUERY --assignee_id=my-project --assignee_type=PROJECT
```

```sql
-- Partition + cluster for pruning (cost = bytes scanned)
CREATE TABLE analytics.orders
PARTITION BY DATE(order_ts)
CLUSTER BY customer_id AS
SELECT * FROM staging.orders;

-- See where the bytes went (cost debugging)
SELECT job_id, total_bytes_billed/1e12 AS tb, user_email, query
FROM `region-eu`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
ORDER BY total_bytes_billed DESC LIMIT 20;
```

### 4.4 Redshift: cluster, distribution, sort, workload management

```sql
CREATE TABLE sales (
  sale_id BIGINT, customer_id BIGINT, region VARCHAR(20),
  amount DECIMAL(12,2), sale_ts TIMESTAMP
)
DISTSTYLE KEY DISTKEY (customer_id)         -- co-locate joins on customer_id
COMPOUND SORTKEY (sale_ts, customer_id);    -- zone maps skip blocks by date

-- Isolate ETL (heavy) from BI (concurrent) with WLM queues
-- Configure via the console/CLI: separate queues, memory %, concurrency, and enable
-- Concurrency Scaling for the BI queue.
```

### 4.5 Streaming: MSK + Kinesis ingestion

```bash
# Create a Kafka cluster (MSK) in private subnets
aws kafka create-cluster --cluster-name events \
  --broker-node-group-info 'InstanceType=kafka.m7g.large,ClientSubnets=subnet-a,subnet-b,subnet-c,
      StorageInfo={EbsStorageInfo={VolumeSize=500}}' \
  --kafka-version 3.6.0 --number-of-broker-nodes 3

# Kinesis stream with on-demand capacity + long retention
aws kinesis create-stream --stream-name clickstream --stream-mode-details StreamMode=ON_DEMAND \
  --retention-period-hours 168
```

```python
# Spark Structured Streaming reading from Kinesis (EMR/Glue) and writing Iceberg
df = (spark.readStream.format("kinesis")
        .option("streamName", "clickstream")
        .option("startingPosition", "LATEST")
        .load())
(df.writeStream
   .format("iceberg")
   .outputMode("append")
   .option("checkpointLocation", "s3://acme-lake-prod/_chk/clickstream")
   .trigger(processingTime="1 minute")
   .toTable("lake_silver.clickstream"))
```

### 4.6 Cross-cloud equivalents in one table (see §5 for the full map)

```bash
# RDS → Cloud SQL → Azure SQL/PostgreSQL
aws rds create-db-instance --db-instance-class db.r6g.large --engine postgres ...
gcloud sql instances create pg --tier=db-custom-2-7680 --database-version=POSTGRES_16
az postgres flexible-server create --sku-name Standard_D2ds_v5 ...

# Warehouse
aws redshift create-cluster --node-type ra3.xlplus --number-of-nodes 4 ...
bq mk --dataset analytics
az synapse workspace create ...   # then a Dedicated SQL pool
```

## 5. Where to implement it (AWS / Azure / GCP equivalents)

### 5.1 The full managed-data-service map

| Category | AWS | Azure | GCP |
|---|---|---|---|
| Managed relational (Postgres/MySQL) | RDS | Azure Database for PostgreSQL/MySQL | Cloud SQL |
| High-perf relational | Aurora | Azure SQL DB / SQL MI | AlloyDB |
| NewSQL/global | Aurora Global / DynamoDB | Cosmos DB | Spanner / Bigtable |
| Key-value | DynamoDB | Cosmos DB (Table/SQL API) | Bigtable / Firestore |
| Data warehouse | Redshift (RA3, Serverless) | Synapse Dedicated SQL | BigQuery |
| Lake query engine | Athena | Synapse Serverless / Fabric | BigQuery external / BigLake |
| Catalog/metastore | Glue Data Catalog | Purview / Unity Catalog | Dataplex / Unity Catalog |
| Managed Spark | EMR / EMR Serverless | Synapse Spark / HDInsight / Fabric | Dataproc / Dataproc Serverless |
| Notebook/lakehouse platform | Databricks / EMR Studio / SageMaker | Databricks / Fabric | Databricks / Vertex AI |
| Serverless ETL | Glue (Jobs) | Data Factory (+ Mapping Data Flows) | Dataflow (Beam) |
| Orchestration | MWAA / Step Functions | Data Factory / Logic Apps | Cloud Composer / Workflows |
| Streaming ingest | Kinesis Data Streams | Event Hubs | Pub/Sub |
| Kafka-compatible | MSK / MSK Serverless | Event Hubs (Kafka API) / HDInsight Kafka | Pub/Sub / Dataproc Kafka |
| Managed Flink | Managed Service for Apache Flink | Stream Analytics / HDInsight Flink | Dataflow (Beam) / Dataproc Flink |
| Change data capture | DMS | DMS (Azure) / Debezium on ADF | Datastream |
| Search | OpenSearch Service | Azure AI Search | — (partner/Elastic) |
| Object store + table format | S3 + Iceberg/Delta/Hudi | ADLS Gen2 + Delta/Iceberg | GCS + Iceberg/Delta |
| BI | QuickSight | Power BI | Looker / Looker Studio |
| Feature store | SageMaker Feature Store | Azure ML Feature Store | Vertex AI Feature Store |

### 5.2 Picking by workload (decision table)

| Workload | First choice | Reasonable alternatives | Why |
|---|---|---|---|
| OLTP app DB | Aurora / Cloud SQL / Azure SQL | RDS Postgres | Multi-AZ, low latency, transactional |
| Petabyte warehouse, many concurrent BI | Snowflake / BigQuery | Redshift RA3, Synapse | Concurrency + separation of compute |
| Ad-hoc lake exploration | Athena / BigQuery / Trino | Synapse serverless | Serverless, scan-based cost |
| Nightly large Spark ETL | EMR Serverless / Glue / Dataproc Serverless | Databricks, EKS+Spark | Managed Spark, spot, scale-to-zero |
| Complex lakehouse + governance | Databricks (Unity Catalog) | Snowflake, Fabric | Integrated DevOps + catalog + lineage |
| Streaming ingestion | Kinesis / Event Hubs / Pub/Sub | MSK | Fully managed, elastic |
| Kafka ecosystem (Connect, Streams, Flink) | MSK | Confluent Cloud, Event Hubs | API compatibility |
| Real-time SQL on streams | Flink (MSF/Dataflow/HDInsight) | Spark Structured Streaming | True streaming semantics |
| CDC from OLTP | DMS / Datastream | Debezium (self-managed) | Managed replication |
| Transformations in warehouse | dbt + warehouse | Snowpark / BigQuery Dataform | SQL-first, testable, versioned |
| Unified lakehouse + BI (Microsoft) | Fabric | Synapse + Power BI | One platform, OneLake |

### 5.3 Cost-model mapping (the part that determines architecture)

| Service | Billing unit | Key cost lever |
|---|---|---|
| Redshift (provisioned) | node-hour + storage | pause when idle; concurrency scaling |
| Redshift Serverless | RPU-second | base RPU, autoscaling windows |
| BigQuery on-demand | bytes scanned | partitioning, clustering, `--max_bytes_billed` |
| BigQuery editions | slot-hours (autoscaling/committed) | reservation sizing + assignments |
| Snowflake | credit/second + TB-month | auto-suspend, warehouse size, multi-cluster |
| Athena | bytes scanned + per-query min | partition pruning, file size, compression |
| Synapse Dedicated | DWU-hour | pause/resume, workload isolation |
| Synapse serverless | bytes scanned | `OPENROWSET` pruning |
| EMR | instance-hour (EC2 + EMR uplift) | transient clusters, spot, managed scaling |
| EMR Serverless | vCPU-hour + GB-hour | worker size, pre-init, max capacity |
| Dataproc | vCPU-hour + $/DBU-like | serverless, spot, autoscaling |
| Databricks | DBU-hour + cloud VM | job clusters, Photon, autoscaling |
| Kinesis | shard-hour + PUT payload units | on-demand vs provisioned, aggregating records |
| MSK | broker-hour + storage | broker rightsizing, tiered storage |
| Event Hubs | throughput/processing unit-hour (or per-event) | autoscale TUs, capture to cheap storage |
| Pub/Sub | per GiB + per 10 GiB | message size, batching |

## 6. Patterns and anti-patterns

### 6.1 Patterns

- **Separate compute per workload** (ETL warehouse vs. BI warehouse vs. ad-hoc) to prevent
  noisy-neighbor contention and to attribute cost.
- **Catalog as the contract:** register all lake tables in one catalog; every engine reads
  from it.
- **Scale-to-zero defaults:** auto-suspend warehouses, transient Spark clusters, on-demand
  streams.
- **Push down and prune:** partition + cluster + columnar formats so engines scan the minimum.
- **Medallion + open table format** for the lake; avoid engine-proprietary file formats.
- **Managed ingestion + managed transformation + managed serving**, picking each independently.
- **Right tool per latency class:** batch (minutes–hours), micro-batch (seconds), streaming
  (sub-second) each get a different engine.
- **Replicate only what you need; don't duplicate the lake per engine.**

### 6.2 Anti-patterns

- **One giant warehouse/cluster for everything** → contention, no cost attribution, constant
  resizing.
- **Loading all data into the warehouse when the lake would do** → storage and loading costs,
  rigidity.
- **No catalog / per-engine tables** → silos and duplicated metadata.
- **Serverless for steady, high-volume workloads** (or provisioned for spiky ones) — mismatched
  cost models.
- **Ignoring scan-based pricing** → a single unpartitioned Athena/BigQuery query with a five-figure cost.
- **Over-relying on one vendor's proprietary features** with no exit path.
- **Kafka-as-a-database** (long retention as the primary store) instead of landing to object
  storage.
- **Small files from streaming sinks** (no compaction).
- **Orchestrating with cron on a VM** when a managed scheduler exists.
- **Self-managed Kafka when you don't operate Kafka** — you inherit paging for brokers.
- **Not enabling PITR/time travel** on a warehouse you rely on for correctness.

## 7. Real-world use cases and industry applications

- **E-commerce:** Clickstream → Kinesis/Event Hubs → Spark Structured Streaming (EMR/Dataproc)
  → Iceberg on S3/ADLS → Redshift/BigQuery/Snowflake for BI. Real-time recommendations via
  Flink + a low-latency store.
- **Fintech:** CDC (DMS/Datastream) from OLTP → Kafka → stream processing for fraud → warehouse
  for regulatory reporting; immutable archive in object storage with Object Lock.
- **Healthcare:** Batch genomics on EMR/Dataproc serverless; PHI isolated in a private VPC with
  CMEK; de-identified datasets in a separate account.
- **SaaS product analytics:** Event ingestion (Kinesis/Pub/Sub) → lakehouse (Databricks/Glue) →
  dbt models in the warehouse → embedded analytics (Looker/Power BI/QuickSight).
- **Media:** Large-scale transcoding metadata + viewing events; Athena/BigQuery for ad-hoc
  forensics; Snowflake for monetization dashboards.
- **Supply chain:** Near-real-time inventory from IoT via Kinesis/Event Hubs; batch forecasting
  with managed Spark + GPU training.

## 8. Senior-level deep dive: trade-offs, architecture decisions, performance, failure modes, debugging, cost

### 8.1 The five selection questions

1. **Workload shape:** interactive/ad-hoc, scheduled batch, continuous streaming, or
   operational serving? Each maps to different services.
2. **Concurrency and SLAs:** how many simultaneous users/jobs, and what latency is acceptable?
   (Drives warehouse design and isolation.)
3. **Cost model fit:** is the workload steady (favor committed/provisioned) or spiky (favor
   serverless/scan/credits)?
4. **Ecosystem and lock-in:** how much do you value Kafka/SQL/Iceberg portability vs.
   best-in-class features?
5. **Operational maturity:** do you have the team to run Kafka/Trino/Spark yourself, or must it
   be managed?

### 8.2 Trade-offs: warehouse vs. lake engine

| Dimension | Warehouse (Snowflake/BQ/Redshift) | Lake engine (Athena/Trino/Spark SQL) |
|---|---|---|
| Performance on big joins | Optimized, cached, tuned | Good but usually slower/less cached |
| Concurrency (100s of BI users) | Designed for it | Harder; needs a service layer |
| Cost model | credits/slots/nodes | bytes scanned or cluster hours |
| Data duplication | Often load/copy into warehouse | Query in place |
| Governance | Stronger built-in | Via catalog/lakehouse layer |
| Flexibility/formats | Engine-managed | Open formats (Iceberg/Delta) |
| Best for | Curated, high-concurrency, governed analytics | Raw/exploratory, huge, infrequent, cost-sensitive |

Modern answer: **lakehouse blurs the line** — Snowflake/BigQuery/Redshift can query Iceberg/
external tables, and Trino/Spark can be fast on well-laid-out data. Choose based on
concurrency, governance, and cost model rather than "warehouse vs. lake."

### 8.3 Trade-offs: Kinesis vs. MSK vs. Event Hubs vs. Pub/Sub

| Dimension | Kinesis | MSK | Event Hubs | Pub/Sub |
|---|---|---|---|---|
| API | AWS proprietary | Kafka | Kafka-compatible + AMQP | GCP proprietary |
| Ops | none | some (brokers, versions) | none | none |
| Ordering | per shard | per partition | per partition | ordering keys |
| Replay/retention | up to 365 d | configurable (tiered storage) | up to 90 d (Premium) | up to 31 d (seek) |
| Ecosystem | KCL/KDA, Lambda | full Kafka (Connect, Streams) | Kafka ecosystem | GCP-native |
| Best for | AWS-native simple ingest | Kafka ecosystems, portability | Azure-native, Kafka apps | Global, serverless, GCP |
| Cost model | shard-hour + PUT units | broker-hour + storage | TU/PU-hour | per GiB |

### 8.4 Performance internals and tuning

- **Warehouse:** distribution/sort keys (Redshift), clustering + partitioning (BigQuery/
  Snowflake), result caches, materialized views, and **avoiding data skew** across nodes.
- **Athena/Trino:** partition pruning, column projection, file size (128 MB–1 GB), compression
  (ZSTD/Snappy), Iceberg partition evolution; avoid `SELECT *`.
- **Spark:** shuffle partitioning, AQE, broadcast joins, skew handling, file compaction.
- **Streaming:** partition/shard count = parallelism ceiling; hot partitions cause lag; check
  consumer throughput per partition; enable enhanced fan-out (Kinesis) or more partitions
  (Kafka) as needed.
- **Concurrency:** multi-cluster warehouses (Snowflake), concurrency scaling (Redshift),
  reservations (BigQuery) turn "slow under load" into "provisioned for load."

### 8.5 Failure modes

| Service | Typical failure | Mitigation |
|---|---|---|
| RDS/Aurora | AZ failure, failover | Multi-AZ, retry with jitter, connection pooling (RDS Proxy) |
| Redshift | Full disk, paused cluster, queue waits | RA3 elastic storage, WLM, concurrency scaling |
| BigQuery | Quota/slot exhaustion, streaming buffer limits | Reservations, `max_bytes_billed`, batch loads |
| Snowflake | Credit exhaustion (resource monitor suspends), warehouse thrash | Resource monitors, right-size, auto-suspend |
| Athena | Throttling, huge scans, small-file doom | Workgroup limits, partitioning, compaction |
| Kinesis | Hot shard, ProvisionedThroughputExceeded | Reshard, on-demand mode, enhanced fan-out |
| MSK | Broker under-replication, disk full | Tiered storage, monitoring, right-sizing |
| EMR/Dataproc | Spot capacity loss, driver death | Managed scaling, instance fleets, checkpointing |
| Glue | DPU under/over-provisioning, schema drift | Worker sizing, job bookmarks, catalog evolution |

### 8.6 Debugging playbook

1. **Start at the cost meter** for performance problems: BigQuery `INFORMATION_SCHEMA.JOBS`,
   Snowflake `QUERY_HISTORY`, Redshift `STL/SVL` views, Athena query stats. Slow usually means
   "scanning/handling more than it should."
2. **Then layout:** is the filter column a partition/cluster key? Are files the right size?
3. **Then skew:** distribution of work across nodes/partitions.
4. **Then concurrency:** are you queuing (WLM, slots, clusters)?
5. **Then the plumbing:** are you moving data between regions/services unnecessarily?

### 8.7 Cost mechanics and levers

- **Match the cost model to the workload.** Serverless (Athena/BigQuery on-demand) for spiky
  and low-volume; committed/provisioned (slots/warehouses/clusters) for steady high volume.
  The same workload can be 3–10× cheaper under the right model.
- **Turn things off.** Auto-suspend, pause, terminate transient clusters, delete idle
  endpoints/warehouses.
- **Prune scans.** Layout decisions (partition/cluster/format) dominate scan-based bills.
- **Isolate and attribute.** Separate warehouses/projects/clusters so you can bill teams.
- **Compact and compress.** Fewer, larger, compressed files reduce everything.
- **Use spot for Spark.** 60–90% off for the majority of batch work.

## 9. Common mistakes and how to avoid them

| Mistake | Impact | Fix |
|---|---|---|
| One warehouse/cluster for all workloads | Contention + no attribution | Separate by workload |
| No auto-suspend/pause | Paying for idle | Set aggressive auto-suspend/pause |
| Unpartitioned scans | Enormous scan bills | Partition/cluster; set byte limits |
| Loading everything into the warehouse | Cost + rigidity | Query the lake; load only curated marts |
| Per-engine silos with no catalog | Duplication, drift | Single catalog, open table formats |
| Wrong cost model (serverless for steady) | Overspend | Match model to workload shape |
| Small files from streaming | Slow, expensive queries | Compaction jobs / auto-compaction |
| Self-managed Kafka without ops maturity | Paging, data loss risk | Use MSK/Event Hubs/Confluent |
| Ignoring concurrency design | Slow under load | Multi-cluster, scaling, reservations |
| No PITR/time travel | Unrecoverable bad load | Enable time travel/PITR |
| No resource monitors/budgets | Runaway spend | Monitors, budgets, alerts |
| Vendor lock-in without an exit plan | Strategic risk | Iceberg/Delta + SQL portability |

## 10. Exercises / interview questions (hints only)

1. **Pick a warehouse** for 500 BI users and a 40 TB nightly load. Justify against two
   alternatives. *(Hint: concurrency isolation, cost model, separation of compute.)*
2. **Design the data flow** from an OLTP Postgres to a lakehouse with < 5 min latency. *(Hint:
   CDC/DMS/Datastream → stream bus → Spark/Flink → Iceberg.)*
3. **A BigQuery query costs $400.** Explain why and how you'd prevent it structurally.
   *(Hint: partitioning, clustering, `max_bytes_billed`, column projection.)*
4. **When is MSK the wrong choice vs. Kinesis/Event Hubs?** *(Hint: ops burden, ecosystem
   needs, cloud affinity.)*
5. **Compare Athena vs. Redshift vs. Snowflake for a workload that runs for 3 h/day.**
   *(Hint: scan vs node vs credit cost models.)*
6. **Design a catalog strategy** so Spark, Athena, and a warehouse can query the same tables.
   *(Hint: Iceberg + Glue/Unity/Dataplex.)*
7. **Your streaming job lags at peak.** Where do you look first? *(Hint: partitions/shards,
   consumer parallelism, hot keys.)*
8. **How would you isolate ETL from BI on Snowflake/Redshift/BigQuery?** *(Hint: warehouses,
   WLM queues, reservations.)*

## References

- Amazon RDS Multi-AZ — https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Concepts.MultiAZ.html
- Amazon Aurora — https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/CHAP_AuroraOverview.html
- Amazon Redshift concurrency scaling — https://docs.aws.amazon.com/redshift/latest/dg/concurrency-scaling.html
- Amazon Redshift RA3 / managed storage — https://docs.aws.amazon.com/redshift/latest/mgmt/working-with-clusters.html
- Amazon Athena — https://docs.aws.amazon.com/athena/latest/ug/what-is.html
- Use the AWS Glue Data Catalog with Athena — https://docs.aws.amazon.com/athena/latest/ug/data-sources-glue.html
- AWS Glue — https://docs.aws.amazon.com/glue/
- Amazon EMR Serverless — https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/emr-serverless.html
- Amazon Kinesis Data Streams — https://docs.aws.amazon.com/streams/latest/dev/introduction.html
- Amazon MSK — https://docs.aws.amazon.com/msk/latest/developerguide/what-is-msk.html
- Amazon DynamoDB — https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Introduction.html
- BigQuery reservations / workload management — https://cloud.google.com/bigquery/docs/reservations-workload-management
- BigQuery pricing — https://cloud.google.com/bigquery/pricing
- BigQuery partitions and clustering — https://cloud.google.com/bigquery/docs/partitioned-tables
- Google Cloud Dataproc Serverless — https://cloud.google.com/dataproc-serverless/docs
- Pub/Sub — https://cloud.google.com/pubsub/docs/overview
- Azure Synapse dedicated SQL pool best practices — https://learn.microsoft.com/en-us/azure/synapse-analytics/sql/best-practices-dedicated-sql-pool
- Azure Event Hubs — https://learn.microsoft.com/en-us/azure/event-hubs/
- Apache Kafka protocol support in Event Hubs — https://learn.microsoft.com/en-us/azure/event-hubs/azure-event-hubs-apache-kafka-overview
- Snowflake virtual warehouses — https://docs.snowflake.com/en/user-guide/warehouses
- Snowflake understanding compute cost — https://docs.snowflake.com/en/user-guide/cost-understanding-compute
- Databricks Lakehouse / Delta Live Tables — https://docs.databricks.com/
- Apache Iceberg — https://iceberg.apache.org/docs/latest/
