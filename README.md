# The Data Engineering Book: From Tools to Systems Architecture

This repository is a **study book** for a Data Engineer who already knows the tools
(Spark, Kafka, dbt, Airflow, Snowflake, etc.) but needs to understand **how real data
systems are built**: the concepts, frameworks, and techniques that let you design,
integrate, and operate data platforms at a **senior** level.

This is not a "hello world" tutorial. Every topic explains:

- **Definitions** and the vocabulary the industry actually uses.
- **The role** each piece plays inside a data architecture.
- **How it works internally** (internals) — the thing that separates a junior from a senior.
- **How to implement it**, with code, SQL, configuration, and pseudocode.
- **Where to implement it**: which tool/service each technique belongs to.
- **Use cases and real-world applications** in the industry.
- **Trade-offs, anti-patterns, debugging, and architecture decisions** at a senior level.
- **References** (official docs, papers, engineering blogs) at the end of every file.

> The original syllabus lives in [`sql-and-data-systems-design.md`](./sql-and-data-systems-design.md).
> The **SQL** section was intentionally left out for now (it will be a second part).

---

## How to use this book

1. **Read in chapter order** the first time. Chapters are ordered from foundations
   (modeling) to systems (streaming, cloud).
2. **Every file is self-contained**, but there are cross-references. If you don't
   understand a reference, jump to the linked file.
3. **Do the "exercises / interview questions"** at the end of every file. If you can't
   answer them in your own words, you don't own the topic yet.
4. **Don't memorize**: focus on *trade-offs*. At senior level the correct answer is almost
   always "it depends", and what you're evaluated on is whether you know **what it depends on**.

### Suggested learning path

| Order | Chapter | Why |
|------|---------|-----|
| 1 | Data Modeling | Every data system starts with the model. Without it, the rest is chaos. |
| 2 | Data Warehouses and Data Lakes | Where the data lives and in what format. |
| 3 | ETL/ELT Pipelines | How you move and transform data reliably. |
| 4 | Distributed Systems | The theory behind *why* all of this is hard (CAP, replication). |
| 5 | Spark Internals | The compute engine you probably already use — but from the inside. |
| 6 | Streaming | From batch to real time: Kafka, offsets, watermarks. |
| 7 | Cloud Architecture | Where to deploy all of this and how not to bankrupt yourself. |
| 8 | Data Quality | How to guarantee data is correct, traceable, and observable. |

---

## Table of contents

### [01 · Data Modeling](./01-data-modeling/)
- [OLTP vs OLAP](./01-data-modeling/01-oltp-vs-olap.md)
- [Dimensional modeling](./01-data-modeling/02-dimensional-modeling.md)
- [Star and Snowflake schemas](./01-data-modeling/03-star-snowflake-schemas.md)
- [Fact and dimension tables](./01-data-modeling/04-fact-dimension-tables.md)
- [Slowly changing dimensions (SCD)](./01-data-modeling/05-slowly-changing-dimensions.md)
- [Normalization vs Denormalization](./01-data-modeling/06-normalization-vs-denormalization.md)

### [02 · Data Warehouses and Data Lakes](./02-data-warehouses-data-lakes/)
- [Data Warehouses](./02-data-warehouses-data-lakes/01-data-warehouses.md)
- [Data Lakes](./02-data-warehouses-data-lakes/02-data-lakes.md)
- [When to use each (decision)](./02-data-warehouses-data-lakes/03-warehouse-vs-lake-decision.md)
- [Lakehouse architecture](./02-data-warehouses-data-lakes/04-lakehouse-architecture.md)
- [Partitioning](./02-data-warehouses-data-lakes/05-partitioning.md)
- [File formats (Parquet, ORC, Avro)](./02-data-warehouses-data-lakes/06-file-formats.md)
- [Compression](./02-data-warehouses-data-lakes/07-compression.md)
- [Metadata catalogs](./02-data-warehouses-data-lakes/08-metadata-catalogs.md)

### [03 · ETL/ELT Pipelines](./03-etl-elt-pipelines/)
- [Fundamentals: ETL vs ELT and pipeline anatomy](./03-etl-elt-pipelines/00-etl-vs-elt-fundamentals.md)
- [Scalability](./03-etl-elt-pipelines/01-scalable-pipelines.md)
- [Fault tolerance](./03-etl-elt-pipelines/02-fault-tolerance.md)
- [Idempotency](./03-etl-elt-pipelines/03-idempotency.md)
- [Monitoring and observability](./03-etl-elt-pipelines/04-monitoring-observability.md)
- [Recoverability after failure](./03-etl-elt-pipelines/05-recoverability.md)

### [04 · Distributed Systems](./04-distributed-systems/)
- [Replication](./04-distributed-systems/01-replication.md)
- [Partitioning (sharding)](./04-distributed-systems/02-partitioning-sharding.md)
- [CAP theorem](./04-distributed-systems/03-cap-theorem.md)
- [Consistency models](./04-distributed-systems/04-consistency-models.md)
- [Eventual consistency](./04-distributed-systems/05-eventual-consistency.md)
- [Leader/follower replication](./04-distributed-systems/06-leader-follower-replication.md)

### [05 · Spark Internals](./05-spark-internals/)
- [Shuffle](./05-spark-internals/01-shuffle.md)
- [Partitions](./05-spark-internals/02-partitions.md)
- [Narrow vs wide transformations](./05-spark-internals/03-narrow-vs-wide-transformations.md)
- [Broadcast joins](./05-spark-internals/04-broadcast-joins.md)
- [Data skew](./05-spark-internals/05-data-skew.md)
- [Catalyst optimizer](./05-spark-internals/06-catalyst-optimizer.md)
- [AQE (Adaptive Query Execution)](./05-spark-internals/07-adaptive-query-execution.md)

### [06 · Streaming](./06-streaming/)
- [Kafka fundamentals](./06-streaming/01-kafka-fundamentals.md)
- [Event-driven architecture](./06-streaming/02-event-driven-architecture.md)
- [Consumer groups](./06-streaming/03-consumer-groups.md)
- [Offsets](./06-streaming/04-offsets.md)
- [Delivery semantics: exactly-once vs at-least-once](./06-streaming/05-delivery-semantics.md)
- [Windowing](./06-streaming/06-windowing.md)
- [Watermarks](./06-streaming/07-watermarks.md)

### [07 · Cloud Architecture](./07-cloud-architecture/)
- [Cloud fundamentals](./07-cloud-architecture/01-cloud-fundamentals.md)
- [Compute (VMs, containers, serverless, Kubernetes)](./07-cloud-architecture/02-compute.md)
- [Storage (object, block, file)](./07-cloud-architecture/03-storage.md)
- [Networking (VPC, subnets, load balancers)](./07-cloud-architecture/04-networking.md)
- [Managed data services](./07-cloud-architecture/05-managed-data-services.md)
- [IAM and security](./07-cloud-architecture/06-iam-security.md)
- [IaC and DevOps for data](./07-cloud-architecture/07-iac-devops.md)
- [Cost optimization](./07-cloud-architecture/08-cost-optimization.md)

### [08 · Data Quality](./08-data-quality/)
- [Data validation](./08-data-quality/01-data-validation.md)
- [Schema evolution](./08-data-quality/02-schema-evolution.md)
- [Data lineage](./08-data-quality/03-data-lineage.md)
- [Data observability](./08-data-quality/04-data-observability.md)

---

## Book conventions

- **`Code examples`**: SQL, Python (PySpark), config (YAML/JSON), CLI commands.
- **Diagrams**: ASCII or Mermaid when it helps visualization.
- **Comparison tables**: for trade-offs and decisions.
- **`> Senior note` blocks**: highlight nuances, traps, and things you only learn with experience.
- **"References" section**: real links to official docs, papers, and blogs.
