# Schema Evolution

> Schemas are contracts that change. The moment two systems exchange data, one of them will
> want to add a field, rename one, or drop one — and the moment that happens in a distributed
> system with rolling deploys and long-lived data, "just update both sides" stops working.
> Schema evolution is the discipline of changing the shape of data **without breaking readers,
> writers, or the historical data already on disk**. A senior engineer knows the compatibility
> modes by heart, knows *why* Avro resolves by field name and Protobuf by field number, and
> knows/that table formats like Iceberg and Delta turn schema changes into metadata
> operations. This chapter is that knowledge made operational.

## TL;DR

- **Compatibility is a direction, and the direction is the whole point.**
  - **Backward compatible:** *new* code reads *old* data. Enables consumer-then-producer upgrade.
  - **Forward compatible:** *old* code reads *new* data. Enables producer-then-consumer upgrade.
  - **Full compatible:** both. Only safe for independent, order-free rollout.
  - **Transitive:** checked against *all* previous versions, not just the last one.
- **The default in Confluent Schema Registry is `BACKWARD` (non-transitive).** Know this and
  know when to change it (protobuf and add-new-message best practice → `BACKWARD_TRANSITIVE`).
- **Avro evolves by field name + defaults; Protobuf evolves by field number + `reserved`; JSON
  Schema evolves by required/additionalProperties.** Mixing these up is the #1 interview and
  production mistake.
- **Avro's killer rule:** a new field must have a `default` for backward compatibility, and
  the default must match the **first** union branch (so optional fields are `["null","T"]`
  with `"default": null`).
- **The wire format** (`magic byte 0x0` + 4-byte schema ID, then the payload) is why a schema
  registry can enforce compatibility and why consumers never embed the schema in the message.
- **Table formats solve a different problem:** Iceberg/Delta store data in files, so evolution
  must not rewrite files or misread old ones. Iceberg uses **field IDs** and makes add/drop/
  rename/reorder/widen **metadata-only**; Delta uses **schema enforcement + column mapping**
  for renames/drops.
- **Enforcement modes** are a spectrum: `NONE` (anything goes) → `BACKWARD`/`FORWARD`/`FULL`
  → strict schema contracts that reject *any* unplanned change. Choose per subject, not
  globally.
- **Rolling-deploy safety is derivable:** with `BACKWARD`, deploy consumers first; with
  `FORWARD`, deploy producers first; with `FULL`, order doesn't matter; with `NONE`, you are
  on your own.

## 1. Definitions and core vocabulary

| Term | Meaning |
|------|---------|
| **Schema** | The declared structure/type of a dataset or message. |
| **Schema evolution** | Changing a schema over time while preserving the ability to read old/new data. |
| **Compatibility type** | The rule set enforced when registering a new schema version. |
| **Schema Registry** | A service storing versioned schemas and validating compatibility (Confluent, Apicurio, AWS Glue, Redpanda, Karapace). |
| **Subject** | A named scope under which schema versions are registered (usually `topic-value` or `topic-key`). |
| **Version** | A monotonically increasing integer per subject. |
| **Schema ID** | A globally unique integer assigned to a schema, embedded in the wire format. |
| **Writer schema** | The schema the data was encoded with. |
| **Reader schema** | The schema the consumer wants to decode with. |
| **Schema resolution** | Avro's algorithm to decode writer-schema data with a reader schema. |
| **Projection / promotion** | Reading a subset of fields / widening a type on read. |
| **Field ID** | Iceberg's stable identifier for a column, independent of name and position. |
| **Column mapping** | Delta's mechanism to decouple physical column names/order from logical names (enables rename/drop). |
| **Enforcement mode** | Whether mismatched writes/registers are rejected, warned, or allowed. |

### 1.1 The four compatibility modes, precisely

Let the sequence of schemas be `X-2 → X-1 → X`.

| Mode | Definition (non-transitive) | Consumer upgrade rule |
|------|-----------------------------|-----------------------|
| `BACKWARD` | Consumers with `X` can read data written with `X` or `X-1` | Upgrade **consumers first** |
| `BACKWARD_TRANSITIVE` | Consumers with `X` can read data written with `X, X-1, …, X-n` (all) | Consumers first |
| `FORWARD` | Consumers with `X-1` can read data written with `X` | Upgrade **producers first** |
| `FORWARD_TRANSITIVE` | Consumers with any prior schema can read data with `X` | Producers first |
| `FULL` | `X` ↔ `X-1` both work | Either order |
| `FULL_TRANSITIVE` | `X` ↔ every prior schema | Either order |
| `NONE` | No checks | Manual coordination |

> **Senior note.** Non-transitive `BACKWARD` only guarantees you can read the *previous*
> version. If you have `X-2` data still in the topic (retention!) and you rewind a consumer,
> `BACKWARD` won't save you. That is the exact reason `BACKWARD_TRANSITIVE` exists, and why
> "we set BACKWARD" is an incomplete answer in an interview.

## 2. What problem it solves / why it matters

Every data system is a set of producers and consumers that **deploy at different times**:

```
   t0: producer v1 ──writes(schema A)──▶ Kafka ──reads(schema A)──▶ consumer v1
   t1: producer v2 will write schema B, but consumer v2 is not deployed yet.
       ┌─────────────────────────────────────────────────────────────┐
       │  Between producer-upgrade and consumer-upgrade there is a   │
       │  window where BOTH schemas coexist in flight. That window   │
       │  is the entire problem.                                     │
       └─────────────────────────────────────────────────────────────┘
```

Without a compatibility discipline you get one of three outcomes:

1. **Deserialization crash.** Consumer gets bytes it can't decode (`Unknown union branch`,
   `Missing required field`). Latency spikes, DLQ floods.
2. **Silent corruption.** Consumer "reads" the data with the wrong shape — a widened type
   truncates, a renamed field reads as null, a reused field number returns the wrong value.
   **This is the dangerous one**, because nothing errors.
3. **Frozen schemas.** Teams are so afraid to change anything that columns are
   append-only-forever-with-suffixes (`amount_v2`, `amount_v3`), and the dataset becomes
   unmaintainable.

Schema evolution + a registry turns a coordination problem ("everyone upgrade at 2am") into a
**protocol** ("change X is allowed; deploy in order Y").

## 3. How it works internally (mechanisms, internals)

### 3.1 The Confluent wire format and why it exists

When a producer serializes with a registry-backed serializer:

```
 ┌──────┬────────────────────┬───────────────────────────────┐
 │ 0x00 │  schema ID (4 bytes, big-endian int) │  Avro/Proto/JSON payload │
 └──────┴────────────────────┴───────────────────────────────┘
   ▲          ▲
   │          └─ consumer looks up the schema by ID and decodes with it
   └─ magic byte; 0 = "schema registry framing"

 The schema is NOT in the message. The message carries only an ID pointer.
   → message stays small
   → the producer can validate compatibility at (or before) register time
   → the consumer can always find the exact schema the bytes were written with
```

This is why a registry is more than a schema dump: it is the **enforcement point** that makes
"can this writer register this new schema?" a yes/no the broker never has to know about.

### 3.2 Avro schema resolution (the algorithm a senior must know)

Avro stores the **writer schema** with each file (in the file header) and sends the schema ID
per message. On read, Avro resolves **writer schema → reader schema** field by field:

```
For each field in the READER schema:
  - if a field with the SAME NAME exists in the WRITER schema:
        - if types match → copy
        - if reader type can be promoted from writer (int→long→float→double) → promote
        - if writer field is a union → pick matching branch
  - else (field is NEW to the reader):
        - if the reader field has a DEFAULT → fill with default
        - else → ERROR ("Found ... expecting ...") -- NOT backward compatible
For each field in the WRITER schema NOT in the reader:
  - simply dropped (projection) → fine
Enums: reader enum must contain the writer's symbol, else use reader's default
       (adding a new enum symbol = forward-incompatible unless enum default set)
Unions: reader union must contain a branch matching the writer's chosen branch
```

The **default value** is the crux. Avro's spec requires the default to match the type;
for a union, it must match the **first** branch. So the idiomatic optional field is:

```json
{ "name": "favorite_color", "type": ["null", "string"], "default": null }
```

Add it → old data (which lacks the field) reads as `null` → **backward compatible**.

### 3.3 Avro compatibility rules (per change)

| Change | Backward | Forward | Full |
|--------|:--------:|:-------:|:----:|
| Add field **with default** | ✔ | ✔ | ✔ |
| Add field **without default** | ✘ | ✔ | ✘ |
| Remove field **with default** | ✔ | ✔ | ✔ |
| Remove field **without default** | ✔ | ✘ | ✘ |
| Widen scalar (`int`→`long`, `float`→`double`) | ✔ | ✘ | ✘ |
| Narrow scalar (`long`→`int`) | ✘ | ✔ | ✘ |
| Add union branch | ✔ | ✔* | ✔* |
| Remove union branch | ✘ | ✔ | ✘ |
| Rename field (no alias) | ✘ | ✘ | ✘ |
| Rename field **with alias** | ✔ | ✔ | ✔ |
| Add enum symbol without enum default | ✔ | ✘ | ✘ |
| Change field type to *incompatible* | ✘ | ✘ | ✘ |

> **Senior note.** Avro has **aliases** precisely so you can rename a field compatibly:
> `{"name":"uid","aliases":["user_id"],"type":"string"}` lets a reader using `uid` resolve
> old data written with `user_id`. This is the Avro answer to "how do I rename a column",
> and it's the detail juniors miss.

### 3.4 Protobuf compatibility rules (field numbers are the identity)

Protobuf's wire format is `(field_number, wire_type, value)`. The **field number**, not the
name, is the identity. Rules:

- **Never reuse a field number** for a different meaning. If you delete a field, mark it
  `reserved`:
  ```proto
  message Order {
    reserved 3, 7;              // numbers you deleted
    reserved "legacy_status";   // names you deleted
    string order_id = 1;
    int64  order_total_cents = 2;
  }
  ```
- Adding a field is **backward compatible** (old readers ignore unknown fields) and
  **forward compatible** if it's optional/repeated.
- Changing an `int32` ↔ `int64` is wire-compatible but semantically risky (truncation);
  Confluent's default rules treat widening as OK and narrowing as not.
- **Adding a new `message` type is not forward compatible** → best practice for Protobuf is
  **`BACKWARD_TRANSITIVE`**, not `FORWARD`.
- Renaming a field is *wire*-compatible (numbers don't change) but breaks generated code and
  JSON/text serialization — keep `json_name` stable or don't rename.

```proto
// evolution of the same Order across versions
syntax = "proto3";
message Order {
  string order_id   = 1;
  int64  total_cents = 2;
  // v2: added, optional, does not break v1 readers
  optional string currency = 5;
  // v3: deleted field 3 -> reserve it forever
  reserved 3;
  reserved "old_customer_ref";
}
```

### 3.5 JSON Schema compatibility

JSON Schema has no built-in resolution algorithm (unlike Avro) and no field numbers (unlike
Protobuf). Compatibility depends on two things:

1. **`required`** — adding a property to `required` makes readers reject old docs (breaks
   backward? actually breaks data written by old producers when read by new validators).
2. **`additionalProperties`** — open (`true`, the default) allows unknown fields; closed
   (`false`) rejects them, which makes adding fields break consumers that validate strictly.

Confluent maps this onto two policies: `LENIENT` (ignore the content model) and `STRICT`
(content-model-aware: open vs closed), plus a `LOGICAL` policy that additionally checks
Flink/Iceberg representability. Net: JSON Schema is the **weakest** format for evolution,
which is why schema-first formats (Avro/Protobuf) dominate event-driven systems.

### 3.6 Table formats: evolution *on storage*, not on the wire

Kafka/registry evolution assumes messages are decoded with a schema ID. Lakehouse tables are
different: the data is **files** (Parquet) with no per-row schema pointer; a query engine must
map file columns to table columns. Two designs:

- **Name-based** (e.g. Hive): a column is matched by name. Reusing a name after a drop can
  **un-delete** an old column — a correctness bug.
- **Position-based**: a column is matched by ordinal. Dropping a column shifts positions and
  **changes values** — a correctness bug.

**Iceberg** sidesteps both with **field IDs**:

```
 Iceberg table metadata: schema (with ids)  ──┐
                                              │  files store data by FIELD ID (in Parquet
 Parquet file: column id 4 = "customer_id"   ─┘  field_metadata), not by name or position

  - Add column  → assign a NEW id; old files have no id 4 → reads as null (never "un-deleted")
  - Drop column → the id is never reused
  - Rename      → change the name for the same id; old files still map correctly
  - Reorder     → ids are stable; order is cosmetic
  - Widen type  → id unchanged; readers promote (int→long, float→double)
  All of the above are METADATA-ONLY: no file rewrite.
```

**Delta Lake** uses a **transaction log** + a table schema in the log. By default Delta is
*strict* (schema enforcement): writes with extra/different columns fail unless you opt in:

| Delta switch | Effect |
|--------------|--------|
| default | **Schema enforcement** — incompatible write fails |
| `mergeSchema=true` | Allow adding new columns on write (schema evolution) |
| `overwriteSchema=true` | Replace the whole table schema (destructive) |
| **Column mapping** (`delta.columnMapping.mode=name`) | Decouple physical Parquet names/IDs from logical names → enables rename/drop without rewrite |
| **Type widening** | Widen a column type without rewriting (int→long, float→double, etc.) |
| `MERGE` with `schema.evolution` | Allow evolving schema during an upsert |

## 4. How to implement it (step-by-step, with code/YAML/SQL examples)

### 4.1 Step 1 — Author the Avro schema with evolution in mind

```json
// schemas/order-v1.avsc
{
  "type": "record",
  "name": "Order",
  "namespace": "com.acme.events",
  "fields": [
    { "name": "order_id",       "type": "string" },
    { "name": "customer_id",    "type": "string" },
    { "name": "total_cents",    "type": "long"   },
    { "name": "status",         "type": {
        "type": "enum", "name": "Status",
        "symbols": ["PLACED","SHIPPED","DELIVERED","RETURNED","CANCELLED"],
        "default": "PLACED"                       // <- lets us ADD symbols later
    }},
    { "name": "created_ts",     "type": { "type": "long", "logicalType": "timestamp-millis" } }
  ]
}
```

```json
// schemas/order-v2.avsc  — backward compatible additions only
{
  "type": "record",
  "name": "Order",
  "namespace": "com.acme.events",
  "fields": [
    { "name": "order_id",       "type": "string" },
    { "name": "uid",            "type": "string", "aliases": ["customer_id"] },  // RENAME via alias
    { "name": "total_cents",    "type": "long"   },
    { "name": "status",         "type": {
        "type": "enum", "name": "Status",
        "symbols": ["PLACED","SHIPPED","DELIVERED","RETURNED","CANCELLED","REFUNDED"],
        "default": "PLACED"
    }},
    { "name": "created_ts",     "type": { "type": "long", "logicalType": "timestamp-millis" } },
    { "name": "currency",       "type": ["null","string"], "default": null }    // new optional field
  ]
}
```

### 4.2 Step 2 — Register and *enforce* with the Schema Registry

```bash
# Set the compatibility mode for a subject (per-topic, the right granularity)
curl -X PUT -H "Content-Type: application/vnd.schemaregistry.v1+json" \
     --data '{"compatibility": "BACKWARD_TRANSITIVE"}' \
     http://schema-registry:8081/config/orders-value

# Dry-run: is v2 compatible with the current subject?  (do this in CI!)
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
     --data "{\"schema\": $(jq -Rs . < schemas/order-v2.avsc)}" \
     http://schema-registry:8081/compatibility/subjects/orders-value/versions/latest
# -> {"is_compatible": true}
```

A **CI guard** (fail the PR that breaks compatibility):

```yaml
# .github/workflows/schema-compat.yml
name: schema-compat
on: [pull_request]
jobs:
  check:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Compatibility check against prod registry
        run: |
          for f in schemas/*.avsc; do
            subject="$(basename "$f" .avsc)-value"
            body=$(jq -Rs '{schema: .}' "$f")
            ok=$(curl -sf -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
                   --data "$body" \
                   "$REGISTRY/compatibility/subjects/$subject/versions/latest" \
                   | jq -r .is_compatible)
            [ "$ok" = "true" ] || { echo "INCOMPATIBLE: $subject"; exit 1; }
          done
```

### 4.3 Step 3 — Wire producers and consumers

```python
# Producer: register-on-write (serializer auto-registers & validates)
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import Serializer

sr = SchemaRegistryClient({"url": "http://schema-registry:8081"})
avro = AvroSerializer(sr, open("schemas/order-v2.avsc").read())
```

```python
# Consumer: registry-backed deserializer looks up the schema by ID from the wire header
from confluent_kafka.schema_registry.avro import AvroDeserializer
deser = AvroDeserializer(sr)         # no schema string needed: it's in the message
```

For **batch** Avro files (HDFS/S3), the writer schema is in the file header, so a reader can
always decode regardless of the current table schema:

```python
import fastavro
with open("orders-2025-01.avro", "rb") as f:
    reader = fastavro.reader(f)
    for record in reader:          # decoded with the FILE's writer schema
        ...
# New files written with v2; old files (v1) still decode. Query-time union handles both.
```

### 4.4 Step 4 — Enforce schema as a *contract* in the warehouse (dbt)

```yaml
# models/marts/fct_orders/schema.yml
version: 2
models:
  - name: fct_orders
    config:
      contract:
        enforced: true          # dbt will FAIL the build if the model's columns
                                # differ from what is declared here
    columns:
      - name: order_id
        data_type: string
        constraints:
          - type: not_null
          - type: primary_key
      - name: total_cents
        data_type: bigint
      - name: currency
        data_type: string
```

Now a model that returns an extra/hard-to-type column is a **build failure**, not silent drift.

### 4.5 Step 5 — Evolve lakehouse tables (Iceberg / Delta)

**Iceberg (metadata-only, no file rewrite):**

```sql
-- Add / rename / drop / widen / reorder — all metadata operations
ALTER TABLE prod.orders ADD COLUMN currency STRING;
ALTER TABLE prod.orders RENAME COLUMN customer_id TO uid;
ALTER TABLE prod.orders ALTER COLUMN total_cents TYPE bigint;   -- int4 -> int8 widening
ALTER TABLE prod.orders DROP COLUMN legacy_flag;
-- Partition evolution (also metadata-only, old files keep old layout):
ALTER TABLE prod.orders ADD PARTITION FIELD days(created_ts);
```

```python
# PyIceberg: schema update via field IDs
from pyiceberg.catalog import load_catalog
tbl = load_catalog("prod").load_table("db.orders")
with tbl.update_schema() as update:
    update.add_column("currency", StringType())
    update.rename_column("customer_id", "uid")
```

**Delta Lake:**

```python
# Default: schema enforcement ON -> a new-column write fails
df.write.format("delta").mode("append").save("/mnt/delta/orders")   # ERROR on new col

# Opt in to evolution for adding columns:
df.write.format("delta").option("mergeSchema", "true") \
        .mode("append").save("/mnt/delta/orders")

# Overwrite schema (destructive, replaces the table schema):
df.write.format("delta").option("overwriteSchema", "true").mode("overwrite") \
        .save("/mnt/delta/orders")
```

```sql
-- Delta column mapping: enable roster/name mapping so RENAME works without rewrite
ALTER TABLE orders SET TBLPROPERTIES (
  'delta.columnMapping.mode' = 'name',
  'delta.minReaderVersion'   = '2',
  'delta.minWriterVersion'   = '5'
);
ALTER TABLE orders RENAME COLUMN customer_id TO uid;
-- Delta type widening (no rewrite): int -> bigint, float -> double, etc.
```

> **Senior note.** Iceberg was *designed* for evolution (field IDs from day one). Delta added
> column mapping and type widening *later*, and they require table-property opt-in with a
> reader/writer protocol version bump — which can lock older readers out. If you know you'll
> rename/drop columns, enable column mapping **at table creation**, not after you have 10 TB.

### 4.6 Step 6 — Decide enforcement mode per subject

```
   Who owns the data?                     Recommended mode
   ───────────────────────────────────    ─────────────────────────────
   Internal producers, one team           BACKWARD  (move fast, one owner)
   Multiple consumers on long retention   BACKWARD_TRANSITIVE (rewind-safe)
   Protobuf, additive-first               BACKWARD_TRANSITIVE
   Producer upgrades before consumers     FORWARD   (rare)
   Independent deploys, strict SLOs       FULL_TRANSITIVE
   Migration period, breaking change      NONE + new topic + deprecation window
```

A breaking change that cannot be made compatible has exactly one good procedure:

```
1. Create a NEW schema/topic/table (orders-v2-value or orders_v2) with NONE semantics.
2. Dual-write from the producer for the deprecation window.
3. Migrate consumers; run reconciliation (see chapter 01) between old and new.
4. Announce, monitor adoption, then decommission the old subject.
   (Never "force" an incompatible change onto an existing subject in production.)
```

## 5. Where to implement it (tools/platforms and when each fits)

| Layer | Mechanism | Notes |
|-------|-----------|-------|
| **Messaging** | Confluent Schema Registry, Apicurio, AWS Glue Schema Registry, Redpanda Schema Registry, Karapace | Enforce compat at register time; wire format with schema ID |
| **Serialization** | Avro, Protobuf, JSON Schema, Thrift | Avro = evolution-first; Protobuf = field numbers; JSON Schema = weakest |
| **Warehouse transforms** | dbt contracts, SQL DDL (`ALTER TABLE`) | Structural enforcement at transform output |
| **Lakehouse tables** | Iceberg (field IDs), Delta (schema enforcement + column mapping + type widening), Hudi | Iceberg metadata-only; Delta needs opt-in for rename/drop/widen |
| **Stream processing** | Flink/Spark Structured Streaming + registry | State stores + changelog topics must stay readable (BACKWARD for Kafka Streams) |
| **Contracts** | ODCS / datacontract-cli, dbt contracts | Cross-system, product-oriented agreements |
| **Catalogs** | DataHub / OpenMetadata / Glue Catalog | Store/diff schemas, power impact analysis |

**When each fits:**
- Event streams between services → **registry + Avro/Protobuf**, mode per subject.
- Analytics tables you fully control → **dbt contracts** + warehouse DDL.
- Large lakehouse tables you must evolve in place → **Iceberg** (preferred) or **Delta with
  column mapping**.
- Cross-team agreements → **ODCS contract** as the human-facing document, generated checks as
  the machine enforcement.

## 6. Patterns and anti-patterns

### Patterns

| Pattern | Why it works |
|---------|--------------|
| **Additive-first** (new fields optional with defaults) | Backward + forward compatible forever |
| **Aliases for renames** (Avro) / **column mapping** (Delta) | Rename without breaking readers or rewriting |
| **`reserved` for deletes** (Protobuf) | Prevents field-number reuse corruption |
| **Per-subject compatibility** | Not every dataset needs FULL; match the mode to the upgrade order |
| **Dual-write + new subject for breaking changes** | Never forces an incompatible register |
| **CI compatibility gate** | Incompatible schema never reaches prod |
| **Schema-as-code in Git** | Review, history, and rollback for schemas |
| **Contract + deprecation window** | Gives consumers time without freezing the producer |

### Anti-patterns

| Anti-pattern | Consequence |
|--------------|-------------|
| Global `NONE` compatibility | Every change is a potential outage |
| Reusing a deleted Protobuf field number | Silent wrong-value corruption |
| Adding a required Avro field without default | Consumers crash on old data |
| Renaming a field with no alias | Old data reads as null / error |
| `overwriteSchema=true` in prod casually | Silent data loss / incompatible rewrite |
| Position-based column matching | Drop/reorder silently shifts values |
| One shared subject for many event types | Any incompatible change breaks everyone |
| Evolving a topic with `NONE` + force-register | Historical data undecodable; rewind broken |
| "We'll version by appending `_v2`" forever | Schema sprawl; no deprecation discipline |

## 7. Real-world use cases and industry applications

- **Payments/Fintech.** A `Transaction` event schema must never silently change field 3 from
  `amount_cents` to `amount`. `BACKWARD_TRANSITIVE` + `reserved` numbers + CI gate is how they
  guarantee that a 7-year retention topic is always rewindable.
- **E-commerce event streams.** Marketing, recommendation, and finance consume the same order
  stream. The order team adds `currency` as an optional Avro field with default `null`;
  all consumers keep working, no coordinated deploy.
- **IoT telemetry.** Firmware versions ship out of order and stay deployed for years — the
  device (producer) and backend (consumer) both lag. `FULL_TRANSITIVE` lets backend and
  device evolve independently.
- **Data lakehouse.** A parquet-based `events` table must add 30 new columns over a year.
  Iceberg field IDs mean thousands of historical files never need rewriting; `SELECT *` on old
  partitions returns nulls for new columns instead of shifting data.
- **CDC pipelines.** Debezium emits schema changes; the sink must evolve the target table
  automatically (`mergeSchema` in Delta, auto-evolve in Snowflake `COPY`/Snowpipe). Getting
  this wrong turns a routine source column addition into a broken pipeline.
- **ML feature stores.** Feature tables evolve as models add features; versioned feature views
  + contracts prevent a train/serve schema mismatch.

## 8. Senior-level deep dive

### 8.1 Trade-offs

| Decision | Option A | Option B | Depends on |
|----------|----------|----------|-----------|
| Mode strictness | `BACKWARD` (fast) | `FULL_TRANSITIVE` (safe) | Number of consumers, retention length |
| Format | Avro | Protobuf | Evolution ergonomics vs code-gen/perf/ecosystem |
| Where to evolve | Registry (wire) | Table format (storage) | Streaming vs lakehouse |
| Rename strategy | Alias (Avro) | New field + backfill | Whether consumers can migrate |
| Breaking change | New subject/topic | Force-register | Whether you can run dual-write |
| Enforce at read or write | Schema-on-read (flexible) | Schema-on-write (safe) | Ops maturity, data volume |
| Column mapping | Enable at creation | Add later | Existing table age/size |

### 8.2 Rolling-deploy safety (derive it, don't memorize it)

```
Given compatibility C and the deployment graph producer(P) -> topic -> consumer(C):

  BACKWARD / BACKWARD_TRANSITIVE : new consumer reads old data → deploy CONSUMER(s) first
  FORWARD  / FORWARD_TRANSITIVE  : old consumer reads new data → deploy PRODUCER first
  FULL     / FULL_TRANSITIVE     : both directions safe        → any order
  NONE                           : no guarantee               → coordinate manually

Kafka Streams special case: state stores/changelog topics must be readable by the
new version (it reads its OWN old state), so only BACKWARD-family is safe.
```

### 8.3 Failure modes and debugging

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| `SerializationException: Error deserializing` | Incompatible schema registered (or wrong subject) | Check registry `compatibility` + version history; use CI gate |
| `Unknown union branch` | New union branch without default / union order | Add default; put `null` first |
| Field reads as `null` suddenly | Renamed without alias, or dropped | Add alias / restore field / backfill |
| Wrong values (no error) | Protobuf field reused; date↔timestamp promotion; decimal scale change | `reserved`; `LOGICAL` policy; never reuse numbers |
| Works in dev, fails at rewind | Non-transitive `BACKWARD` on a long-retention topic | Switch to `BACKWARD_TRANSITIVE` |
| Delta write fails after source change | Schema enforcement rejecting new column | Opt into `mergeSchema` or add explicit DDL |
| Iceberg query returns nulls after drop+add | (correct behaviour) column was dropped, new id assigned | Expected — never reuse names as "same column" |
| Query planning regression after partition evolution | Mixed partition specs | Iceberg splits planning per spec; expect it; monitor file counts |

### 8.4 Evolution across the whole pipeline (the senior mental model)

A schema does not evolve in one place; it evolves along a **chain**:

```
  Source system  ──▶  CDC / ingestion  ──▶  Kafka (wire schema)  ──▶  Lake/Warehouse table
   (DDL change)        (parse/emit)          (registry compat)        (table-format evolution)
        │                    │                      │                        │
     "schema drift"     "mapping drift"       "compatibility"          "field-ID evolution"
```

The senior question is *"what is the schema of record at each hop, and does each hop's
evolution rule preserve the guarantees the next hop needs?"* Forgetting any hop is how a
"compatible" Avro change still breaks the warehouse (e.g. the Avro format is fine but the
sink doesn't auto-evolve the table).

### 8.5 Org and process

- **Schemas are code.** Store them in Git, review changes, require the compatibility check to
  pass in CI, and keep a changelog per schema.
- **The registry is a shared service**, not per-team. Its per-subject config is the contract.
- **Ownership:** the **producer** owns the schema and the deprecation window; the platform team
  owns the registry, the CI gate, and catalog/diffing tooling.
- **Deprecation policy:** document how long deprecated fields/topics live, who must migrate,
  and what "done" means (e.g. <1% traffic). A policy nobody measures is a policy nobody
  follows.

## 9. Common mistakes and how to avoid them

1. **Setting global `NONE`** to make a blocked change pass. → Fix the schema, or fork a new
   subject; never disable checks globally.
2. **Confusing backward and forward.** → Memorize from the *deploy order* (§8.2), not from the
   words.
3. **Adding a required Avro field without a default.** → Always give new fields defaults.
4. **Renaming without an alias (Avro) or column mapping (Delta).** → Renames are a change
   like any other; make them compatible.
5. **Reusing Protobuf field numbers.** → `reserved` on delete, forever.
6. **Forgetting long retention.** → Use `*_TRANSITIVE` for topics you may rewind.
7. **Assuming the registry protects the warehouse.** → Table-format evolution is a *separate*
   concern; wire the sink to auto-evolve or add DDL.
8. **Enabling Delta column mapping after the fact.** → Plan it at creation.
9. **No CI gate.** → Compatibility is checked at 2am during a deploy instead of at PR time.
10. **Treating a breaking change as a runtime problem.** → It's a *process* problem: new
    subject, dual-write, deprecation window.

## 10. Exercises / interview questions

1. *Modes:* A topic retains 90 days and consumers are routinely rewound. Which mode, and why
   is plain `BACKWARD` insufficient? *(Hint: previous-version-only.)*
2. *Avro:* Make `customer_id` → `uid` a compatible rename, then explain how a consumer using
   `uid` reads data written with `customer_id`. *(Hint: aliases, resolution.)*
3. *Protobuf:* You deleted `string legacy_status = 3;`. What must you do, and what happens if
   you reuse `3` for a new `int32`? *(Hint: reserved; wire-type + value corruption.)*
4. *Deploy order:* Given `FORWARD` compatibility, which side deploys first and why? Flip it for
   `BACKWARD`.
5. *Tables:* Why is Iceberg's "drop then re-add the same column name" not a rename, and why is
   that a *feature*? *(Hint: field IDs, correctness guarantee #1.)*
6. *Delta:* What do `mergeSchema` and `overwriteSchema` each do, and which one is dangerous in
   prod?
7. *Breaking change:* Design the full migration for renaming a field in a
   `FULL_TRANSITIVE`-impossible case. *(Hint: new subject, dual-write, reconciliation,
   decommission.)*
8. *Internals:* Explain the Confluent wire format and why the schema is not in each message.
9. *Cross-hop:* An Avro change passes the registry but the warehouse pipeline breaks anyway.
   Name three places to look. *(Hint: sink evolution, mapping, catalog.)*
10. *Trade-off:* When is JSON Schema the right choice despite weaker compatibility rules?
    *(Hint: human-readable APIs, tooling, open content model.)*

## References

- Confluent — Schema evolution and compatibility: https://docs.confluent.io/platform/current/schema-registry/fundamentals/schema-evolution.html
- Confluent — Compatibility checks / serdes: https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html
- Apache Avro — specification (schema resolution): https://avro.apache.org/docs/1.11.1/specification/#schema-resolution
- Apache Avro — project site: https://avro.apache.org/docs/
- Protocol Buffers — language guide / compatibility: https://protobuf.dev/programming-guides/proto3/
- Protobuf — best practices (reserved, field numbers): https://protobuf.dev/best-practices/dos-donts/
- Understanding Protobuf Compatibility (Confluent blog): https://yokota.blog/2021/08/26/understanding-protobuf-compatibility/
- Understanding JSON Schema Compatibility (Confluent blog): https://yokota.blog/2021/03/29/understanding-json-schema-compatibility/
- Apache Iceberg — evolution: https://iceberg.apache.org/docs/latest/evolution/
- Apache Iceberg — spec: https://iceberg.apache.org/spec/
- Delta Lake — schema evolution (Azure Databricks): https://learn.microsoft.com/en-us/azure/databricks/tables/update-schema
- Delta Lake — protocol / column mapping: https://docs.delta.io/latest/delta-column-mapping.html
- dbt — model contracts: https://docs.getdbt.com/docs/build/model-contracts
- Apicurio Registry: https://www.apicur.io/registry/
- AWS Glue Schema Registry: https://docs.aws.amazon.com/glue/latest/dg/schema-registry.html
