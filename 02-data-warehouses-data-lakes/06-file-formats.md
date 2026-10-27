# File Formats: Parquet, ORC, Avro (+ CSV/JSON)

> The file format is the last line of defense between your query and your data. Everything the
> optimizer wants to do — skip files, skip pages, read only four of forty columns, decode 10 GB as
> 400 MB — depends on what the writer put in the file. Choose Parquet or ORC and you inherit page
> indexes, per-column min/max statistics, dictionary encoding, and a decade of engine optimization.
> Choose CSV or JSON and you inherit quoting bugs, silent type coercion, a full parse of every byte,
> and no statistics at all. A senior engineer knows the physical layout of these formats well
> enough to reason about why a scan is slow, how big a file should be, which encoding the writer
> will pick, and when the "wrong" format is actually the right one (Avro for wire and row-level
> ingestion; CSV because the vendor sends you CSV and you have no leverage).

---

## TL;DR

- **Columnar (Parquet, ORC) for analytics; row (Avro, JSONL, Protobuf) for transport and record-level
  ingestion.** That single rule covers 95% of decisions.
- **Columnar's win is I/O amplification**: if a query touches 4 of 40 columns, you read ~10% of the
  bytes. Add per-column encodings and compression and the effective reduction is typically 20–100×
  versus CSV.
- **Statistics are what make pruning possible**: file/row-group/stripe-level min/max, null counts,
  and (Parquet) page-level column indexes and bloom filters. Without stats, the engine reads
  everything.
- **Parquet's physical hierarchy**: file → row group (~128 MB) → column chunk → page (~1 MB) →
  data page v2 (levels separated from values) → encoded values. Footer carries schema, per-chunk
  stats, and the page index.
- **ORC's hierarchy**: file → stripe (~64 MB) → index stream (row index every ~10,000 rows + bloom
  filters) + data streams per column → stripe footer. ORC's indexes are written *inline* and are
  used for predicate push-down by Hive/Spark/Trino.
- **Avro is a row format with the strongest schema-evolution story** (writer schema embedded in the
  container file, reader schema used for resolution). It is the default Kafka value format for a
  reason — and a poor choice for wide analytical scans.
- **Encodings matter more than compression codecs.** Dictionary + RLE + delta encodings often give
  5–10× before any codec runs; the codec then works on already-repetitive data. Choose encoding
  first, codec second (see [07-compression.md](./07-compression.md)).
- **Write sorted or clustered data whenever the sort matches the predicate** — it costs nothing and
  makes min/max skipping multiply in effectiveness.
- **Row group / stripe size is a tuning knob with three-way trade-offs**: bigger = fewer requests,
  more memory per read, coarser skipping granularity; smaller = finer pruning, more overhead. 128 MB
  row groups with ~1 MB pages is a well-tested default for Parquet.
- **Nested data is not free.** Every leaf column under a nested structure gets its own column chunk;
  a "JSON-ish" schema with 300 leaves turns one row read into 300 column reads. Flatten what you
  query; keep the raw nested blob for the rare full-fidelity case.
- **CSV/JSON are interchange formats, not storage formats.** Land them in raw, parse them once into
  Parquet/ORC, and never let them into your curated layer.

---

## 1. Definitions and core vocabulary

| Term | Meaning |
|---|---|
| **Row-oriented** | Values of one record stored contiguously (`a1,b1,c1,a2,b2,c2,…`). Great for reading/writing whole records. |
| **Columnar** | Values of one column stored contiguously (`a1,a2,…,b1,b2,…`). Great for scanning few columns over many rows. |
| **Row group (Parquet)** | A horizontal slice of the table (~128 MB target), containing one column chunk per column. The unit of parallel reading and of statistics. |
| **Column chunk** | All values of one column within one row group; compressed and encoded independently. |
| **Page (Parquet)** | Smallest unit of encoding/compression inside a column chunk; dictionary page + data pages. ~1 MB is typical. |
| **Stripe (ORC)** | The ORC equivalent of a row group (~64 MB default), containing column streams plus an inline index. |
| **Index stride (ORC)** | Number of rows between row-index entries (default 10,000). The granularity of ORC predicate push-down. |
| **Encoding** | How values are represented inside a page: PLAIN, DICTIONARY, RLE, DELTA_BINARY_PACKED, BYTE_STREAM_SPLIT… |
| **Compression codec** | Byte-level compressor applied (usually after encoding) to a page/stream: Snappy, ZSTD, GZIP, LZ4. |
| **Predicate pushdown / PPD** | Pushing filters into the scan so the reader skips data using statistics. |
| **Bloom filter** | Probabilistic membership index for high-cardinality equality predicates where min/max is useless. |
| **Page index (Parquet)** | `ColumnIndex` (per-page statistics) + `OffsetIndex` (page positions), stored in the footer region, enabling page-level skipping. |
| **Definition level / repetition level** | Dremel's mechanism for encoding NULLs and repeated (nested/array) fields in a columnar file. |
| **Splittable** | A file whose interior can be read in parallel by seeking to a boundary (e.g., Avro blocks, ORC stripes). A `.gz` stream is not splittable. |
| **Schema evolution** | Adding/removing/renaming/widening columns across writes. Formats differ sharply in how well this works. |
| **Object container file (Avro)** | Avro's file format: header with writer schema + metadata, then blocks of binary-encoded records separated by a 16-byte sync marker. |
| **Vectorized reader** | Reader that decodes batches (e.g., 1024 values) into columnar memory and applies SIMD-friendly operators (the reason columnar layouts win in CPU too). |
| **Arrow** | Language-agnostic in-memory columnar representation used for zero-copy interchange between engines (an IPC format, not a storage format). |

---

## 2. What problem it solves / why it matters

### 2.1 The I/O amplification argument

```text
Table: 40 columns, 2 billion rows, 1 TB as CSV, 180 GB as Parquet
Query: SELECT country, COUNT(*), SUM(revenue) WHERE order_date = '2026-03-01'

CSV:
  - must read and parse ALL bytes: 1 TB
  - no stats → no file/page skipping (only partition-level if partitioned)
  - string→number parsing of ~10 columns per row even if unused
  - single-threaded-ish parse before vectorized ops
Parquet (4 of 40 columns, ~7.5 GB of the 180 GB after compression):
  - read 4 column chunks per row group → ~7–10 GB
  - skip row groups whose min/max exclude the date → maybe 0.03%
  - decode with dictionary/plain + Snappy at hundreds of MB/s/core
  → same answer, ~100–1000× fewer bytes touched
```

The economics follow directly: on BigQuery-style pricing, bytes scanned *is* the bill. On Spark,
bytes scanned drives task count, shuffle size, and cluster minutes.

### 2.2 What each format family is actually for

| Need | Right format | Why |
|---|---|---|
| Analytical scans, aggregations, BI | Parquet (or ORC in Hive-centric stacks) | Columnar + stats + encodings |
| Lakehouse tables (Iceberg/Delta/Hudi) | Parquet (Iceberg/Delta), Parquet or HFiles (Hudi) | Broad engine support, page index, bloom filters |
| Kafka/streaming payloads, CDC events | Avro, Protobuf, JSON | Compact, schema-carrying, row-wise serialization |
| Landing zone for files from partners | whatever arrived (CSV/JSON/XML) | You don't control it; parse early |
| Machine learning features/labels | Parquet | Wide tables, selective column reads, ecosystem support |
| Log archives queried rarely with text search | JSON (compressed) or columnar-with-a-string-column | Flexibility vs scan cost |
| In-memory exchange between engines | Arrow IPC / Flight | Zero-copy columnar handoff |
| Long-term cold archive | Parquet + ZSTD at high level, tiered storage | Best bytes-per-dollar with retrievability |

---

## 3. How it works internally

### 3.1 Parquet layout

```text
┌────────────────────────────────────────────────────────────┐
│ "PAR1" magic                                                │
├────────────────────────────────────────────────────────────┤
│ Row Group 0                                                 │
│  ├─ ColumnChunk(user_id)                                    │
│  │   ├─ Dictionary page (optional; ≤1 MB)                   │
│  │   ├─ Data page v2 (rep/def levels | encoded values)      │
│  │   └─ Data page v2 ...                                    │
│  ├─ ColumnChunk(event_ts)  → pages                          │
│  ├─ ColumnChunk(revenue)   → pages                          │
│  └─ ColumnChunk(country)   → pages                          │
├────────────────────────────────────────────────────────────┤
│ Row Group 1 ...                                             │
├────────────────────────────────────────────────────────────┤
│ ColumnIndex (per-page min/max/null counts, per column)      │
│ OffsetIndex (page offsets/sizes, per column)                │
│ Bloom filters (optional, per column chunk)                  │
├────────────────────────────────────────────────────────────┤
│ Footer (Thrift): schema, row-group metadata, column stats,   │
│ key-value metadata (e.g. iceberg.schema, pandas schema)      │
│ 4-byte footer length                                        │
├────────────────────────────────────────────────────────────┤
│ "PAR1" magic                                                │
└────────────────────────────────────────────────────────────┘
```

How a reader uses it:

```text
1. Range-read the last 8 bytes → footer length → range-read the footer.
2. Deserialize the schema + row-group metadata (stats included).
3. Evaluate the predicate against row-group stats → prune row groups.
4. For surviving row groups, read the ColumnIndex → prune pages.
5. Read only the needed column chunks, seek per page (OffsetIndex),
   decompress, decode, vectorized filter.
6. If a page's stats pass, evaluate the values row-by-row (or via bitmaps).
```

Cost properties: **one GET per file for the footer** plus range GETs for the needed chunks. A reader
never touches columns it does not need, and never decodes pages that cannot match.

### 3.2 Parquet encodings

| Encoding (enum) | What it does | Best for | Typical win |
|---|---|---|---|
| `PLAIN` (0) | Raw little-endian values | Fallback, random data, fixed-width ints/floats | 1× |
| `PLAIN_DICTIONARY` (2) / `RLE_DICTIONARY` (8) | Build a dictionary of distinct values; store indices with RLE/bit-packing hybrid | Low-cardinality strings/enums (country, status, device type) | 5–50× |
| `RLE` (3) | Run-length encoding of repeats | Booleans, sorted/low-cardinality columns, def/rep levels | 2–100× on runs |
| `DELTA_BINARY_PACKED` (5) | Delta + zig-zag + bit-packing of integers | Monotonic IDs, timestamps, counters | 2–10× |
| `DELTA_LENGTH_BYTE_ARRAY` (6) | Lengths delta-encoded, then concatenated bytes | Variable-length strings with similar lengths | 2–5× |
| `DELTA_BYTE_ARRAY` (7) | Prefix-encoding (like incremental encoding in databases) | Sorted strings, URLs, UUID prefixes | 3–20× on sorted data |
| `BYTE_STREAM_SPLIT` (9) | Splits floats/doubles into byte planes | Floating-point data that otherwise compresses poorly | 2–5× + codec gains |

Writers choose encodings automatically. The `<column>.encoding` properties and, more usefully,
**dictating the data's shape** (sorting, low cardinality, consistent types) determine the outcome.

The dictionary fallback matters operationally:

```text
Parquet dictionary encoding builds a per-column-chunk dictionary.
If the dictionary exceeds parquet.dictionary.page.size (default 1 MB),
the writer falls back to PLAIN for the remainder of the chunk.
⇒ A high-cardinality column (e.g. a UUID or free-text field) silently loses
   dictionary encoding for the tail of the chunk. Symptom: that column's
   compressed size explodes vs its siblings.
```

### 3.3 Parquet statistics, page index, and bloom filters

```text
ColumnChunk statistics: min, max, null_count, distinct_count (optional),
                        and exactness flags (is_min_value_exact / is_max_value_exact)
Truncation: min/max values are truncated (parquet.statistics.truncate.length, default 64 bytes)
            → long strings lose exactness and may fail to prune correctly, or over-prune
Page index: ColumnIndex = per-page min/max/null_count; OffsetIndex = page locations
Bloom filters: SBBF (split-block bloom filter) per column chunk, sized by NDV and FPP
```

```python
# PySpark: enable bloom filters on a high-cardinality equality column
(df.write
   .mode("overwrite")
   .option("parquet.bloom.filter.enabled#user_id", "true")
   .option("parquet.bloom.filter.expected.ndv#user_id", "10000000")
   .option("parquet.bloom.filter.fpp", "0.01")
   .parquet("s3://lake/silver/users/"))
```

> **Senior note**: min/max is useless for a random UUID column (the range covers everything after a
> few thousand rows). If your predicates are equality on a high-cardinality column, you need bloom
> filters, a sort order (so min/max becomes narrow), or bucketing — and if you have none of those,
> you are scanning everything, which is fine only if the table is small.

### 3.4 ORC layout and indexes

```text
┌──────────────────────────────────────────────────────────────┐
│ "ORC" magic + postscript + footer metadata                    │
├──────────────────────────────────────────────────────────────┤
│ Stripe 0 (~64 MB default)                                     │
│  ├─ Index streams (per column):                              │
│  │    RowIndex  (min/max, positions, null counts per stride) │
│  │    BloomFilter (optional per column)                       │
│  ├─ Data streams (per column):                                │
│  │    PRESENT (nulls), DATA, LENGTH, DICTIONARY_DATA,         │
│  │    SECONDARY (compressed value bytes for strings)          │
│  └─ StripeFooter (stream directory, encodings, statistics)    │
├──────────────────────────────────────────────────────────────┤
│ Stripe 1 ...                                                  │
├──────────────────────────────────────────────────────────────┤
│ File footer: schema, stripe list, column statistics           │
└──────────────────────────────────────────────────────────────┘
```

ORC's distinguishing features:

- **Predicate push-down built in**: the reader walks the row index (every `orc.row.index.stride`
  rows, default 10,000), and can skip strides and even portions of strides.
- **Per-stream compression**: small streams (PRESENT, LENGTH, indices) can be compressed or left raw
  to avoid overhead — a real advantage on narrow columns.
- **RLE v2** with SHORT_REPEAT/DIRECT/PATCHED_BASE/DELTA modes: excellent for sorted integers and
  timestamps.
- **Hive ACID** tables use ORC base + delta directories — the predecessor of the delta-file idea.
- **Type support**: rich nested types, decimals, timestamps with nanosecond precision, union types,
  and ORC's own `struct`/`list`/`map` handling.

```sql
-- Hive/Spark: ORC with explicit index and compression settings
CREATE TABLE silver.orders_orc (
  order_id BIGINT, customer_id BIGINT, ts TIMESTAMP, amount DECIMAL(18,4)
)
STORED AS ORC
TBLPROPERTIES (
  'orc.compress'            = 'ZSTD',
  'orc.stripe.size'         = '134217728',   -- 128 MB stripes
  'orc.row.index.stride'    = '10000',
  'orc.bloom.filter.columns'= 'customer_id',
  'orc.bloom.filter.fpp'    = '0.01'
);
```

### 3.5 Avro layout

```text
┌────────────────────────────────────────────────────┐
│ Magic: 'O','b','j', 0x01                           │
├────────────────────────────────────────────────────┤
│ Metadata map (bytes):                              │
│   avro.schema  = JSON writer schema (REQUIRED)     │
│   avro.codec   = null|deflate|bzip2|snappy|xz|zstd │
├────────────────────────────────────────────────────┤
│ 16-byte random sync marker                         │
├────────────────────────────────────────────────────┤
│ Block 0: [long: object count]                      │
│          [long: compressed byte length]            │
│          [binary-encoded records, compressed]      │
│          [16-byte sync marker]                     │
├────────────────────────────────────────────────────┤
│ Block 1 ... (each block independently readable)    │
└────────────────────────────────────────────────────┘
```

Key properties:

- **Binary encoding carries no field names or types** — the writer schema is required to decode.
  This makes Avro records byte-compact relative to JSON (5–10× smaller) at the cost of needing the
  schema out-of-band (or in the file header).
- **Schema resolution is a first-class feature**: the reader supplies its own schema and Avro
  resolves differences — new fields get defaults, removed fields are ignored, `int` promotes to
  `long`/`float`/`double`, `string` ↔ `bytes`. This is unmatched by Parquet/ORC (which instead rely
  on field IDs / column mapping managed by the table format).
- **Splittable at block boundaries** because each block ends with the file's sync marker and carries
  its own byte length: MapReduce/Spark readers can start mid-file. This is why Avro is fine for large
  row-oriented files and why `.gz` is not.
- **Snappy blocks carry a CRC32** of the uncompressed data — cheap integrity checking for transports.

```python
# PySpark: Avro for an ingestion/wire path
(df.write
   .format("avro")
   .option("avroSchema", schema_json)     # explicit writer schema
   .option("compression", "zstandard")
   .mode("append")
   .save("s3://lake/raw/events_avro/"))

# Reading with evolution: reader schema supplies defaults for new fields
sq = (spark.read.format("avro").option("avroSchema", reader_schema_json)
        .load("s3://lake/raw/events_avro/"))
```

### 3.6 CSV and JSON: why they cost so much

| Problem | CSV | JSON |
|---|---|---|
| No schema | Types inferred per read; leading zeros, quoted numbers, `NA`/`null`/`NULL`/`` variants | Types inferrable but inconsistently; numbers lose precision as doubles |
| No statistics | Zero pruning below the partition level | Same |
| Parsing cost | Full byte scan + delimiter/quote state machine | Full byte scan + full parser (2–5× CSV cost) |
| Ambiguity | Embedded delimiters, embedded newlines in quotes, `\r\n` vs `\n`, BOM, encoding (Latin-1 vs UTF-8) | Duplicate keys, mixed-type arrays, deeply nested objects, NaN/Infinity, big integers |
| Nested data | Impossible | Natural but expensive: every nested array element repeats keys |
| Splittability | `.csv` yes; `.csv.gz` no (one task per file) | `.jsonl` yes; `.json.gz` no; a JSON *array* file must be fully parsed |
| Size | Baseline | 1.5–3× CSV for the same logical data |
| Evolution | Positional → breaking | Additive only; no enforcement |

```python
# The only correct long-term treatment of CSV/JSON: parse once, land columnar.
raw = (spark.read
        .option("header", True)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt")
        .option("encoding", "UTF-8")
        .option("dateFormat", "yyyy-MM-dd")
        .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
        .csv("s3://lake/raw/partner/orders/"))

clean, quarantine = raw.filter("_corrupt IS NULL"), raw.filter("_corrupt IS NOT NULL")
(clean.selectExpr(
    "cast(order_id as bigint) as order_id",
    "cast(customer_id as bigint) as customer_id",
    "to_timestamp(created_at) as created_at",
    "cast(amount as decimal(18,4)) as amount")
  .write.mode("overwrite").partitionBy("order_date")
  .option("compression", "zstd").parquet("s3://lake/silver/partner_orders/"))
quarantine.write.mode("append").json("s3://lake/quarantine/partner_orders/")
```

> **Senior note**: never let a decimal money field travel through JSON as a float. `0.1 + 0.2`
> problems are real, and `NUMBER(38,18)` values can round-trip differently through different parsers.
> Serialize money as a string or an integer of minor units, and cast explicitly at the boundary.

### 3.7 Nested data and Dremel shredding

A nested schema is flattened into leaf columns; each value carries **definition levels** (how many
optional ancestors were present) and **repetition levels** (how many repeated ancestors were new).

```text
Record: { user_id: 1, tags: ["a","b"] }
Leaf column: tags.list.element
  repetition level: 0 (first) , 1 (continuation)
  definition level: 2 (both list and element present) or 1 (empty list) or 0 (null list)

Row group == column chunk
  user_id            → [1]
  tags.list.element  → ["a","b"] with rep levels [0,1], def levels [2,2]
```

Consequences:

- Skipping a nested leaf still requires reading its levels, so pruning benefits are smaller for
  deeply nested columns.
- A JSON-like schema with 300 leaves produces 300 column chunks per row group. Reading one
  "document" touches 300 streams. This is the "nested schema penalty".
- Repeated fields make the *parent* row's statistics non-representative: min/max of a repeated leaf
  tells you nothing about the row, so filtering on nested arrays is often pushed down only per page.

**Practical rule**: promote what you filter on into top-level, flat columns (or generated columns),
and keep the nested payload for full-fidelity retrieval.

### 3.8 Comparison table

| Dimension | **Parquet** | **ORC** | **Avro** | **CSV** | **JSON/JSONL** |
|---|---|---|---|---|---|
| Layout | Columnar | Columnar | Row | Row (text) | Row (text) |
| Typical compression vs CSV | 5–10× | 5–10× | 1.5–3× | 1× | 1.5–3× CSV |
| Scanning speed | Fastest | Fast (fastest in Hive/Vectorized) | Slow | Slowest | Slow |
| File stats | Row group + page index + bloom | Stripe + row index + bloom | None (block counts only) | None | None |
| Predicate pushdown | Yes (row group + page) | Yes (stripe + row index stride) | No | Partition only | Partition only |
| Schema evolution | Limited (needs table format + field IDs) | Limited (Hive column evolution) | **Excellent** (writer/reader + defaults) | Positional, fragile | Additive only |
| Nested support | Good (Dremel levels) | Good | Good (unions/records) | None | Native but costly |
| Splittable | Yes (row-group aligned) | Yes (stripe aligned) | **Yes (block aligned)** | Yes (uncompressed); no (gzip) | Yes (`.jsonl`); no (gzip) |
| Multi-engine support | Best-in-class | Hive/Spark/Trino/Presto strong | Spark, Kafka, Flink, many libs | Universal | Universal |
| Typical home | Lakehouse tables, ML, BI | Hive ecosystems, ACID-on-Hive legacy | Kafka, CDC, ingestion/wire | Partner drops | Logs, semi-structured landing |
| Extras | Modular encryption, page CRC, GEOMETRY/GEOGRAPHY, VARIANT | ACID-friendly, nanosecond timestamps | Single-object encoding with fingerprint, OCF, RPC | — | — |

---

## 4. How to implement it (step by step)

### Step 1 — Choose the format by role, not preference

```text
raw/landing        → whatever arrived (JSON/CSV/XML/Avro) + compression
silver (modeled)   → Parquet (or ORC if your stack is Hive-first)
gold (BI/ML)       → Parquet, partitioned/clustered, ZSTD
streaming/wire     → Avro or Protobuf (schema registry), JSON only for debugging
intermediate       → Parquet (or Arrow for in-memory)
archive            → Parquet + ZSTD (high level) on cheap storage
```

### Step 2 — Tune the writer, not just the reader

```python
# PySpark Parquet writer tuning — the settings that actually move the needle
spark.conf.set("spark.sql.parquet.compression.codec", "zstd")
spark.conf.set("spark.sql.parquet.compression.level", "3")      # 1..22; 3 is the sweet spot
spark.conf.set("spark.sql.parquet.enable.dictionary", "true")
spark.conf.set("spark.sql.parquet.writeLegacyFormat", "false")  # use modern logical types
spark.conf.set("spark.sql.files.maxRecordsPerFile", "0")        # 0 = no cap
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128m")

# Bigger target row groups + bounded memory per task
spark.conf.set("parquet.block.size", str(128 * 1024 * 1024))
spark.conf.set("parquet.page.size", str(1024 * 1024))
spark.conf.set("parquet.page.row.count.limit", "20000")
spark.conf.set("spark.sql.parquet.writeBatchSize", "16384")

# Write: sort within files so min/max skipping works (nearly free during a compaction)
from pyspark.sql import functions as F
(df.repartition(64, "event_date")
   .sortWithinPartitions("country", "event_type")
   .write.mode("overwrite")
   .partitionBy("event_date")
   .parquet("s3://lake/silver/events/"))
```

### Step 3 — Verify what the writer actually produced

```python
# Inspect Parquet metadata without reading the data (pyarrow)
import pyarrow.parquet as pq
pf = pq.ParquetFile("s3://lake/silver/events/event_date=2026-03-01/part-00000.parquet")
print(pf.metadata.num_row_groups, pf.metadata.num_columns, pf.metadata.serialized_size)
for rg in range(pf.metadata.num_row_groups):
    md = pf.metadata.row_group(rg)
    print("row group", rg, "rows", md.num_rows, "bytes", md.total_byte_size)
    for c in range(md.num_columns):
        col = md.column(c)
        st = col.statistics
        print("  ", col.path_in_schema, col.compression,
              "encodings:", col.encodings,
              "min/max:", (st.min, st.max) if st else None,
              "nulls:", st.null_count if st else None,
              "size:", col.total_compressed_size)
```

```sql
-- DuckDB: an excellent zero-infrastructure way to sanity-check files
SELECT * FROM parquet_metadata('s3://lake/silver/events/**/*.parquet') LIMIT 20;
SELECT * FROM parquet_file_metadata('s3://lake/silver/events/*.parquet');
```

```bash
# parquet-tools (Java) and orc-tools
parquet-tools meta --show-column-indexes s3://lake/silver/events/part-00000.parquet | head -50
parquet-tools rowcount s3://lake/silver/events/
orc-tools meta s3://lake/silver/events_orc/part-00000.orc | head -40
```

### Step 4 — Read-side tuning

```python
# Spark: fewer, larger tasks and vectorized reads
spark.conf.set("spark.sql.files.maxPartitionBytes", "134217728")   # 128 MB per task
spark.conf.set("spark.sql.files.openCostInBytes", "4194304")       # assume 4 MB open cost
spark.conf.set("spark.sql.parquet.enableVectorizedReader", "true")
spark.conf.set("spark.sql.parquet.filterPushdown", "true")
spark.conf.set("spark.sql.orc.filterPushdown", "true")
spark.conf.set("spark.sql.orc.enableVectorizedReader", "true")

# AQE: coalesce small partitions and skip empty ones
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
```

```sql
-- Trino: limits and pushdown
SET SESSION hive.parquet_optimized_writer_enabled = true;
-- check pushdown in EXPLAIN
EXPLAIN (TYPE DISTRIBUTED)
SELECT country, sum(revenue) FROM lake.silver.events WHERE event_date = DATE '2026-03-01' GROUP BY 1;
```

### Step 5 — Compaction and layout (formats don't fix themselves)

```sql
-- Iceberg: rewrite to 128 MB files, sorted for skipping
CALL lake.system.rewrite_data_files(
  table => 'silver.events',
  options => map('target-file-size-bytes','134217728','min-input-files','5'));

-- Delta
OPTIMIZE silver.events ZORDER BY (country, event_type);
```

---

## 5. Where to implement it (tools/platforms and when each fits)

| Format | Predominant stack | When to choose it | Watch out for |
|---|---|---|---|
| **Parquet** | Spark, Trino, Athena, Iceberg, Delta, BigQuery (via load), Snowflake (via unload/load or external), DuckDB, pandas/pyarrow, ML | Default for any lakehouse table and any analytics dataset | Dictionary fallback on high-cardinality columns; nested-schema fan-out; older writers missing page index |
| **ORC** | Hive, Spark, Presto/Trino, Hive-ACID transactional tables | Hive-first estates, existing ORC tables, ACID-on-Hive legacy, some ML frameworks with ORC support | Weaker ecosystem outside Hive family; tooling (`orc-tools`) less common; minor engine differences in predicate pushdown |
| **Avro** | Kafka + Schema Registry, Flink, Spark ingestion, Hadoop MR, Confluent ecosystem | Wire/transport, append-only ingestion where row-level schema evolution dominates | Terrible for wide analytical scans; no column pruning; no statistics |
| **Protobuf** | gRPC services, Kafka (Confluent), event pipelines | Wire format where schema evolution + language support + compactness matter | Requires schema registry discipline; nested/oneof semantics need flattening for analytics |
| **CSV** | Partner drops, spreadsheet exports, legacy exports | Only as a landing/interchange format | Everything in 3.6; always quarantine and type explicitly |
| **JSON / JSONL** | Logs, API dumps, event landing | Landing, debugging, low-volume semi-structured | Full-parse cost; float/precision issues; arrays/nesting |
| **XML** | Legacy B2B, healthcare (HL7/CDA), finance (FIX/FpML) | Landing only | Verbose; needs a dedicated parser; schema validation is a project in itself |
| **Arrow IPC / Feather** | pandas/polars/DuckDB interop, Flight | In-memory exchange, single-node analytics | Not designed for durable, evolvable storage |
| **HFile / Parquet log blocks** | Hudi MoR log files | Only inside Hudi | Not readable by generic engines |

---

## 6. Patterns and anti-patterns

### 6.1 Patterns

| Pattern | Description | Why it works |
|---|---|---|
| **Parse-once, land-columnar** | Raw stays in source format; silver is Parquet/ORC | Removes the parsing tax from every downstream query |
| **Sort within files by the dominant predicate** | `sortWithinPartitions` before write | Narrows row-group min/max → real skipping |
| **Right-sized row groups** | 128 MB row groups, 1 MB pages | Fewer requests, good skipping granularity, bounded memory |
| **ZSTD-3 for columnar data** | Codec after encodings | Best ratio/speed trade-off on modern CPUs (Chapter 07) |
| **Explicit schemas at read time** | `spark.read.schema(...)` | Determinism, no inference pass, fail-fast on drift |
| **Quarantine + `_corrupt` column** | Route bad rows instead of dropping | Correctness with observability |
| **Bloom filters for equality on high-cardinality columns** | Enable per column | Turns a full scan into a bounded read |
| **Wide table = column order by access frequency** | Group rarely used columns | Some engines read contiguous chunks better; improves cache locality in row-group reads |
| **Small side table = ORC/Parquet "flat" schema** | No nested narrowing needed | Simplifies pushdown |
| **Keep the raw blob in a separate column** | `payload STRING` alongside extracted fields | Full fidelity for rare needs without the nested penalty on hot paths |
| **Format-per-zone** | Wire format at ingest, columnar thereafter | Each format used for its strength |

### 6.2 Anti-patterns

| Anti-pattern | Symptom | Fix |
|---|---|---|
| **CSV/JSON in the curated layer** | Slow scans, type drift, quoting bugs | Convert at the boundary |
| **One giant Parquet file (10 GB)** | No parallelism, huge memory per read, coarse pruning | Target 128 MB–1 GB; split/compact accordingly |
| **Millions of tiny Parquet files** | Planning and task overhead dominate | Compaction (Chapters 04/05) |
| **`SELECT *` on a 300-column wide table** | Reads all 300 column chunks | Project columns; create narrowed views |
| **Unsorted ID column used for min/max pruning** | Stats cover the whole range → no skipping | Sort, bucket, or bloom filter |
| **Compressed CSV (`.csv.gz`) for parallel processing** | One task per file (not splittable) | Decompress once into Parquet, or use Parquet/ZSTD |
| **JSON arrays (not JSONL) as a big file** | Whole-file parse; no streaming splits | JSONL; or convert to Parquet |
| **Unbounded nested schema (auto-inferred JSON)** | Hundreds of leaf columns, some `struct<…>` 10 levels deep | Flatten explicitly; pin the schema; reject unbounded evolution |
| **Ignoring dictionary fallback** | One column is 100× larger than others | Move high-cardinality columns out of dictionary expectations; sort them |
| **Using Avro for analytical tables** | Full-row reads, no pruning, huge scans | Parquet/ORC |
| **Floats for money / microsecond precision in float** | Rounding and drift | `DECIMAL`, integers of minor units, or epoch micros as `BIGINT` |
| **`writeLegacyFormat=true` (Spark default historically)** | Old-style Int96 timestamps, weaker logical types, interop issues | Disable legacy format; write modern logical types |
| **Ignoring the footer read cost at high file counts** | Each file costs one GET for the footer | Fewer files; table format with manifest stats |

---

## 7. Real-world use cases and industry applications

| Context | Format decision | Rationale |
|---|---|---|
| **Netflix analytics lake** | Parquet inside Iceberg | Page index + stats enable skipping at petabyte scale; Iceberg handles evolution |
| **Uber's Hudi lake** | Parquet base files + Avro/HFile log files | Upsert-friendly write path (log blocks) with columnar reads once compacted |
| **Kafka-centered event platforms** | Avro/Protobuf values + Schema Registry | Row-level evolution, compactness, cross-language support |
| **Airbnb / data platform** | Parquet for modeled tables; Avro for ingestion | Same split: transport vs analytics |
| **Ad-tech / gaming telemetry** | Parquet + ZSTD, tight file sizing, frequent compaction | Volume makes per-byte cost dominant |
| **Financial market data** | Parquet or ORC with decimal + nanosecond timestamps; sometimes specialized binary formats | Precision requirements; huge row counts; sorted by instrument/time |
| **Healthcare (HL7/FHIR landing)** | JSON/XML landing → Parquet with flattened clinical fields | Regulatory fidelity in raw, usability in curated |
| **ML feature pipelines** | Parquet wide tables + Arrow for in-memory training | Column pruning, ecosystem support, fast handoff to training |
| **Retail partner data** | CSV landing → typed Parquet silver | You cannot control the source; you can control the conversion |
| **Log analytics** | JSONL → Parquet with a raw line column + extracted fields | Balances flexibility and scan cost |

---

## 8. Senior-level deep dive

### 8.1 Encoding + compression interaction (why order matters)

```text
Naive:  compress the raw byte stream (CSV + gzip)
          → gzip sees mixed types, repeated whole rows → ratio ~4×, no pruning, slow parse

Columnar: encode then compress
          country   (200 distinct)  dictionary+RLE  → 0.05× before codec
          event_ts  (sorted, delta) DELTA_BINARY_PACKED → 0.15× before codec
          amount    (decimal, similar magnitudes) plain + ZSTD → 0.4×
          payload   (free text)     plain + ZSTD → 0.55×
          → weighted overall ≈ 0.2× of raw values, then ZSTD takes another 1.5–2×
          → and, crucially, a query touching 4/40 columns reads a fraction of it
```

Rule: **encoding choices explain 5–10×; codec choice explains another 1.2–2×.** Tuning the codec
while ignoring cardinality, sorting, and types is optimizing the wrong 10%.

### 8.2 Row group sizing: the three-way trade-off

| Row group size | Requests per file | Memory per task | Pruning granularity | Net effect |
|---|---|---|---|---|
| 8 MB | Many (footer + many range reads) | Low | Fine | Good on highly selective queries, bad on big scans |
| 128 MB (default) | Moderate | ~128 MB × cols read | Coarse but usually sufficient | Balanced |
| 1 GB | Few | High (spill/OOM risk) | Very coarse | Bad when statistics matter; OK for full scans |

For Parquet specifically: a row group must be buffered in memory by the writer, so a 1 GB row group
means ~1 GB per concurrent-writer task. Writers typically configure "row group = target file size"
which is why 128 MB files give 128 MB row groups.

### 8.3 Choosing a format for a specific workload (decision table)

| Workload signature | Format | Layout guidance |
|---|---|---|
| Wide table, few columns queried, time-filtered | Parquet + ZSTD, partitioned by date, sorted by 2 columns | 128 MB row groups, bloom filters on ID columns if equality-filtered |
| Narrow table (< 8 cols), full scans | Parquet (any codec) or ORC | Fewer, larger files |
| Append-only event log read by Kafka consumers | Avro | Keep row-oriented; compact into Parquet in the lake |
| Update-heavy CDC landing | Avro/JSON in raw; Parquet + MoR in silver | Log files (Hudi) or DVs (Iceberg/Delta) |
| Feature store offline | Parquet wide + Arrow for training | Column pruning critical; avoid nested |
| Small dimension tables | Parquet (or ORC) single file | Avoid many files; broadcast-friendly sizing (< 100 MB) |
| Cold archive, occasional audit | Parquet + ZSTD-19 in Glacier | Max compression, retrieval latency acceptable |
| ML text corpus | Parquet with a `content` string column (+ offsets) | Bloom filters useless; consider a search index instead |

### 8.4 Failure modes and debugging

| Symptom | Likely cause | Diagnostic | Fix |
|---|---|---|---|
| Query reads 10× more bytes than expected for 4 columns | Nested schema fan-out or `SELECT *` | `pyarrow` column list; plan projection | Flatten; project explicitly |
| One column far larger than the others | Dictionary fallback on high cardinality | Per-column `total_compressed_size` in metadata | Sort by that column, use delta/byte-array encodings, or move it out |
| No file skipping despite a filter | Unsorted data → wide min/max; or stats truncated | Compare `min`/`max` in footer to the filter values | Sort on write; enable bloom filters; check `is_min_value_exact` |
| `Column statistics truncated, cannot be used` (or over-pruning) | Long strings exceed the 64-byte stat truncation | Writer logs / engine warnings | Store a hash/short key for filtering; sort by prefix |
| Task count = file count, each tiny | Small files | Count files and sizes | Compaction; `maxPartitionBytes`; AQE coalescing |
| Job fails with "corrupt record" in random places | CSV quoting/encoding (Latin-1, embedded newlines) | Sample raw lines; validate encoding | Specify encoding + quote/escape; quarantine; convert early |
| Numbers differ between CSV-sourced and JSON-sourced copies | Float precision / type coercion | Diff aggregates on the same key | Use decimals/exact types at the boundary |
| JSON read is 5× slower than expected | Non-JSONL (array) or huge nested objects | Check file structure; measure parse vs decode | Convert to JSONL, then to Parquet |
| Timestamps shift by hours | Timezone handling in CSV/JSON parsing (`XXXX` vs `XXX` vs naive) | Compare min/max timestamps across layers | Force UTC + explicit format strings |
| Old files unreadable after schema change | Avro reader schema missing defaults; Parquet missing field IDs | Error details | Provide reader schema with defaults; use a table format for evolution |
| Engine can't read a Parquet feature (e.g., new logical type, VARIANT) | Writer/reader version skew | Error on read; check writer version | Pin writer versions; test cross-engine in CI |

Debugging toolkit recap:

```text
Parquet: pyarrow.parquet.ParquetFile → row groups, column chunks, statistics, encodings
         parquet-tools meta / cat / dump / rowcount
         DuckDB: SELECT * FROM parquet_metadata(...)
ORC:     orc-tools meta / data / scan
Avro:    avro-tools getschema, tojson, random; `java -jar avro-tools.jar`
General: compare bytes-scanned metrics before/after any writer change
```

### 8.5 Schema evolution per format (the part that bites in year two)

| Evolution | Parquet alone | ORC alone | Avro | With a table format (Iceberg/Delta) |
|---|---|---|---|---|
| Add nullable column | Works (old files → null) | Works | Works (default value) | Works, metadata-only |
| Add non-nullable column | Breaks readers | Breaks | Needs default | Works with a declared default |
| Rename column | **Breaks** (name-based) | Breaks | Works via aliases | Works (Iceberg field IDs / Delta column mapping) |
| Drop column | Works (ignored) | Works | Works | Works (metadata-only in Iceberg) |
| Reorder columns | Works (name-based in modern readers) | Works | Works | Works |
| Widen type (int→long) | Partially (reader must widen) | Partially | Works (promotion rules) | Works (Delta type widening; Iceberg promotion) |
| Change nested structure | Fragile | Fragile | Versioned schemas | Field-ID based; still careful |
| Change partition scheme | n/a | n/a | n/a | Iceberg evolution / liquid clustering |

> **Senior note**: the reason Iceberg/Delta exist at all is that raw Parquet/ORC lack a safe evolution
> and commit story. If you are choosing between "Parquet files with a Hive schema" and "Parquet
> files with Iceberg", you are choosing between name-based fragility and field-ID stability.

---

## 9. Common mistakes and how to avoid them

1. **Treating CSV/JSON as a storage format.** Fix: parse-once into columnar at the zone boundary.
2. **Choosing one format for everything.** Fix: match role — wire vs columnar vs archive.
3. **Ignoring file sizing because "Parquet is efficient".** Fix: file sizing is a first-class
   requirement (128 MB–1 GB, < 100 files/partition).
4. **Forgetting to sort.** Fix: `sortWithinPartitions` by the dominant predicates during normal
   writes and compaction.
5. **Believing compression codec choice is the main lever.** Fix: encodings + sorting + cardinality
   first.
6. **Reading `SELECT *` from a wide table in a notebook and then filtering.** Fix: project and
   filter in the source scan.
7. **Letting JSON schemas auto-infer.** Fix: pin the schema; quarantine surprises; reject unbounded
   evolution.
8. **Using floats for money or for high-precision timestamps.** Fix: `DECIMAL`/`BIGINT`.
9. **Never validating what the writer produced.** Fix: inspect a sample file's metadata in CI after
   any writer upgrade.
10. **Assuming all engines interpret a logical type identically.** Fix: test cross-engine reads for
    timestamps, decimals, and nested types; document the contract.
11. **Compressing with GZIP out of habit.** Fix: ZSTD unless a specific constraint applies (Chapter
    07).
12. **Letting nested-web-API-shaped JSON flow into the curated layer unchanged.** Fix: flatten into
    queryable entities; keep raw for fidelity.

---

## 10. Exercises / interview questions

1. **Internals.** Describe the byte-level structure of a Parquet file and explain how a reader
   answers `SELECT a FROM t WHERE b > x` using only part of the file. *Hint: footer → row-group
   stats → ColumnIndex → column chunks → pages.*

2. **Numbers.** A 40-column, 500 GB CSV table is converted to Parquet. Estimate the Parquet size and
   the bytes a 4-column query reads. Then compute the same for a 12-column query. *Hint:
   compression ratios and column-fraction arithmetic.*

3. **Encoding.** For each column, pick the encoding the writer should choose and why:
   `country` (200 values), `session_id` (UUID), `amount` (decimal), `ts` (sorted timestamp),
   `is_bot` (boolean), `embedding` (1536 floats). *Hint: dictionary, plain (or bloom+sort),
   plain/decimal, delta, RLE, byte-stream-split.*

4. **Design.** Your nested schema has 240 leaves. Users query 6 of them 95% of the time and need the
   full payload 0.1% of the time. Design the physical layout. *Hint: flattened hot columns +
   separate payload column or table; document the join.*

5. **Diagnosis.** A DataFrame write produces 12,000 files averaging 700 KB. Enumerate four causes
   and the fixes. *Hint: over-partitioning, high task parallelism, streaming micro-batches, small
   partitions.*

6. **Trade-off.** Argue for ORC over Parquet in a Hive-centric bank, then argue the reverse for the
   same bank in 2026. *Hint: ecosystem momentum, tooling, table-format support, ACID legacy.*

7. **Avro.** Explain how Avro resolves a writer schema with a field the reader doesn't know and a
   reader field the writer never wrote. When does it fail? *Hint: ignored / default value /
   error when no default.*

8. **CSV pathology.** Find five ways a CSV file can be silently wrong while looking fine in Excel.
   *Hint: leading-zero IDs, embedded delimiters/newlines, encoding, localized decimals, repeated
   headers, mixed line endings.*

9. **Pushdown.** Why can't a plain Parquet file be filtered by a non-key column without reading it?
   What would you add to fix it? *Hint: min/max statistics; add bloom filter or sort order.*

10. **Interview classic.** "Why not store everything as JSON in S3 and let the query engine figure it
    out?" Give the six concrete costs. *Hint: parse, no stats, nested fan-out, no types, no
    evolution guarantees, no column pruning.*

---

## References

- Apache Parquet — **File format documentation (structure, pages, encodings, page index)**. https://parquet.apache.org/docs/file-format/
- Apache Parquet — **Encodings (PLAIN, RLE, DELTA_*, BYTE_STREAM_SPLIT)**. https://parquet.apache.org/docs/file-format/data-pages/encodings/
- Apache Parquet — **Page index for page-level filtering**. https://parquet.apache.org/docs/file-format/pageindex/
- apache/parquet-format — **Encodings.md** (dictionary fallback semantics). https://github.com/apache/parquet-format/blob/master/Encodings.md
- Melnik et al. — **Dremel: Interactive Analysis of Web-Scale Datasets** (repetition/definition levels). https://research.google/pubs/pub36632/
- Apache ORC — **ORC specification (stripes, index streams, RLE v2)**. https://orc.apache.org/specification/ORCv1/
- Apache ORC — **Indexes and predicate pushdown**. https://orc.apache.org/docs/indexes.html
- Apache Avro — **Specification (object container files, codecs, schema resolution)**. https://avro.apache.org/docs/1.11.1/specification/
- Apache Avro — **Object container file internals and codecs**. https://avro.apache.org/docs/1.11.1/specification/#object-container-files
- Apache Spark — **Parquet files: configuration and tuning**. https://spark.apache.org/docs/latest/sql-data-sources-parquet.html
- Apache Arrow — **Columnar format specification**. https://arrow.apache.org/docs/format/Columnar.html
- DuckDB — **Parquet metadata functions (`parquet_metadata`)**. https://duckdb.org/docs/data/parquet/metadata
- Trino — **Hive connector: Parquet/ORC performance and file layout guidance**. https://trino.io/docs/current/connector/hive.html
- Iceberg — **Field IDs and why name-based evolution is unsafe**. https://iceberg.apache.org/spec/#schemas
- Delta Lake — **Column mapping / schema evolution**. https://docs.delta.io/latest/delta-column-mapping.html
