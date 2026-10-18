# ADR-003: Table format — DuckLake (Postgres catalog)

Status: Accepted (revised 2026-10-18 after the Phase 2 spike)    Date: 2026-10-17    Owner: Kids

## Decision

**DuckLake**, with **Postgres as the catalog** and **Parquet** data files on object storage (Garage locally → S3 on AWS, see [ADR-004](ADR-004-object-storage.md)).

## 1. Driving requirement

**Spark micro-batches every 5 s into a single-node lakehouse run by one person: keep the file count bounded at 500–5,000 trades/s, and land bronze + deduplicated silver exactly once, without adding services.**

Constraints that apply (IDs from the [constraint trace](../design/00-overview.md#3-constraints--decisions-trace)):

| ID | Constraint | What it asks of the table format |
|---|---|---|
| C1 | One 16 GB laptop; DuckDB is the query and batch engine, Spark only streams | DuckDB must **write** the format, not just read it |
| C3 | One operator | As few services as possible |
| C7 | No trade lost, duplicates possible | Transactional writes; dedupe on `trade_id` |
| C11 | Dev ~500/s → ~2.5k rows per 5 s batch; production 1–5k/s → 5–25k rows per batch | Small files at both rates |
| C13 | Move to AWS without code changes | Works on S3 |

Must-haves (every option below passes): open table format over Parquet, ACID transactions, time travel.

## 2. Alternatives considered

Where the metadata lives is the key difference between the formats:

| Format | Where metadata lives | Needs a catalog? |
|---|---|---|
| Delta | `_delta_log/` beside the data, in object storage | Optional (path-based tables work) |
| Hudi | `.hoodie/` timeline beside the data | Optional |
| Iceberg | Tree of metadata files + a catalog holding the current pointer | **Yes**, for atomic commits |
| DuckLake | Tables in a SQL database (Postgres) | The database *is* the catalog |

Trade-offs against the constraints (✓ = fits, ~ = fits with a cost, ✗ = conflicts):

| Constraint | DuckLake | Apache Iceberg | Delta Lake |
|---|---|---|---|
| C1 DuckDB writes it | ✓ native | ~ writes go through a REST catalog | ✗ DuckDB support is read-focused; writes need Spark |
| C3 services to run | ~ Postgres (a plain database) | ✗ REST catalog service (Lakekeeper / Nessie / Polaris), plus its database | ✓ none (path-based) |
| C7 bronze + silver + ledger in one commit | ✓ one catalog transaction across tables (built and crash-tested in the spike) | ~ commits are per table | ~ commits are per table |
| C11 small files at dev rate | ✓ inlining: 21 vs 363 files in 10 min (measured) | ~ compaction job | ~ `OPTIMIZE` job |
| C11 small files at production rate | ~ compaction job: one SQL call in DuckDB, 103 files → 1 in 5.6 s (measured) | ~ compaction job | ~ `OPTIMIZE` job, runs on Spark |
| Engine reach | ✗ DuckDB only today | ✓ Spark, Flink, Trino, Athena; AWS-native (Glue, S3 Tables) | ~ Spark / Databricks first |
| Maturity | ✗ 1.0, small community | ✓ mature | ✓ mature |

Dismissed early: **Hudi** — powerful for upserts but complex (many settings) and Spark-centric.

## 3. Deciding axis

**It came down to fit for one operator on one node (C1, C3, C7) vs ecosystem reach, and fit mattered more because no second engine exists yet.**

DuckLake is the only option that does all three at once:
1. DuckDB, the engine the project already runs, writes it natively. Delta would need Spark for every write; Iceberg would need a catalog service.
2. Its catalog is a SQL database, so bronze, silver and the batch ledger commit in **one transaction**. That one transaction is the whole exactly-once write path; with per-table commits it would need cross-table recovery logic.
3. At the dev rate, inlining keeps files few with no job at all. At the production rate it needs compaction like the others, but that is one SQL call from the same DuckDB process, safe next to the live stream.

Iceberg's reach is real, but it only pays off once a second engine needs the data, and that trigger is written down in step 5.

> **Revision note (spike, 2026-10-18).** The original axis was "small-file handling: inlining means no compaction jobs, Iceberg and Delta need them." The spike showed inlining only applies below its row limit: at ~3,000 trades/s, 102 of 103 batches skipped it. All three formats need compaction at the production rate, so small files no longer separate them. The decision stands on the reasons above (DuckDB-native writes, multi-table transactions, one fewer service than Iceberg), not on avoiding compaction.

Write path:
- Spark writes via `foreachBatch` → DuckDB → DuckLake. Going through the DuckDB client is what makes inlining and the multi-table transaction apply.
- **Silver dedupe on `trade_id` runs in DuckDB**, in the same transaction as bronze. It is an insert-if-absent, not `MERGE INTO`, which bypasses inlining in DuckDB 1.5.6.

## 4. What I gave up

1. Engine reach — no Trino, Athena or Flink today
2. Maturity — young project, small community
3. Native Spark writes (Spark goes through DuckDB in `foreachBatch`, so the lake write runs on the driver)
4. Postgres becomes a single point of failure for the whole lake
5. Write latency at the dev rate — inlined batches write ~7x slower than plain Parquet (p50 1.24 s vs 0.18 s, measured); still inside the 5 s trigger
6. A compaction job at the production rate, same as the alternatives (`make compact`)

## 5. What would change my mind

1. More than one engine needs to query the tables (Trino / Athena for BI, Flink in Phase 3) → **Iceberg**
2. The team adopts Databricks → **Delta**
3. Compaction can't keep the file count bounded at the production rate (a run takes close to its schedule interval), or DuckLake development stalls → re-evaluate (**Iceberg**)
4. The Postgres catalog becomes the bottleneck or an unacceptable single point of failure in production → managed Postgres first (RDS), then **Iceberg** on AWS S3 Tables

The original #3 ("inlining can't keep up at ~2k rows per micro-batch") was met at the production rate by the spike; the answer chosen was compaction, not a format switch, because every alternative needs compaction too.

## Interview line

> "One laptop, one operator, DuckDB as the engine. DuckLake is the format DuckDB writes natively, and because its catalog is a database, bronze, silver and my batch ledger commit in one transaction. At the dev rate inlining keeps files few with no job; at production rate it needs compaction like Iceberg or Delta — I measured 103 files down to 1 in six seconds next to the live stream. Iceberg wins on reach, and I'd switch the day a second engine needs the data."

## To verify in the Phase 2 spike

- DuckLake's data-inlining row limit vs ~2,000 rows per micro-batch (likely needs raising)
- `MERGE INTO` on a DuckLake table works in the pinned DuckDB version

## Spike results (2026-10-18)

Details in [phase-2 spike results](../design/phase-2-lakehouse-storage.md#spike-results-2026-10-18).

- **Inlining limit.** Raised from DuckLake's default 10 to 5,000. At ~560 trades/s (~2.8k rows per batch) inlining cut Parquet files 17x (21 vs 363 in 10 min), at ~7x the write latency (p50 1.24 s vs 0.18 s), still inside the 5 s trigger.
- **Production rate.** At ~3,000 trades/s (~14.5k rows per batch) 102 of 103 batches skipped inlining: one ~1 MB file per batch per table. Raising the limit further costs more write latency than the trigger allows.
- **Compaction.** `ducklake_merge_adjacent_files` with a 128 MB `max_file_size`, then snapshot expiry and file cleanup (`make compact`): 103 files → 1 of 105.6 MB in 5.6 s; ran next to the live stream without conflicts in testing.
- **MERGE.** `MERGE INTO` works, but writes a Parquet file per call even below the inlining limit. Silver uses an insert-if-absent with the same meaning.
