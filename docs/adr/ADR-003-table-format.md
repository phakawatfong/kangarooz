# ADR-003: Table format — DuckLake (Postgres catalog)

Status: Accepted    Date: 2026-10-17    Owner: Kids

## Decision

**DuckLake**, with **Postgres as the catalog** and **Parquet** data files on object storage (Garage locally → S3 on AWS, see [ADR-004](ADR-004-object-storage.md)).

## 1. Driving requirement

**Spark micro-batches every ~5 s into a single-node lakehouse must not create a small-file problem, and silver needs MERGE on `trade_id`.**

Must-haves (every option below passes):
1. Open table format over Parquet
2. ACID transactions
3. Time travel

Constraints:
- Small team (one operator) — start simple
- Single-node query, no distributed engine yet (16 GB laptop)

Context from Phase 1 sizing: ~390 msg/s sustained → **~2,000 rows per 5 s micro-batch**.

## 2. Alternatives considered

Where the metadata lives is the key difference between the formats:

| Format | Where metadata lives | Needs a catalog? |
|---|---|---|
| Delta | `_delta_log/` beside the data, in object storage | Optional (path-based tables work) |
| Hudi | `.hoodie/` timeline beside the data | Optional |
| Iceberg | Tree of metadata files + a catalog holding the current pointer | **Yes**, for atomic commits |
| DuckLake | Tables in a SQL database (Postgres) | The database *is* the catalog |

| Option | Strengths | Weaknesses |
|---|---|---|
| **DuckLake** | Simple, developer-friendly; **data inlining** absorbs small writes into the catalog instead of creating tiny files; SQL catalog; native in DuckDB | Early stage, small ecosystem; Spark connector is append-only |
| **Apache Iceberg** | Engine-agnostic (Spark, Flink, Trino, Athena, Dremio); AWS-native (Glue, Athena, S3 Tables); hidden partitioning, partition evolution | Needs a catalog service (REST / Glue / Nessie); streaming small files need compaction |
| **Delta Lake** | Spark-native; deletion vectors; CDF; my production experience (Databricks) | Strongest on Spark/Databricks; DuckDB's Delta support is read-focused; streaming small files need `OPTIMIZE` |

Dismissed early: **Hudi** — powerful for upserts but complex (many settings) and Spark-centric.

## 3. Deciding axis

**It came down to small-file handling on a single node vs ecosystem reach.** Small files mattered more: DuckLake's inlining absorbs ~5 s micro-batches into Postgres, so there are no compaction jobs and no separate catalog service. Iceberg and Delta would both need scheduled compaction.

Write path (why the append-only Spark connector doesn't hurt):
- Spark writes via `foreachBatch` → DuckDB → DuckLake (**bronze, append-only**). Going through the DuckDB client is also what makes inlining apply.
- **Silver MERGE on `trade_id` runs in DuckDB**, not Spark.

## 4. What I gave up

1. Engine reach — no Trino, Athena or Flink today
2. Maturity — young project, small community
3. Native Spark writes (Spark goes through DuckDB in `foreachBatch`)
4. Postgres becomes a single point of failure for the whole lake

## 5. What would change my mind

1. More than one engine needs to query the tables → **Iceberg**
2. The team adopts Databricks → **Delta**
3. Inlining can't keep up at ~2k rows per micro-batch (spike result), or DuckLake development stalls → re-evaluate (**Iceberg**)

## Interview line

> "Iceberg wins on ecosystem, and it's where I'd go once more than one engine needs the data. For a single-node streaming lakehouse, DuckLake's inlining removes the small-file problem without compaction jobs or a separate catalog service — and I wrote down what would trigger the switch."

## To verify in the Phase 2 spike

- DuckLake's data-inlining row limit vs ~2,000 rows per micro-batch (likely needs raising)
- `MERGE INTO` on a DuckLake table works in the pinned DuckDB version
