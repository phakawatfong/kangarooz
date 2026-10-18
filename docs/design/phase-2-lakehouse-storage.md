# Phase 2 — Lakehouse Storage (Week 2)

**Status:** design done (ADR-003, ADR-004 accepted) · spike next · **DDIA:** Ch 3

## Goal
Land raw trades durably in a lakehouse and be able to defend every storage layer.

## Lakehouse layers (design session, 2026-10-17)

| Layer | Choice | Why (one line) |
|---|---|---|
| Object storage | Garage locally → S3 on AWS | Same S3 API, so moving is config-only ([ADR-004](../adr/ADR-004-object-storage.md)) |
| File format | Parquet | Columnar, compressed, predicate pushdown. *Avro on the wire, Parquet at rest.* |
| Table format | DuckLake | Data inlining solves streaming small files; SQL catalog ([ADR-003](../adr/ADR-003-table-format.md)) |
| Catalog | Postgres | Comes with DuckLake, no extra service |
| Engine | DuckDB (queries + batch layer); Spark for streaming only | Fast on one node, native DuckLake support |

Switch when: one machine can't handle the data → distributed engine; many concurrent users → Trino.

## The small-file problem

- **Why it happens:** every streaming micro-batch commit writes at least one new Parquet **data file**. At a 5 s trigger that's ~17k commits/day → thousands of tiny files. Metadata growth is a side effect.
- **Why it hurts:** every file costs an open, a footer read and an S3 request (latency + money); tiny files compress poorly and have weak min/max statistics (worse pruning); planning slows down.
- **Fixes:**
  - Generic: scheduled compaction (target ~128 MB–1 GB files; Delta `OPTIMIZE` defaults to ~1 GB); longer trigger interval (fewer, bigger files, at the cost of latency); coarse partitioning (by day, not by pair/minute).
  - DuckLake-specific: **data inlining** — small inserts are stored as rows in Postgres until a checkpoint flushes them to Parquet. This is the main reason for choosing DuckLake.

## Data layers (medallion)

- **Bronze:** raw records as landed, append-only, duplicates allowed. Adds `etl_loaded_at` plus Kafka partition and offset (lineage + replay). Engineers only.
- **Silver:** deduplicated **at write time** (MERGE on `trade_id`), validated (bad records → quarantine), late trades included. Analysts query this.
- **Gold:** aggregates for BI reports and ML features.
- **Alerts table (`anomaly_event`):** merged on `alert_id` (see Phase 3) so reprocessing doesn't duplicate fired alerts.

Dedupe at write time, not query time, so no report double-counts volume because someone forgot a `DISTINCT`.

## Decisions (ADRs)
- [ADR-003](../adr/ADR-003-table-format.md) Table format: DuckLake (Postgres catalog) — Accepted
- [ADR-004](../adr/ADR-004-object-storage.md) Object storage: Garage locally → S3 on AWS — Accepted

## Spike plan (to implement with Claude Code)

**Scope:** Redpanda → Spark Structured Streaming → `foreachBatch` → DuckDB → DuckLake **bronze** (Postgres catalog, Parquet on Garage) → DuckDB MERGE into **silver**.

Components to add to `docker-compose.yml`: Postgres (DuckLake catalog), Garage (bucket + access key), Spark job (local mode is fine).

Known gotchas:
- Messages use the **Confluent wire format** (5-byte header: magic byte + 4-byte schema ID). Spark's built-in `from_avro` does not strip it — strip the 5 bytes (fixed schema) or use a registry-aware decoder.
- `foreachBatch` can re-run a batch after a failure → bronze may get duplicates (allowed); silver MERGE removes them.
- Pin versions (Spark, DuckDB, DuckLake extension, Garage) in `.env` like Phase 1.

**Acceptance checks (prove the ADRs):**
- [x] Bronze row count = messages produced (duplicates included); has `etl_loaded_at`, partition, offset
- [x] Silver distinct `trade_id` count = unique trades produced; no duplicates after the dedupe
- [x] Late events (from the producer's chaos) land in bronze and silver
- [x] **Small files:** after ~10 min of `make produce`, Parquet file count in Garage stays low with inlining on; compare with inlining off. Record the inlining row limit used vs ~2k rows/micro-batch (ADR-003 risk)
- [x] `MERGE INTO` on DuckLake works in the pinned DuckDB version (works, but bypasses inlining; see results)
- [x] Kill and restart the Spark job → resumes from checkpoint, no lost trades
- [x] A separate DuckDB process can query bronze/silver
- [x] Storage endpoint + credentials come only from env/config (S3 switch = config change, ADR-004)
- [ ] Optional: Superset → DuckDB/DuckLake connection (skipped)

## Spike results (2026-10-18)

Pinned: DuckDB 1.5.6 (DuckLake 1.0), pyspark 4.2.0 on Java 17, Postgres 17.9, Garage v2.4.1. Run with `make stream`, `make produce`, `make lake-verify`.

**What was built.** One DuckLake transaction per 5 s micro-batch:
1. skip if `batch_id` is already in the `ingest_batch` ledger;
2. append to bronze with Kafka lineage;
3. insert into silver the `trade_id`s it doesn't have;
4. record the ledger row.

A replayed Spark batch is a no-op, so each Kafka offset lands exactly once. That's stronger than the "bronze may get duplicates" assumption above. Decoding happens in the driver with the Phase 1 fastavro decoder. The schema id is looked up from the registry, and any other header fails the batch.

**Small files, 10 min at ~560 trades/s (121 batches, ~2.8k rows per batch):**

| Inlining row limit | Parquet files | Batch write p50 / p95 |
|---|---|---|
| 5000 (flush every 60 batches) | 21 | 1.24 s / 2.53 s |
| 0 (off) | 363 (3 per batch: bronze, silver, ledger) | 0.18 s / 0.22 s |

- Inlining cuts files 17x. Batches still finish inside the 5 s trigger.
- The cost is a write about 7x slower, because rows go into Postgres instead of one Parquet file. Inferred cause: per-row inserts into the catalog. Not profiled.
- Batches above the limit (catch-up, bursts) skip inlining and go straight to Parquet. So inlining only helps near the dev rate. At the 1–5k/s production target, a 5 s batch holds 5–25k rows: either raise the limit (and the latency), or accept Parquet per batch plus compaction. This is ADR-003's "what would change my mind" #3 in numbers.

**MERGE finding.** In DuckDB 1.5.6, a `MERGE INTO` that inserts rows writes a new Parquet file every time, even below the inlining limit. An `INSERT … WHERE NOT EXISTS` with the same insert-if-absent meaning stays inlined. Chaos duplicates are identical copies, so silver uses the anti-join insert. Revisit when DuckLake inlines MERGE.

**Crash tests.**
- Two SIGKILLs of the stream, one during production and one between batches. After each restart, bronze matched Kafka offset for offset.
- To test the replay path, I deleted Spark's newest commit marker. That is the state of a crash after the lake commit but before Spark's commit. Spark re-ran the batch and the ledger skipped it (`replay, skipped`). Bronze stayed exact.
- Gotcha: delete the matching `.N.crc` file too, or Spark refuses to rewrite the marker.

**Operational notes.**
- A machine-wide `SPARK_HOME` breaks pyspark ("'JavaPackage' object is not callable"), so the Makefile unexports it.
- The Spark JVM outlives a killed Python driver by a few seconds.

## Exit criteria
- [x] ADR-003, ADR-004 written
- [ ] Raw trades table design (columns, partitioning, dedupe) — confirmed by the spike
- [x] Spike acceptance checks pass
- [ ] Mock answered: "Why not Iceberg?" / "Why not just Postgres?"
