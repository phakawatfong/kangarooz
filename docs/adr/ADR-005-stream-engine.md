# ADR-005: Stream engine — Spark Structured Streaming

Status: Accepted    Date: 2026-10-26    Owner: Kids

## Decision

**Spark Structured Streaming in local mode.** Alerts → `fx.alerts`; trades → `foreachBatch` → DuckDB → DuckLake.

## 1. Driving requirement

Stateful, windowed alerts at **p95 < 5 s** (C4), on one laptop with one operator (C1, C3), writing trades into DuckLake exactly once (C7).

Must-haves (both Spark and Flink pass):
- Keyed, windowed state (per pair, per user) with event time + watermarks (30 s lateness rule)
- Durable state + recovery (checkpoints)
- Replay from Kafka offsets (Kappa backtests, C8)

Spark meeting p95 < 5 s is an assumption, to be measured in the Phase 3 spike (trade → alert).

## 2. Alternatives considered

| | Spark Structured Streaming | Flink |
|---|---|---|
| Processing model, latency | Micro-batch, seconds | Per event, milliseconds |
| What you run | One JVM process (local mode), already running for ingest | JobManager + TaskManager(s) |
| State and event time | Windows, watermarks, RocksDB state | Richest: timers, CEP (event patterns) |
| Python | First-class (PySpark) | PyFlink exists; the Java API is the mature one |
| Sink to DuckLake | `foreachBatch` → DuckDB, proven in the Phase 2 spike | No DuckLake connector; would need a custom sink |
| Batch layer | Same engine | Separate |

Dismissed early: Quix Streams (Python, no cluster, but young, and it would not reuse the proven Spark sink path).

## 3. Deciding axis

**Latency headroom vs cost of ownership.** Spark's micro-batch fits inside the 5 s SLA, so Flink's millisecond latency buys nothing here. Spark also keeps the proven `foreachBatch` → DuckLake path with its one-transaction exactly-once, while Flink would need a custom sink. Familiarity breaks the tie.

The DuckLake Spark connector is append-only and immature, so Spark writes through `foreachBatch` → DuckDB, which keeps the one-transaction path.

## 4. What I gave up

- A latency floor set by the trigger interval; alerts arrive in batches per trigger
- Weaker stateful tooling than Flink: no native CEP, clunkier per-key timers
- A heavy JVM on 16 GB

## 5. What would change my mind

- The SLA drops below ~1 s
- Rules need event sequences, e.g. deposit → trade → withdraw within 10 min (CEP)
- The Phase 3 spike shows Spark can't hold p95 < 5 s at the production rate
- A platform team that runs Flink becomes available
