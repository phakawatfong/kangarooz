# F1 — Design Overview

> Real-time FX trading analytics platform. A design-first portfolio project: the goal is
> **architecture judgment**, so every component is chosen deliberately and defended in an ADR.
> Code exists to prove a design, not to ship a product.

## 1. Problem & scope

Detect anomalous FX trades in near real time, and give analysts accurate historical views of
trading activity, on a stack that runs on one 16 GB laptop.

**In scope:** synthetic FX trade stream, real-time anomaly alerts, analytical views,
detector grading against ground truth, infrastructure as code, optional read-only MCP server.

**Out of scope:** production uptime, real users, ML models (statistical rules first),
Kubernetes (scaling is argued on paper), a polished frontend.

## 2. Requirements

### Functional
- Synthetic FX producer with **labelled injected anomalies** and on-demand late / duplicate events.
- Real-time alerts from windowed statistics (price z-score, VWAP deviation, volume / account bursts).
- Analytical views: per-pair OHLC / VWAP / volume, anomaly history, detector precision & recall.
- Rule backtesting by replaying history through the same streaming job (Kappa property).
- Optional: read-only MCP server with named tools.

### Constraints
- Open-source **or source-available**, self-hostable, no paid licences (Redpanda is BSL: say "source-available").
- Runs on a 16 GB laptop with Docker Compose. No Kubernetes.
- Infrastructure as code with OpenTofu.

### Non-functional targets
| Dimension | Production design target | Laptop dev profile (Phase 1 sizing) |
|---|---|---|
| Throughput | 1–5k trades/s sustained, 20k/s bursts | 500/s sustained, 20k/s bursts |
| Alert latency | p95 < 5 s trade → alert | same |
| Analytics freshness | < 60 s | same |
| Late events | accepted up to 30 s (watermark); later → batch layer corrects | same |
| Delivery | at-least-once + idempotent sink (dedupe on `trade_id`) | same |
| Redpanda retention | 7 days (replay / backtest window), RF=3 | 24 h, RF=1 |
| Schema | Avro, BACKWARD compatibility via Schema Registry | same |

## 3. Constraints → decisions (trace)

Design by constraint: every decision should trace back to a constraint or a sizing number,
not to "I know this tool". **For ADR-005 onwards, write the applicable constraints (step 1)
before listing any options**, then let them filter the alternatives.

| # | Constraint / number | What it forces | Decision | ADR |
|---|---|---|---|---|
| C1 | One 16 GB laptop, Docker Compose, no Kubernetes | Light components, single-node engine | Redpanda (single binary, no JVM); DuckDB; Garage (single container); Spark in local mode, streaming only | 002, 003, 004 |
| C2 | Open-source or source-available, zero cost | Self-host everything locally | Garage locally instead of S3; MinIO CE out (archived); Redpanda accepted as BSL (say "source-available"); OpenTofu over Terraform | 002, 004, 009 |
| C3 | One operator | Fewest services to run | Redpanda's built-in Schema Registry + Console; DuckLake's catalog is Postgres (no separate catalog service); Garage over SeaweedFS | 002, 003, 004 |
| C4 | Alerts p95 < 5 s | A streaming path for alerts | Kappa for alerts; micro-batch latency is acceptable | 001 (005) |
| C5 | Trades > 30 s late must still count in reports | Recompute after the watermark | Lambda for analytics: batch model over raw events | 001 |
| C6 | Analytics freshness ~60 s | A fresh but approximate view | Speed view; serving by use case | 001 |
| C7 | No acknowledged trade lost; duplicates possible | At-least-once + idempotent sink | Producer acks=all + idempotence; bronze allows duplicates, silver MERGE on `trade_id` | 001, 003 (006) |
| C8 | Replay for rule backtesting | Durable, replayable log | Redpanda retention = replay window; Kappa replay | 001, 002 |
| C9 | Money values | Exact decimals, never floats | Avro `decimal(18,8)` (Protobuf/JSON rejected) | 002 |
| C10 | Schema evolution, compact messages | Enforced schemas with compatibility rules | Avro + Schema Registry, BACKWARD; `currency_pair` as string to avoid the enum trap | 002 |
| C11 | ~390 msg/s → **~2k rows per 5 s micro-batch** | Small-file risk on a single node | DuckLake data inlining; Spark writes via `foreachBatch` → DuckDB | 003 |
| C12 | ~150 GB/month, no retention | Laptop disk is finite | Trigger recorded: move to S3 or add retention | 004 |
| C13 | Move to AWS without code changes | Same storage API everywhere | S3 API everywhere: Garage → S3 is config only | 004 |
| C14 | Regulatory: data residency per region | Region-scoped storage, retention controls | **Gap.** Partly covered: object lock / retention → S3 trigger. Residency (per-region buckets) still to design | 004 (009 / Phase 6) |
| C15 | Infrastructure as code with OpenTofu | Modules + environments | Pending | (009) |

ADRs in brackets are not written yet.

**Open gaps to close in later ADRs:** C14 data residency; hot currency pairs (ADR-008).

**Interview opening (constraint first):** "One laptop, one operator, open source only, alerts under 5 s,
and reports that must include trades up to minutes late. Those constraints give me Kappa for alerts,
Lambda for analytics, and a single-node lakehouse, and I'll show you which constraint drove each component."

## 4. Architecture

Decided in [ADR-001](../adr/ADR-001-overall-architecture.md): **Kappa for alerts, Lambda for analytics.**

```mermaid
flowchart LR
  APP["trade-app<br>(simulator in the build)"] -->|event_topic = fx.trades| MQ["Redpanda<br>+ Schema Registry"]
  APP -.->|"production design:<br>DB + transactional outbox"| DB[("DB")]
  MQ --> DET["detector-worker<br>(Spark, windows · watermarks)"]
  DET -->|anomaly_topic = fx.alerts| MQ
  MQ --> NOT["notifier-worker"] --> AL["alert notification"]
  MQ -->|sink: dedupe on trade_id| RAW["raw_event + anomaly_event<br>(DuckLake)"]
  RAW --> BATCH["batch scheduled model<br>(exact, incl. late trades)"]
  RAW --> SPEED["speed view<br>(incremental micro-batch)"]
  BATCH --> SB["serving batch"] --> BI["BI reports / analysts"]
  SPEED --> SS["serving speed"] --> RT["freshness dashboards / real-time app"]
  MQ -.->|"replay = backtest"| DET
```

- **Alert path (Kappa):** `detector-worker` reads `event_topic`, writes `anomaly_topic`; `notifier-worker` sends alerts. Rule changes are backtested by replaying the log.
- **Analytics path (Lambda):** raw trades and fired alerts land as an immutable record; a scheduled batch model recomputes exact results (late trades included); a speed view gives fresh, approximate results.
- **Serving by use case:** reports read batch (exact, stale until the next run); freshness dashboards and the real-time app read speed (fresh, approximate).
- **Deciding axis:** timeliness (alerts) vs completeness (reports).
- **Production note:** trades originate in an OLTP service and reach the log via a transactional outbox (known improvement). The build is log-first, so it has no dual write.
- **Platform layer:** storage (Garage/S3) → open table format (DuckLake) → query engine (DuckDB).

## 5. Stack

| Layer | Choice | ADR |
|---|---|---|
| Broker + registry | Redpanda (built-in Schema Registry, Console) | ADR-002 |
| Encoding | Avro | ADR-002 |
| Stream processing | Spark Structured Streaming | ADR-005 |
| Table format + catalog | DuckLake (Postgres catalog) | ADR-003 |
| Object storage | Garage (laptop) → S3 (AWS) | ADR-004 |
| Batch + query | DuckDB | — |
| BI | Superset | — |
| IaC | OpenTofu | ADR-009 |

## 6. Phases

| Phase | Week | Doc | Theme | Key output |
|---|---|---|---|---|
| 1 | Wk 1 | [phase-1-ingestion-contract](phase-1-ingestion-contract.md) | Event contract + producer | Avro contracts, sizing, running producer |
| 2 | Wk 2 | [phase-2-lakehouse-storage](phase-2-lakehouse-storage.md) | Lakehouse layers | DuckLake on Garage, raw table |
| 3 | Wk 3 | [phase-3-stream-processing](phase-3-stream-processing.md) | Streaming semantics | Spark windows → alerts |
| 4 | Wk 4 | [phase-4-batch-serving-reconciliation](phase-4-batch-serving-reconciliation.md) | Lambda vs Kappa | Batch layer, serving views, precision/recall |
| 5 | Wk 5 | [phase-5-distribution-failure-iac](phase-5-distribution-failure-iac.md) | Distribution, failure, IaC | Failure walkthrough, OpenTofu |
| 6 | Wk 6–8 | [phase-6-synthesis-demo](phase-6-synthesis-demo.md) | Cost, security, MCP, synthesis | One-page architecture doc, demo |

## 7. Decision index

See [`../adr/README.md`](../adr/README.md).
