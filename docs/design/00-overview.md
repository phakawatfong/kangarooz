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
| C7 | No acknowledged trade lost; duplicates possible | At-least-once + idempotent sink | Producer acks=all + idempotence; batch ledger makes lake writes exactly-once; silver insert-if-absent on `trade_id`, same transaction as bronze | 001, 003 (006) |
| C8 | Replay for rule backtesting | Durable, replayable log | Redpanda retention = replay window; Kappa replay | 001, 002 |
| C9 | Money values | Exact decimals, never floats | Avro `decimal(18,8)` (Protobuf/JSON rejected) | 002 |
| C10 | Schema evolution, compact messages | Enforced schemas with compatibility rules | Avro + Schema Registry, BACKWARD; `currency_pair` as string to avoid the enum trap | 002 |
| C11 | Dev ~500/s → **~2.5k rows per 5 s micro-batch**; production 1–5k/s → 5–25k rows | Small-file risk on a single node, at both rates | DuckLake data inlining at the dev rate; scheduled compaction (`make compact`) at the production rate; Spark writes via `foreachBatch` → DuckDB | 003 |
| C12 | ~150 GB/month, no retention | Laptop disk is finite | Trigger recorded: move to S3 or add retention | 004 |
| C13 | Move to AWS without code changes | Same storage API everywhere | S3 API everywhere: Garage → S3 is config only | 004 |
| C14 | Regulatory: data residency per region | Region-scoped storage, retention controls | **Gap.** Partly covered: object lock / retention → S3 trigger. Residency (per-region buckets) still to design | 004 (009 / Phase 6) |
| C15 | Infrastructure as code with OpenTofu | Modules + environments | Pending | (009) |

ADRs in brackets are not written yet.

**Open gaps to close in later ADRs:** C14 data residency; hot currency pairs (ADR-008).

**One-line summary (constraint first):** "One laptop, one operator, open source only, alerts under 5 s,
and reports that must include trades up to minutes late. Those constraints give me Kappa for alerts,
Lambda for analytics, and a single-node lakehouse, and I'll show you which constraint drove each component."

## 4. Architecture

Decided in [ADR-001](../adr/ADR-001-overall-architecture.md): **Kappa for alerts, Lambda for analytics.**

```mermaid
flowchart LR
  APP["fx_producer<br>(simulator + labelled injected anomalies)"] -->|"fx.trades (key = trade_id)"| MQ["Redpanda + Schema Registry<br>Avro · BACKWARD"]
  APP -->|"fx.trades.ground_truth"| MQ
  APP -.->|"production only, not in this build:<br>trade service DB + transactional outbox"| DB[("OLTP DB")]

  subgraph KAPPA["Alert path (Kappa)"]
    DET["detector (Spark, own process)<br>fat_finger: stateless, append<br>account_burst: 10 s tumbling, 30 s watermark, update"]
    NOT["notifier<br>dedupe on alert_id · 60 s cooldown"]
  end
  subgraph EVAL["Rule evaluation (offline, not runtime)"]
    GRADE["grader<br>precision · recall · p95 latency"]
  end
  MQ -->|fx.trades| DET
  DET -->|"fx.alerts (key = alert_id)"| MQ
  MQ -.->|"replay = backtest (new rule_version)"| DET
  MQ -.->|fx.alerts| NOT --> AL["alert notification"]
  MQ -->|"fx.alerts + ground truth + fx.trades"| GRADE

  subgraph LAMBDA["Analytics path (Lambda)"]
    SINK["lake sink (Spark foreachBatch → DuckDB)<br>dedupe on trade_id"]
    LAKE[("DuckLake: Postgres catalog + Garage files<br>bronze · silver · ingest_batch<br>+ scheduled compaction")]
    ANOM[("anomaly_event")]
    BATCH["batch scheduled model<br>(exact, incl. late trades)"]
    SPEED["speed view<br>(incremental micro-batch)"]
  end
  MQ -->|fx.trades| SINK --> LAKE
  MQ -.->|fx.alerts| ANOM
  LAKE -.-> BATCH
  LAKE -.-> SPEED
  ANOM -.-> BATCH
  BATCH -.-> SB["serving batch"] -.-> BI["BI reports / analysts"]
  SPEED -.-> SS["serving speed"] -.-> RT["freshness dashboards / real-time app"]

  classDef planned stroke-dasharray: 5 5
  class DB,NOT,AL,ANOM,BATCH,SPEED,SB,BI,SS,RT planned
```

Solid boxes and arrows are built (Phases 1–3). Dashed ones are designed but not built yet.

- **Alert path (Kappa):** the `detector` reads `fx.trades` and writes `fx.alerts`, keyed by a deterministic `alert_id`. The `notifier` is designed but not built; it would dedupe on `alert_id` and apply a cooldown. Rule changes are backtested by replaying the log with a new `rule_version`.
- **Rule evaluation (offline):** the `grader` scores the rules' alerts against the simulator's ground-truth labels (precision, recall, p95 latency). It is a measuring tool, not part of the alert path.
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
