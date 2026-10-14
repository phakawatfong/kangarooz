# Phase 3 — Stream Processing & Alerts (Week 3)

**Status:** not started · **DDIA:** Ch 11

## Goal
Turn the trade stream into real-time alerts (Kappa path) within p95 < 5 s.

## Questions to answer
- Event time vs processing time; tumbling vs sliding windows; watermarks; state.
- Window sizes and thresholds for each rule (price z-score, VWAP deviation, volume burst, account burst).
- What happens to a trade that is 2 minutes late?
- Alert event schema: rule, observed value, baseline, threshold, window, scope_key.

## Decisions (ADRs)
- ADR-005 Stream engine: Spark Structured Streaming vs Flink vs Quix Streams
- ADR-006 Delivery guarantee: at-least-once + idempotent sink

## Planned spike
Spark reads `fx.trades` → sliding-window z-score per pair → `fx.alerts`; `foreachBatch` → DuckLake; measure p95 latency.

## Exit criteria
- [ ] ADR-005, ADR-006 written
- [ ] Alert schema defined
- [ ] Latency measured against the 5 s target
