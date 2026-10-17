# Phase 3 — Stream Processing & Alerts (Week 3)

**Status:** not started · **DDIA:** Ch 11

## Goal
Turn the trade stream into real-time alerts (Kappa path) within p95 < 5 s.

## Questions to answer
- Event time vs processing time; tumbling vs sliding windows; watermarks; state.
- Window sizes and thresholds for each rule (price z-score, VWAP deviation, volume burst, account burst).
- What happens to a trade that is 2 minutes late?
- Alert event schema: rule, observed value, baseline, threshold, window, scope_key.

## Open questions from mock round 1 (2026-10-17)
- **Alert identity:** deterministic `alert_id = hash(rule, scope_key, window_start)`. `trade_id` only identifies single-trade alerts; window alerts (volume/account burst) need the rule + scope + window.
- **Where duplicates stop:** the notifier keeps a store of sent `alert_id`s and skips repeats (idempotent consumer). Redpanda transactions give exactly-once *inside* the broker, but a page/email is an external side effect. Exactly-once ends at the system boundary.
- **Late trades in the alert path:** trades past the 30 s watermark are excluded from window statistics (window not reopened). Should single-trade rules (fat-finger, price spike) still evaluate late trades and emit an alert flagged `late`?
- **Crash before offset commit:** the batch is reprocessed (at-least-once). Verify that the deterministic `alert_id` + notifier dedupe makes this harmless.

## Decisions (ADRs)
- ADR-005 Stream engine: Spark Structured Streaming vs Flink vs Quix Streams
- ADR-006 Delivery guarantee: at-least-once + idempotent sink

## Planned spike
Spark reads `fx.trades` → sliding-window z-score per pair → `fx.alerts`; `foreachBatch` → DuckLake; measure p95 latency.

## Exit criteria
- [ ] ADR-005, ADR-006 written
- [ ] Alert schema defined
- [ ] Latency measured against the 5 s target
