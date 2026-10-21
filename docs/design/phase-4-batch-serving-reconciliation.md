# Phase 4 — Batch Layer, Serving & Reconciliation (Week 4)

**Status:** not started · **DDIA:** Ch 10, 12

## Goal
Build the Lambda analytics path and grade the detector against ground truth.

## Questions to answer
- Serving by use case (ADR-001): reports read batch, freshness dashboards read speed. How to label freshness, and how big the speed-vs-batch gap gets.
- Fired alerts vs batch labels vs ground truth — what each one means.
- Precision / recall using the Phase 1 matching rule; how much late data the stream missed.
- Backtesting a rule change by replaying history (Kappa property).

## Open questions (design review, 2026-10-17)
- Batch model and serving views read **silver** (deduplicated), never bronze.
- Reconciliation measures the speed-vs-batch gap, i.e. how much late data the stream missed. What threshold would trigger widening the watermark?

## Decisions (ADRs)
- ADR-007 Lambda drift + reconciliation design (+ batch scheduler: cron vs Dagster vs Airflow)

## Planned spike
DuckDB batch job recomputing exact aggregates; reconciliation query; replay-based backtest of one rule.

## Exit criteria
- [ ] ADR-007 written
- [ ] Precision/recall reported per anomaly type
