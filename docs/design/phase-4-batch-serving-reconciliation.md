# Phase 4 — Batch Layer, Serving & Reconciliation (Week 4)

**Status:** not started · **DDIA:** Ch 10, 12

## Goal
Build the Lambda analytics path and grade the detector against ground truth.

## Questions to answer
- Serving views: how batch results (up to last run) and speed results (since) are combined.
- Fired alerts vs batch labels vs ground truth — what each one means.
- Precision / recall using the Phase 1 matching rule; how much late data the stream missed.
- Backtesting a rule change by replaying history (Kappa property).

## Decisions (ADRs)
- ADR-007 Lambda drift + reconciliation design (+ batch scheduler: cron vs Dagster vs Airflow)

## Planned spike
DuckDB batch job recomputing exact aggregates; reconciliation query; replay-based backtest of one rule.

## Exit criteria
- [ ] ADR-007 written
- [ ] Precision/recall reported per anomaly type
- [ ] Mock answered: "Why run both Lambda and Kappa?"
