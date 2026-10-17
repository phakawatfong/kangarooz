# Phase 2 — Lakehouse Storage (Week 2)

**Status:** not started · **DDIA:** Ch 3

## Goal
Land raw trades durably in a lakehouse and be able to defend every storage layer.

## Questions to answer
- What does each layer do: object storage → file format → table format → catalog → engine?
- Why does streaming create the small-file problem, and how does DuckLake's inlining address it?
- Where is `etl_loaded_at` stamped, and how does the sink dedupe on `trade_id`?

## Open questions from mock round 1 (2026-10-17)
- **Lakehouse layers:** bronze (raw, as landed, may contain duplicates; mirrors the topic for reprocessing) → silver (deduplicated at write time via MERGE on `trade_id`) → gold (aggregates). Which layers do analysts and BI query? (Target: silver/gold only; restrict bronze.)
- **Dedupe at write time, not query time:** so no report double-counts volume because someone forgot a `DISTINCT`.
- **Alerts table (`anomaly_event`):** merged on `alert_id` (see Phase 3) so reprocessing doesn't duplicate fired alerts.

## Decisions (ADRs)
- ADR-003 Table format: DuckLake vs Iceberg vs Delta
- ADR-004 Object storage: Garage vs SeaweedFS vs local FS (MinIO CE archived)

## Planned spike
Garage + Postgres + DuckLake in compose; write and read a table from DuckDB; test the Superset → DuckDB connection.

## Exit criteria
- [ ] ADR-003, ADR-004 written
- [ ] Raw trades table design (columns, partitioning, dedupe)
- [ ] Mock answered: "Why not Iceberg?" / "Why not just Postgres?"
