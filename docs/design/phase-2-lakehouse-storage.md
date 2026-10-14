# Phase 2 — Lakehouse Storage (Week 2)

**Status:** not started · **DDIA:** Ch 3

## Goal
Land raw trades durably in a lakehouse and be able to defend every storage layer.

## Questions to answer
- What does each layer do: object storage → file format → table format → catalog → engine?
- Why does streaming create the small-file problem, and how does DuckLake's inlining address it?
- Where is `etl_loaded_at` stamped, and how does the sink dedupe on `trade_id`?

## Decisions (ADRs)
- ADR-003 Table format: DuckLake vs Iceberg vs Delta
- ADR-004 Object storage: Garage vs SeaweedFS vs local FS (MinIO CE archived)

## Planned spike
Garage + Postgres + DuckLake in compose; write and read a table from DuckDB; test the Superset → DuckDB connection.

## Exit criteria
- [ ] ADR-003, ADR-004 written
- [ ] Raw trades table design (columns, partitioning, dedupe)
- [ ] Mock answered: "Why not Iceberg?" / "Why not just Postgres?"
