# Phase 5 — Distribution, Failure Modes & IaC (Week 5)

**Status:** not started · **DDIA:** Ch 5–6, skim Ch 9

## Goal
Show the design survives scale and failure, and manage the platform as code.

## Questions to answer
- Partitioning and keys at 100×: hot pairs, whales, consumer scaling, adding partitions.
- Failure walkthrough: Spark dies mid-batch, broker down, Postgres catalog down, duplicate flood, schema change.
- OpenTofu layout: shared modules, `envs/local` vs `envs/aws`, remote state, secrets.

## Decisions (ADRs)
- ADR-008 Partitioning, keys, hot currency pairs
- ADR-009 IaC scope + environments

## Planned spike
OpenTofu `envs/local` (Redpanda topics replacing `init-topics`, Postgres roles) + GitHub repo settings; `envs/aws` S3 + IAM + Budgets.

## Exit criteria
- [ ] ADR-008, ADR-009 written
- [ ] Failure-mode table (what breaks, what's lost, recovery, alerting)
