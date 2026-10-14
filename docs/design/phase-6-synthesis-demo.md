# Phase 6 — Cost, Security, MCP, Synthesis & Demo (Weeks 6–8)

**Status:** not started

## Goal
Turn the project into an interview artifact.

## Questions to answer
- Cost: laptop vs AWS demo; what would production cost and why.
- Security: IAM, secrets, least privilege, read-only roles.
- Observability: consumer lag, throughput, alert latency.
- MCP access model: read-only named tools, no free-form SQL.

## Decisions (ADRs)
- ADR-010 MCP access model

## Deliverables
- One-page architecture doc: diagram, ADR links, failure modes, what changes at 100×
- Superset dashboard (OHLC/VWAP, anomaly timeline, precision/recall)
- Optional: MCP server; AWS demo via OpenTofu (Fargate or Lightsail), then destroy
- 2-minute project pitch + full system-design mock round
