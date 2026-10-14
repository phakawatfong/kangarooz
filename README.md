# F1 — Real-time FX Trading Analytics Platform

A self-built, design-first data platform: synthetic FX trades → Redpanda → Spark → anomaly alerts
(Kappa) + DuckLake analytics (Lambda) → Superset, with OpenTofu for infrastructure.
The point is **architecture judgment**: every component is chosen deliberately and defended in an ADR.

- Design docs by phase: [`docs/design/`](docs/design/00-overview.md)
- Decisions: [`docs/adr/`](docs/adr/README.md)

## Status

| Phase | Week | Scope | Status |
|---|---|---|---|
| 1 | Wk 1 | Ingestion contract + synthetic producer | ✅ built (ADR-001/002 to write) |
| 2 | Wk 2 | Lakehouse storage (DuckLake, Garage, Postgres) | ⬜ |
| 3 | Wk 3 | Stream processing + alerts (Spark, Kappa) | ⬜ |
| 4 | Wk 4 | Batch layer, serving, reconciliation (Lambda) | ⬜ |
| 5 | Wk 5 | Partitioning, failure modes, OpenTofu | ⬜ |
| 6 | Wk 6–8 | Cost/security, MCP, synthesis, demo | ⬜ |

## Phase 1 quickstart

Needs Docker and [uv](https://docs.astral.sh/uv/).

```bash
make up          # Redpanda + Console, creates fx.trades (6 partitions) + fx.trades.ground_truth
make register    # register Avro schemas (subjects fx.trades-value, fx.trades.ground_truth-value)
make produce     # ~500 trades/s for 60s with injected anomalies + late/duplicate events
make verify      # real message size, 5-byte header, partition spread, duplicates, episodes
make console     # http://localhost:8080 — browse decoded Avro messages
make test        # unit tests, no broker needed
```

Tune the producer: `make produce ARGS="--rate 1000 --duration 120 --late-pct 0.05"` (see `--help`).

## Layout

```
docker-compose.yml        Redpanda (Kafka API + Schema Registry) + Console + topic init
schemas/                  Avro contracts (source of truth): fx_trade, anomaly_episode
f1_project/               Python (uv) — fx_producer package + tests
docs/design/              Design docs, one per phase
docs/adr/                 Architecture Decision Records
```
