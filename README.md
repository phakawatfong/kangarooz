# F1 — Real-time FX Trading Analytics Platform

A self-built, design-first data platform: synthetic FX trades → Redpanda → Spark → anomaly alerts
(Kappa) + DuckLake analytics (Lambda) → Superset, with OpenTofu for infrastructure.
The point is **architecture judgment**: every component is chosen deliberately and defended in an ADR.

- Design docs by phase: [`docs/design/`](docs/design/00-overview.md)
- Decisions: [`docs/adr/`](docs/adr/README.md)

## Status

| Phase | Week | Scope | Status |
|---|---|---|---|
| 1 | Wk 1 | Ingestion contract + synthetic producer | ✅ built · ADR-001 + ADR-002 accepted |
| 2 | Wk 2 | Lakehouse storage (DuckLake, Garage, Postgres) | 🟡 ADR-003 + ADR-004 accepted · spike next |
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

## Phase 2 quickstart (lakehouse spike)

Also needs Java 17+ for Spark. With SDKMAN, `sdk env install` installs the version pinned in `.sdkmanrc`.

```bash
make setup        # checks Java, uv sync, prefetches the Spark Kafka jar + DuckDB extensions
make up           # now also starts Postgres (DuckLake catalog) + Garage (S3, bucket f1-lake)
make register
make stream       # Spark: fx.trades -> DuckLake bronze + silver, one transaction per 5 s batch
make produce      # in a second terminal
make lake-verify  # bronze = Kafka offsets, silver deduped, late trades landed, Parquet file count
make lake-ui      # DuckDB UI on the lake, http://localhost:4213
```

To watch it live, use three pages. Redpanda Console (`make console`, :8080) shows messages arriving in `fx.trades`. Spark UI (http://localhost:4040, Structured Streaming tab) shows input rate and batch duration while `make stream` runs. `make lake-ui` shows the lake tables.

`make reset` deletes the lake together with the Spark checkpoint. They only make sense as a pair.

`.env` is not committed. Phase 2 adds these keys (generate the secrets with `openssl rand -hex 32`, and the key id as `GK` + `openssl rand -hex 12`):

```bash
POSTGRES_VERSION=17.9-alpine
GARAGE_VERSION=v2.4.1
F1_PG_PASSWORD=<secret>
F1_GARAGE_RPC_SECRET=<64 hex>
F1_S3_KEY_ID=GK<24 hex>
F1_S3_SECRET=<64 hex>
F1_S3_ENDPOINT=localhost:3900       # remove for AWS S3
F1_S3_REGION=garage
F1_S3_USE_SSL=false
F1_S3_URL_STYLE=path
F1_LAKE_DATA_PATH=s3://f1-lake/
F1_LAKE_INLINING_ROW_LIMIT=5000
F1_LAKE_CATALOG="postgres:host=localhost port=15432 dbname=ducklake user=f1 password=${F1_PG_PASSWORD}"
```

## Layout

```
docker-compose.yml        Redpanda (Kafka API + Schema Registry) + Console + topic init, Postgres, Garage
garage.toml               Garage single-node config (secrets come from .env)
schemas/                  Avro contracts (source of truth): fx_trade, anomaly_episode
app/                      Python (uv) — fx_producer (Phase 1) and lakehouse (Phase 2) packages + tests
docs/design/              Design docs, one per phase
docs/adr/                 Architecture Decision Records
```
