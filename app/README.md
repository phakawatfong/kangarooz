# f1-project (Python)

Prefer `make` from the repo root (see the root `README.md`). Run directly from this folder with the root `.env`:

```bash
uv sync                                                      # install deps
uv run pytest                                                # unit tests (no broker needed)
uv run --env-file ../.env python -m fx_producer.register_schemas
uv run --env-file ../.env python -m fx_producer.produce --rate 500 --duration 60
uv run --env-file ../.env python -m lakehouse.stream
F1_LOG_LEVEL=DEBUG uv run --env-file ../.env python -m lakehouse.stream   # with debug lines
```

| Package | What it is |
|---|---|
| `contract/` | Shared by every producer and consumer: topics, `KafkaConfig`, Avro schemas and wire format |
| `fx_producer/` | Synthetic trades -> Kafka (Phase 1) |
| `lakehouse/` | Kafka -> Spark -> DuckLake bronze/silver (Phase 2) |
| `logs.py` | `F1_LOG_LEVEL` switch for this project's debug output |

Config is read in one place per package, `KafkaConfig.from_env()` and `LakeConfig.from_env()`, one line per variable.
