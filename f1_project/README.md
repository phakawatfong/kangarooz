# f1-project (Python)

Phase 1 synthetic FX trade producer. See the repo root `README.md` and `docs/design/`.

```bash
uv sync                                   # install deps
uv run pytest                             # unit tests (no broker needed)
uv run python -m fx_producer.registry     # register Avro schemas (BACKWARD compatibility)
uv run python -m fx_producer.cli --rate 500 --duration 60
uv run python -m fx_producer.verify       # check size, header, partition spread
```
