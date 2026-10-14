# F1 — Phase 1 commands. Run from the repo root.
PY := cd f1_project && uv run

.PHONY: help up down reset topics register produce produce-dry verify test console logs

help:            ## list commands
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-12s %s\n", $$1, $$2}'

up:              ## start Redpanda + Console and create topics
	docker compose up -d
	docker compose logs init-topics

down:            ## stop the stack (keeps data)
	docker compose down

reset:           ## stop and DELETE all Redpanda data
	docker compose down -v

register:        ## register Avro schemas + BACKWARD compatibility
	$(PY) python -m fx_producer.registry

produce:         ## run the producer (override: make produce ARGS="--rate 1000 --duration 120")
	$(PY) python -m fx_producer.cli $(ARGS)

produce-dry:     ## producer without Kafka: samples + Avro sizes
	$(PY) python -m fx_producer.cli --dry-run --duration 5 $(ARGS)

verify:          ## check size, header, partitions, duplicates, ground truth
	$(PY) python -m fx_producer.verify

test:            ## unit tests (no broker needed)
	$(PY) pytest -q

console:         ## open Redpanda Console
	open http://localhost:8080 || xdg-open http://localhost:8080

logs:            ## tail broker logs
	docker compose logs -f redpanda
