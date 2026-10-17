# F1 commands. Run from the repo root.
PY := cd f1_project && uv run --env-file ../.env
# Spark comes from the pinned pyspark wheel; a machine-wide SPARK_HOME would load another version's jars.
unexport SPARK_HOME

.PHONY: help setup up down reset topics register produce produce-dry verify stream lake-verify test console logs

help:            ## list commands
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-12s %s\n", $$1, $$2}'

setup:           ## check Java 17+, install Python deps, prefetch the Spark jar + DuckDB extensions
	@java="$${JAVA_HOME:+$$JAVA_HOME/bin/}java"; \
	if [ -n "$$JAVA_HOME" ] && [ ! -x "$$java" ]; then \
	  echo "JAVA_HOME=$$JAVA_HOME has no bin/java. Fix or unset it in your shell profile."; exit 1; fi; \
	v=$$("$$java" -version 2>&1 | awk -F'"' '/version/ {split($$2, a, "."); print a[1]}'); \
	if [ -z "$$v" ] || [ "$$v" -lt 17 ]; then \
	  echo "Spark needs Java 17+ (found: $${v:-none}). With SDKMAN: sdk env install (reads .sdkmanrc)."; exit 1; fi; \
	echo "java $$v ok ($$java)"
	cd f1_project && uv sync
	$(PY) python -m lakehouse.prefetch

up:              ## start Redpanda, Console, Postgres, Garage and create topics
	docker compose up -d
	docker compose logs init-topics

down:            ## stop the stack (keeps data)
	docker compose down

reset:           ## stop and DELETE all data (Redpanda, lake catalog + files, Spark checkpoint)
	docker compose down -v
	rm -rf f1_project/.checkpoints

register:        ## register Avro schemas + BACKWARD compatibility
	$(PY) python -m fx_producer.registry

produce:         ## run the producer (override: make produce ARGS="--rate 1000 --duration 120")
	$(PY) python -m fx_producer.cli $(ARGS)

produce-dry:     ## producer without Kafka: samples + Avro sizes
	$(PY) python -m fx_producer.cli --dry-run --duration 5 $(ARGS)

verify:          ## check size, header, partitions, duplicates, ground truth
	$(PY) python -m fx_producer.verify

stream:          ## Spark sink: fx.trades -> DuckLake bronze + silver (Ctrl+C to stop)
	$(PY) python -m lakehouse.stream

lake-verify:     ## check the lake against Kafka: counts, dedupe, late events, small files
	$(PY) python -m lakehouse.verify

test:            ## unit tests (no broker needed)
	$(PY) pytest -q

console:         ## open Redpanda Console
	open http://localhost:8080 || xdg-open http://localhost:8080

logs:            ## tail broker logs
	docker compose logs -f redpanda
