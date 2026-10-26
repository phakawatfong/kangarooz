"""Spark Structured Streaming sink: fx.trades -> foreachBatch -> DuckLake bronze + silver.

    uv run python -m lakehouse.stream

Spark owns the Kafka offsets (checkpoint); the DuckLake ledger makes a replayed batch a no-op.
"""

from __future__ import annotations

import logging
import os
import time
from pathlib import Path

import duckdb
import pyspark
from pyspark.sql import DataFrame, SparkSession

from contract.avro_codec import TRADE_SCHEMA
from contract.kafka import TRADES_TOPIC, KafkaConfig, registered_schema_id
from logs import setup_logging

from .config import LakeConfig
from .lake import connect, create_tables, decode_batch, write_batch

CHECKPOINT = Path(__file__).resolve().parents[1] / ".checkpoints" / "fx_trades"
SPARK_PACKAGES = ",".join(f"org.apache.spark:{name}_2.13:{pyspark.__version__}"
                          for name in ("spark-sql-kafka-0-10", "spark-avro"))
FLUSH_EVERY = 60  # batches (~5 min at the 5 s trigger): inlined rows move from Postgres to Parquet

log = logging.getLogger(__name__)


def spark_session(app_name: str) -> SparkSession:
    os.environ.pop("SPARK_HOME", None)  # use the pinned pyspark's own Spark, not a machine-wide install's jars
    return (SparkSession.builder.master("local[*]").appName(app_name)
            .config("spark.jars.packages", SPARK_PACKAGES)
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.driver.memory", "2g")
            .config("spark.ui.showConsoleProgress", "false")  # the progress bar overwrites the batch log lines
            .getOrCreate())


def check_checkpoint_matches_lake(db: duckdb.DuckDBPyConnection) -> None:
    """Batch ids only mean something next to the checkpoint that produced them."""
    landed = db.execute("SELECT count(*) FROM lake.ingest_batch").fetchone()[0]
    commits = CHECKPOINT / "commits"
    spark_committed = commits.exists() and any(commits.iterdir())
    log.debug("checkpoint %s: exists=%s spark_committed=%s; lake ledger has %d batches",
              CHECKPOINT, CHECKPOINT.exists(), spark_committed, landed)
    if landed and not CHECKPOINT.exists():
        raise SystemExit(f"lake holds {landed} batches but {CHECKPOINT} is gone: new batch ids would be "
                         "skipped as replays. Run `make reset` to start both from scratch.")
    if spark_committed and not landed:
        raise SystemExit(f"{CHECKPOINT} has committed batches but the lake is empty: delete the checkpoint.")


def main() -> None:
    setup_logging()
    kafka_config = KafkaConfig.from_env()
    log.debug("kafka: %s", kafka_config)
    schema_id = registered_schema_id(kafka_config, TRADES_TOPIC, TRADE_SCHEMA)
    db = connect(LakeConfig.from_env())
    create_tables(db)
    check_checkpoint_matches_lake(db)

    def land_batch(batch: DataFrame, batch_id: int) -> None:
        started = time.monotonic()
        kafka_rows = batch.selectExpr("value", "partition AS kafka_partition", "offset AS kafka_offset",
                                      "timestamp AS kafka_timestamp").toArrow()
        fetched = time.monotonic()
        trades = decode_batch(kafka_rows, schema_id)
        decoded = time.monotonic()
        landed = write_batch(db, batch_id, trades)
        if landed and batch_id % FLUSH_EVERY == 0:
            log.debug("batch %d: flushing inlined rows to Parquet", batch_id)
            db.execute("CALL ducklake_flush_inlined_data('lake')")
        finished = time.monotonic()
        log.debug("batch %d timings: spark->arrow %.0f ms, decode %.0f ms, lake write %.0f ms", batch_id,
                  (fetched - started) * 1000, (decoded - fetched) * 1000, (finished - decoded) * 1000)
        status = "landed" if landed else "replay, skipped"
        print(f"batch {batch_id}: {kafka_rows.num_rows} rows {status} in {(finished - started) * 1000:.0f} ms",
              flush=True)

    print(f"streaming {TRADES_TOPIC} (schema id {schema_id}) -> DuckLake; Ctrl+C to stop", flush=True)
    query = (spark_session("f1-lake-sink").readStream.format("kafka")
             .option("kafka.bootstrap.servers", kafka_config.bootstrap_servers)
             .option("subscribe", TRADES_TOPIC)
             .option("startingOffsets", "earliest")
             .option("maxOffsetsPerTrigger", 50_000)
             .option("failOnDataLoss", "true")
             .load()
             .writeStream.foreachBatch(land_batch)
             .trigger(processingTime="5 seconds")
             .option("checkpointLocation", str(CHECKPOINT))
             .start())
    query.awaitTermination()


if __name__ == "__main__":
    main()
