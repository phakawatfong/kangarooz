"""Spark Structured Streaming sink: fx.trades -> foreachBatch -> DuckLake bronze + silver.

    uv run python -m lakehouse.stream

Spark owns the Kafka offsets (checkpoint); the DuckLake ledger makes a replayed batch a no-op.
"""

from __future__ import annotations

import time
from pathlib import Path

import pyspark
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from pyspark.sql import SparkSession

from fx_producer.adapters.avro_local import schema_text
from fx_producer.config import TRADE_SCHEMA, TRADES_TOPIC, Connection

from .lake import LakeConfig, connect, create_tables, decode_batch, write_batch

CHECKPOINT = Path(__file__).resolve().parents[1] / ".checkpoints" / "fx_trades"
KAFKA_PACKAGE = f"org.apache.spark:spark-sql-kafka-0-10_2.13:{pyspark.__version__}"
FLUSH_EVERY = 60  # batches (~5 min at the 5 s trigger): inlined rows move from Postgres to Parquet


def spark_session() -> SparkSession:
    return (SparkSession.builder.master("local[*]").appName("f1-lake-sink")
            .config("spark.jars.packages", KAFKA_PACKAGE)
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.driver.memory", "2g")
            .config("spark.ui.showConsoleProgress", "false")  # the progress bar overwrites the batch log lines
            .getOrCreate())


def check_checkpoint_matches_lake(con) -> None:
    """Batch ids only mean something next to the checkpoint that produced them."""
    landed = con.execute("SELECT count(*) FROM lake.ingest_batch").fetchone()[0]
    commits = CHECKPOINT / "commits"
    spark_committed = commits.exists() and any(commits.iterdir())
    if landed and not CHECKPOINT.exists():
        raise SystemExit(f"lake holds {landed} batches but {CHECKPOINT} is gone: new batch ids would be "
                         "skipped as replays. Run `make reset` to start both from scratch.")
    if spark_committed and not landed:
        raise SystemExit(f"{CHECKPOINT} has committed batches but the lake is empty: delete the checkpoint.")


def main() -> None:
    conn = Connection.from_env()
    sr = SchemaRegistryClient({"url": conn.schema_registry_url})
    schema_id = sr.lookup_schema(f"{TRADES_TOPIC}-value", Schema(schema_text(TRADE_SCHEMA), "AVRO"),
                                 normalize_schemas=True).schema_id
    con = connect(LakeConfig.from_env())
    create_tables(con)
    check_checkpoint_matches_lake(con)

    def handle(df, batch_id: int) -> None:
        t0 = time.monotonic()
        kafka = df.selectExpr("value", "partition AS kafka_partition", "offset AS kafka_offset",
                              "timestamp AS kafka_timestamp").toArrow()
        landed = write_batch(con, batch_id, decode_batch(kafka, schema_id))
        if landed and batch_id % FLUSH_EVERY == 0:
            con.execute("CALL ducklake_flush_inlined_data('lake')")
        status = "landed" if landed else "replay, skipped"
        print(f"batch {batch_id}: {kafka.num_rows} rows {status} in {(time.monotonic() - t0) * 1000:.0f} ms",
              flush=True)

    print(f"streaming {TRADES_TOPIC} (schema id {schema_id}) -> DuckLake; Ctrl+C to stop", flush=True)
    query = (spark_session().readStream.format("kafka")
             .option("kafka.bootstrap.servers", conn.bootstrap_servers)
             .option("subscribe", TRADES_TOPIC)
             .option("startingOffsets", "earliest")
             .option("maxOffsetsPerTrigger", 50_000)
             .option("failOnDataLoss", "true")
             .load()
             .writeStream.foreachBatch(handle)
             .trigger(processingTime="5 seconds")
             .option("checkpointLocation", str(CHECKPOINT))
             .start())
    query.awaitTermination()


if __name__ == "__main__":
    main()
