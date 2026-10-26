"""Spark detector (D1: its own process): fx.trades -> fat_finger + account_burst -> fx.alerts.

    uv run python -m detector.job [--rule-version 1] [--slide "2 seconds"] [--trigger "1 second"]

Each rule is its own query with its own checkpoint under .checkpoints/detector/. Delete a rule's checkpoint to
replay it from the earliest offset (Q3); a new --rule-version or --slide starts a fresh checkpoint by itself.
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery

from contract.avro_codec import ALERT_SCHEMA, TRADE_SCHEMA
from contract.kafka import ALERTS_TOPIC, TRADES_TOPIC, KafkaConfig, registered_schema_id
from lakehouse.stream import spark_session
from logs import setup_logging

from .rules import account_burst, decode_trades, fat_finger, to_kafka

CHECKPOINTS = Path(__file__).resolve().parents[1] / ".checkpoints" / "detector"
PROGRESS_EVERY_S = 10


def _progress(query: StreamingQuery) -> str:
    """One line from the last micro-batch: size, duration, and state (Q2) for the window rule."""
    progress = query.lastProgress
    if not progress:
        return f"{query.name}: no batch yet"
    line = (f"{query.name}: batch {progress['batchId']} rows={progress['numInputRows']} "
            f"duration={progress['durationMs'].get('triggerExecution', 0)} ms")
    for state in progress.get("stateOperators", []):
        line += (f" state_rows={state['numRowsTotal']} state_bytes={state['memoryUsedBytes']}"
                 f" dropped_by_watermark={state['numRowsDroppedByWatermark']}")
    return line


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rule-version", type=int, default=1)
    parser.add_argument("--slide", help='account_burst slide, e.g. "2 seconds"; omit for tumbling')
    parser.add_argument("--trigger", help='processing-time trigger, e.g. "1 second"; omit for as-fast-as-possible (D5)')
    args = parser.parse_args()
    setup_logging()
    kafka_config = KafkaConfig.from_env()
    trade_schema_id = registered_schema_id(kafka_config, TRADES_TOPIC, TRADE_SCHEMA)
    alert_schema_id = registered_schema_id(kafka_config, ALERTS_TOPIC, ALERT_SCHEMA)

    spark = spark_session("f1-detector")
    # The default 200 shuffle partitions = 200 state-store tasks per micro-batch, which costs latency for no
    # parallelism on a laptop. Fixed by the first run: Spark stores it in the checkpoint.
    spark.conf.set("spark.sql.shuffle.partitions", 6)
    trades = decode_trades(spark.readStream.format("kafka")
                           .option("kafka.bootstrap.servers", kafka_config.bootstrap_servers)
                           .option("subscribe", TRADES_TOPIC)
                           .option("startingOffsets", "earliest")
                           .option("failOnDataLoss", "true")
                           .load(), trade_schema_id)

    suffix = f"v{args.rule_version}" + (f"-slide-{args.slide.replace(' ', '')}" if args.slide else "")

    def start(rule: str, alerts: DataFrame, output_mode: str) -> StreamingQuery:
        writer = (to_kafka(alerts, alert_schema_id).writeStream.format("kafka")
                  .queryName(f"{rule}-{suffix}")
                  .option("kafka.bootstrap.servers", kafka_config.bootstrap_servers)
                  .option("topic", ALERTS_TOPIC)
                  .option("checkpointLocation", str(CHECKPOINTS / f"{rule}-{suffix}"))
                  .outputMode(output_mode))
        if args.trigger:
            writer = writer.trigger(processingTime=args.trigger)
        return writer.start()

    queries = [
        start("fat_finger", fat_finger(trades, args.rule_version), "append"),
        # D3: update mode emits at the first trigger past the threshold; append would wait for the watermark (>= 40 s).
        start("account_burst", account_burst(trades, args.rule_version, args.slide), "update"),
    ]
    print(f"detecting on {TRADES_TOPIC} -> {ALERTS_TOPIC} ({suffix}, trigger={args.trigger or 'asap'}); Ctrl+C to stop",
          flush=True)
    while all(query.isActive for query in queries):
        time.sleep(PROGRESS_EVERY_S)
        for query in queries:
            print(_progress(query), flush=True)
    for query in queries:
        if query.exception():
            raise SystemExit(f"{query.name} failed: {query.exception()}")


if __name__ == "__main__":
    main()
