"""Check the Phase 2 spike's acceptance criteria against Kafka and the lake (run once the stream caught up).

    uv run python -m lakehouse.verify

A separate DuckDB process from the stream, so it also proves a second reader works.
"""

from __future__ import annotations

import os

from confluent_kafka import Consumer, TopicPartition

from contract.kafka import TRADES_TOPIC, KafkaConfig
from logs import setup_logging

from .config import LakeConfig
from .lake import connect

failures: list[str] = []


def check(ok: bool, name: str, detail: str) -> None:
    print(f"[{'PASS' if ok else 'FAIL'}] {name}: {detail}")
    if not ok:
        failures.append(name)


def kafka_watermarks(kafka_config: KafkaConfig) -> dict[int, tuple[int, int]]:
    """(low, high) offsets per partition: high - low = messages in Kafka."""
    consumer = Consumer({"bootstrap.servers": kafka_config.bootstrap_servers, "group.id": "f1-lake-verify"})
    try:
        partitions = consumer.list_topics(TRADES_TOPIC, timeout=10).topics[TRADES_TOPIC].partitions
        return {partition: consumer.get_watermark_offsets(TopicPartition(TRADES_TOPIC, partition), timeout=10)
                for partition in sorted(partitions)}
    finally:
        consumer.close()


def main() -> None:
    setup_logging()
    lake_config = LakeConfig.from_env()
    db = connect(lake_config)
    print(f"reader pid={os.getpid()}  {lake_config.describe()}")

    def fetch_one(sql: str) -> tuple:
        return db.execute(sql).fetchone()

    bronze = {partition: (rows, distinct, first, last) for partition, rows, distinct, first, last in db.execute(
        "SELECT kafka_partition, count(*), count(DISTINCT kafka_offset), min(kafka_offset), max(kafka_offset) "
        "FROM lake.bronze_fx_trade GROUP BY 1").fetchall()}
    for partition, (low, high) in kafka_watermarks(KafkaConfig.from_env()).items():
        rows, distinct, first, last = bronze.get(partition, (0, 0, None, None))
        ok = rows == distinct == high - low and (rows == 0 or (first, last) == (low, high - 1))
        check(ok, f"bronze = kafka p{partition}",
              f"rows={rows} distinct_offsets={distinct} kafka={high - low} [{low},{high})")

    (bronze_rows, bronze_ids), (silver_rows, silver_ids) = (
        fetch_one(f"SELECT count(*), count(DISTINCT trade_id) FROM lake.{table}")
        for table in ("bronze_fx_trade", "silver_fx_trade"))
    check(silver_rows == silver_ids == bronze_ids, "silver deduped",
          f"silver rows={silver_rows} distinct={silver_ids} bronze distinct={bronze_ids} "
          f"(duplicates removed: {bronze_rows - silver_rows})")

    late, very_late, missing = fetch_one("""
        SELECT count(*) FILTER (kafka_timestamp - event_timestamp > INTERVAL 5 SECOND),
               count(*) FILTER (kafka_timestamp - event_timestamp > INTERVAL 30 SECOND),
               count(*) FILTER (kafka_timestamp - event_timestamp > INTERVAL 5 SECOND
                                AND trade_id NOT IN (SELECT trade_id FROM lake.silver_fx_trade))
        FROM lake.bronze_fx_trade""")
    check(late > 0 and missing == 0, "late events land",
          f"late>5s={late} ({late / max(bronze_rows, 1):.1%}) beyond 30s watermark={very_late} missing from silver={missing}")

    files = fetch_one(f"SELECT count(*) FROM glob('{lake_config.data_path}**/*.parquet')")[0]
    batches, avg_rows, max_rows = fetch_one("SELECT count(*), avg(bronze_rows), max(bronze_rows) FROM lake.ingest_batch")
    print(f"[INFO] small files: parquet={files} batches={batches} rows/batch avg={avg_rows or 0:.0f} "
          f"max={max_rows} inlining_row_limit={lake_config.inlining_row_limit}")

    if failures:
        raise SystemExit(f"{len(failures)} check(s) failed: {', '.join(failures)}")


if __name__ == "__main__":
    main()
