"""Check the Phase 2 spike's acceptance criteria against Kafka and the lake (run once the stream caught up).

    uv run python -m lakehouse.verify

A separate DuckDB process from the stream, so it also proves a second reader works.
"""

from __future__ import annotations

import os

from confluent_kafka import Consumer, TopicPartition

from fx_producer.config import TRADES_TOPIC, Connection

from .lake import LakeConfig, connect

failures: list[str] = []


def check(ok: bool, name: str, detail: str) -> None:
    print(f"[{'PASS' if ok else 'FAIL'}] {name}: {detail}")
    if not ok:
        failures.append(name)


def kafka_watermarks(conn: Connection) -> dict[int, tuple[int, int]]:
    c = Consumer({"bootstrap.servers": conn.bootstrap_servers, "group.id": "f1-lake-verify"})
    try:
        parts = c.list_topics(TRADES_TOPIC, timeout=10).topics[TRADES_TOPIC].partitions
        return {p: c.get_watermark_offsets(TopicPartition(TRADES_TOPIC, p), timeout=10) for p in sorted(parts)}
    finally:
        c.close()


def main() -> None:
    cfg = LakeConfig.from_env()
    con = connect(cfg)
    print(f"reader pid={os.getpid()}  data={cfg.data_path}  endpoint={cfg.s3_endpoint}  key_id={cfg.s3_key_id}")

    def q(sql: str) -> tuple:
        return con.execute(sql).fetchone()

    bronze = {p: (n, d, lo, hi) for p, n, d, lo, hi in con.execute(
        "SELECT kafka_partition, count(*), count(DISTINCT kafka_offset), min(kafka_offset), max(kafka_offset) "
        "FROM lake.bronze_fx_trade GROUP BY 1").fetchall()}
    for p, (low, high) in kafka_watermarks(Connection.from_env()).items():
        n, distinct, first, last = bronze.get(p, (0, 0, None, None))
        ok = n == distinct == high - low and (n == 0 or (first, last) == (low, high - 1))
        check(ok, f"bronze = kafka p{p}", f"rows={n} distinct_offsets={distinct} kafka={high - low} [{low},{high})")

    (b_rows, b_ids), (s_rows, s_ids) = (q(f"SELECT count(*), count(DISTINCT trade_id) FROM lake.{t}")
                                        for t in ("bronze_fx_trade", "silver_fx_trade"))
    check(s_rows == s_ids == b_ids, "silver deduped",
          f"silver rows={s_rows} distinct={s_ids} bronze distinct={b_ids} (duplicates removed: {b_rows - s_rows})")

    late, very_late, missing = q("""
        SELECT count(*) FILTER (kafka_timestamp - event_timestamp > INTERVAL 5 SECOND),
               count(*) FILTER (kafka_timestamp - event_timestamp > INTERVAL 30 SECOND),
               count(*) FILTER (kafka_timestamp - event_timestamp > INTERVAL 5 SECOND
                                AND trade_id NOT IN (SELECT trade_id FROM lake.silver_fx_trade))
        FROM lake.bronze_fx_trade""")
    check(late > 0 and missing == 0, "late events land",
          f"late>5s={late} ({late / max(b_rows, 1):.1%}) beyond 30s watermark={very_late} missing from silver={missing}")

    files = q(f"SELECT count(*) FROM glob('{cfg.data_path}**/*.parquet')")[0]
    batches, avg_rows, max_rows = q("SELECT count(*), avg(bronze_rows), max(bronze_rows) FROM lake.ingest_batch")
    print(f"[INFO] small files: parquet={files} batches={batches} rows/batch avg={avg_rows or 0:.0f} "
          f"max={max_rows} inlining_row_limit={cfg.inlining_row_limit}")

    if failures:
        raise SystemExit(f"{len(failures)} check(s) failed: {', '.join(failures)}")


if __name__ == "__main__":
    main()
