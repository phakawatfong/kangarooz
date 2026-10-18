"""Check Phase 1 design assumptions against what is actually in Redpanda.

    uv run python -m fx_producer.verify [--max 5000]

Answers: real message size vs the ~150 B estimate, the 5-byte registry header,
partition spread with key=trade_id, duplicates present, ground-truth episodes by type.
"""

from __future__ import annotations

import argparse
import statistics
import uuid
from collections import Counter

from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

from .config import GROUND_TRUTH_TOPIC, TRADES_TOPIC, Connection


def _consume(conn: Connection, topic: str, max_msgs: int, idle_timeout: float = 5.0) -> list:
    c = Consumer(
        {
            "bootstrap.servers": conn.bootstrap_servers,
            "group.id": f"f1-verify-{uuid.uuid4().hex[:8]}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    c.subscribe([topic])
    msgs = []
    try:
        while len(msgs) < max_msgs:
            m = c.poll(idle_timeout)
            if m is None:
                break
            if m.error():
                print(f"[consumer error] {m.error()}")
                continue
            msgs.append(m)
    finally:
        c.close()
    return msgs


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--max", type=int, default=5000)
    args = ap.parse_args()
    conn = Connection.from_env()

    msgs = _consume(conn, TRADES_TOPIC, args.max)
    if not msgs:
        print(f"no messages in {TRADES_TOPIC}; run the producer first")
        return
    values = [m.value() for m in msgs]
    keys = [m.key() or b"" for m in msgs]
    vsizes = [len(v) for v in values]
    print(f"== {TRADES_TOPIC}: read {len(msgs)} messages")
    print(f"value size  avg={statistics.mean(vsizes):.1f}B  min={min(vsizes)}  max={max(vsizes)}  "
          f"(estimate on the sizing card: ~150B incl. key+overhead)")
    print(f"key size    avg={statistics.mean(len(k) for k in keys):.1f}B")
    magic = Counter(v[0] for v in values)
    schema_ids = Counter(int.from_bytes(v[1:5], "big") for v in values)
    print(f"header      magic bytes={dict(magic)}  schema ids={dict(schema_ids)}")
    print(f"raw first 12 bytes of one message: {values[0][:12].hex(' ')}")
    parts = Counter(m.partition() for m in msgs)
    print(f"partitions  {dict(sorted(parts.items()))}  (key=trade_id -> expect roughly even)")
    dup_keys = sum(n - 1 for n in Counter(keys).values() if n > 1)
    print(f"duplicates  {dup_keys} repeated trade_ids (deliberate chaos; the sink must dedupe)")

    sr = SchemaRegistryClient({"url": conn.schema_registry_url})
    deser = AvroDeserializer(sr)
    sample = deser(values[0], SerializationContext(TRADES_TOPIC, MessageField.VALUE))
    print(f"decoded sample (via registry schema id): {sample}")

    eps = _consume(conn, GROUND_TRUTH_TOPIC, 10_000, idle_timeout=3.0)
    types = Counter(deser(m.value(), SerializationContext(GROUND_TRUTH_TOPIC, MessageField.VALUE))["anomaly_type"]
                    for m in eps)
    print(f"== {GROUND_TRUTH_TOPIC}: {len(eps)} episodes {dict(types)}")


if __name__ == "__main__":
    main()
