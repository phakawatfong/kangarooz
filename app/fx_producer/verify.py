"""Check Phase 1 design assumptions against what is actually in Redpanda.

    uv run python -m fx_producer.verify [--max 5000]

Answers: real message size vs the ~150 B estimate, the 5-byte registry header,
partition spread with key=trade_id, duplicates present, ground-truth episodes by type.
"""

from __future__ import annotations

import argparse
import statistics
from collections import Counter

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

from contract.kafka import GROUND_TRUTH_TOPIC, TRADES_TOPIC, KafkaConfig, consume


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--max", type=int, default=5000)
    args = parser.parse_args()
    kafka_config = KafkaConfig.from_env()

    messages = consume(kafka_config, TRADES_TOPIC, args.max)
    if not messages:
        print(f"no messages in {TRADES_TOPIC}; run the producer first")
        return
    values = [message.value() for message in messages]
    keys = [message.key() or b"" for message in messages]
    value_sizes = [len(v) for v in values]
    print(f"== {TRADES_TOPIC}: read {len(messages)} messages")
    print(f"value size  avg={statistics.mean(value_sizes):.1f}B  min={min(value_sizes)}  max={max(value_sizes)}  "
          f"(estimate on the sizing card: ~150B incl. key+overhead)")
    print(f"key size    avg={statistics.mean(len(k) for k in keys):.1f}B")
    magic = Counter(v[0] for v in values)
    schema_ids = Counter(int.from_bytes(v[1:5], "big") for v in values)
    print(f"header      magic bytes={dict(magic)}  schema ids={dict(schema_ids)}")
    print(f"raw first 12 bytes of one message: {values[0][:12].hex(' ')}")
    partitions = Counter(message.partition() for message in messages)
    print(f"partitions  {dict(sorted(partitions.items()))}  (key=trade_id -> expect roughly even)")
    dup_keys = sum(n - 1 for n in Counter(keys).values() if n > 1)
    print(f"duplicates  {dup_keys} repeated trade_ids (deliberate chaos; the sink must dedupe)")

    registry = SchemaRegistryClient({"url": kafka_config.schema_registry_url})
    deserializer = AvroDeserializer(registry)
    sample = deserializer(values[0], SerializationContext(TRADES_TOPIC, MessageField.VALUE))
    print(f"decoded sample (via registry schema id): {sample}")

    episodes = consume(kafka_config, GROUND_TRUTH_TOPIC, 10_000, idle_timeout=3.0)
    types = Counter(deserializer(message.value(), SerializationContext(GROUND_TRUTH_TOPIC, MessageField.VALUE))["anomaly_type"]
                    for message in episodes)
    print(f"== {GROUND_TRUTH_TOPIC}: {len(episodes)} episodes {dict(types)}")


if __name__ == "__main__":
    main()
