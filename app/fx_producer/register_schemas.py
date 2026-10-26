"""Register the Avro contracts and set BACKWARD compatibility.

    uv run python -m fx_producer.register_schemas
"""

from __future__ import annotations

from confluent_kafka.schema_registry import Schema, SchemaRegistryClient

from contract.avro_codec import ALERT_SCHEMA, EPISODE_SCHEMA, TRADE_SCHEMA, schema_text
from contract.kafka import ALERTS_TOPIC, GROUND_TRUTH_TOPIC, TRADES_TOPIC, KafkaConfig, value_subject

SCHEMA_FILE_BY_SUBJECT = {
    value_subject(TRADES_TOPIC): TRADE_SCHEMA,
    value_subject(GROUND_TRUTH_TOPIC): EPISODE_SCHEMA,
    value_subject(ALERTS_TOPIC): ALERT_SCHEMA,
}


def main() -> None:
    kafka_config = KafkaConfig.from_env()
    registry = SchemaRegistryClient({"url": kafka_config.schema_registry_url})
    for subject, filename in SCHEMA_FILE_BY_SUBJECT.items():
        schema_id = registry.register_schema(subject, Schema(schema_text(filename), "AVRO"), normalize_schemas=True)
        # BACKWARD: new readers can read old data. Note the trap discussed in ADR-002:
        # adding an enum symbol passes BACKWARD but can break LAGGING (old) consumers.
        registry.set_compatibility(subject_name=subject, level="BACKWARD")
        print(f"registered {subject:<36} id={schema_id}  compatibility=BACKWARD  ({filename})")


if __name__ == "__main__":
    main()
