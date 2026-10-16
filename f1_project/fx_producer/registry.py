"""Register the Avro contracts and set BACKWARD compatibility.

    uv run python -m fx_producer.registry

Subjects follow TopicNameStrategy: <topic>-value.
"""

from __future__ import annotations

from confluent_kafka.schema_registry import Schema, SchemaRegistryClient

from .avro_local import schema_text
from .config import EPISODE_SCHEMA, GROUND_TRUTH_TOPIC, TRADE_SCHEMA, TRADES_TOPIC, Connection

SUBJECTS = {
    f"{TRADES_TOPIC}-value": TRADE_SCHEMA,
    f"{GROUND_TRUTH_TOPIC}-value": EPISODE_SCHEMA,
}


def main() -> None:
    conn = Connection.from_env()
    sr = SchemaRegistryClient({"url": conn.schema_registry_url})
    for subject, filename in SUBJECTS.items():
        schema_id = sr.register_schema(subject, Schema(schema_text(filename), "AVRO"), normalize_schemas=True)
        # BACKWARD: new readers can read old data. Note the trap discussed in ADR-002:
        # adding an enum symbol passes BACKWARD but can break LAGGING (old) consumers.
        sr.set_compatibility(subject_name=subject, level="BACKWARD")
        print(f"registered {subject:<36} id={schema_id}  compatibility=BACKWARD  ({filename})")


if __name__ == "__main__":
    main()
