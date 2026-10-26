"""Kafka topics and where the broker and Schema Registry run."""

from __future__ import annotations

import os
import uuid

from confluent_kafka import Consumer, Message
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from pydantic import BaseModel, ConfigDict

from .avro_codec import schema_text

TRADES_TOPIC = "fx.trades"
GROUND_TRUTH_TOPIC = "fx.trades.ground_truth"
ALERTS_TOPIC = "fx.alerts"


def value_subject(topic: str) -> str:
    """Schema Registry subject for a topic's values (TopicNameStrategy)."""
    return f"{topic}-value"


class KafkaConfig(BaseModel):
    """Broker and Schema Registry addresses, from .env."""

    model_config = ConfigDict(frozen=True)

    bootstrap_servers: str
    schema_registry_url: str

    @classmethod
    def from_env(cls) -> KafkaConfig:
        return cls(
            bootstrap_servers=os.environ["F1_BOOTSTRAP"],
            schema_registry_url=os.environ["F1_SCHEMA_REGISTRY"],
        )


def registered_schema_id(kafka_config: KafkaConfig, topic: str, filename: str) -> int:
    """The registry id of schemas/<filename> under the topic's subject; fails if that exact schema was never registered."""
    registry = SchemaRegistryClient({"url": kafka_config.schema_registry_url})
    return registry.lookup_schema(value_subject(topic), Schema(schema_text(filename), "AVRO"),
                                  normalize_schemas=True).schema_id


def consume(kafka_config: KafkaConfig, topic: str, max_messages: int, idle_timeout: float = 5.0) -> list[Message]:
    """Read a topic from the earliest offset with a throwaway group, until max_messages or idle_timeout s of silence."""
    consumer = Consumer(
        {
            "bootstrap.servers": kafka_config.bootstrap_servers,
            "group.id": f"f1-read-{uuid.uuid4().hex[:8]}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    messages = []
    try:
        while len(messages) < max_messages:
            message = consumer.poll(idle_timeout)
            if message is None:
                break
            if message.error():
                print(f"[consumer error] {message.error()}")
                continue
            messages.append(message)
    finally:
        consumer.close()
    return messages
