"""Kafka topics and where the broker and Schema Registry run."""

from __future__ import annotations

import os

from pydantic import BaseModel, ConfigDict

TRADES_TOPIC = "fx.trades"
GROUND_TRUTH_TOPIC = "fx.trades.ground_truth"


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
