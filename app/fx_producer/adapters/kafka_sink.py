"""Kafka + Schema Registry I/O for trades (fx.trades) and ground truth (fx.trades.ground_truth)."""

from __future__ import annotations

import logging

from confluent_kafka import KafkaError, Message, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer

from contract.avro_codec import EPISODE_SCHEMA, TRADE_SCHEMA, schema_text
from contract.kafka import GROUND_TRUTH_TOPIC, TRADES_TOPIC, KafkaConfig
from ..domain import AnomalyEpisode, Trade

# Schemas are registered explicitly by `python -m fx_producer.register_schemas` (the contract is owned in
# /schemas, not invented by whichever producer runs first). Serialize with the latest registered version.
SERIALIZER_CONF = {"auto.register.schemas": False, "use.latest.version": True}

log = logging.getLogger(__name__)


class KafkaSink:
    """Writes trades and ground-truth episodes.

    Note: writing to two topics is a dual write. Acceptable here because this is a SIMULATOR
    and the ground truth is grading data, not business data. A real service would use one
    system of record + CDC/outbox (or Kafka transactions to make both writes atomic).
    """

    def __init__(self, kafka_config: KafkaConfig):
        log.debug("kafka: %s", kafka_config)
        registry = SchemaRegistryClient({"url": kafka_config.schema_registry_url})
        self.trade_serializer = AvroSerializer(registry, schema_text(TRADE_SCHEMA), conf=SERIALIZER_CONF)
        self.episode_serializer = AvroSerializer(registry, schema_text(EPISODE_SCHEMA), conf=SERIALIZER_CONF)
        self.key_serializer = StringSerializer("utf_8")
        self.producer = Producer(
            {
                "bootstrap.servers": kafka_config.bootstrap_servers,
                "client.id": "f1-fx-producer",
                "acks": "all",
                "enable.idempotence": True,  # no duplicates from broker-level retries
                "compression.type": "lz4",
                "linger.ms": 20,
            }
        )
        self.sent = 0
        self.delivered = 0
        self.failed = 0
        self.value_bytes = 0
        self.key_bytes = 0

    def _on_delivery(self, err: KafkaError | None, msg: Message) -> None:
        if err is not None:
            self.failed += 1
            if self.failed <= 5:
                print(f"[delivery error] {err}")
        else:
            self.delivered += 1

    def _produce(self, topic: str, key: bytes, value: bytes) -> None:
        while True:
            try:
                self.producer.produce(topic, key=key, value=value, on_delivery=self._on_delivery)
                break
            except BufferError:  # local queue full: let it drain
                self.producer.poll(0.5)
        self.producer.poll(0)

    def send_trade(self, trade: Trade) -> None:
        value = self.trade_serializer(trade.model_dump(), SerializationContext(TRADES_TOPIC, MessageField.VALUE))
        # Record key = trade_id: even spread + duplicates land in the same partition.
        key = self.key_serializer(trade.trade_id, SerializationContext(TRADES_TOPIC, MessageField.KEY))
        self.sent += 1
        self.value_bytes += len(value)
        self.key_bytes += len(key)
        self._produce(TRADES_TOPIC, key, value)

    def send_episode(self, episode: AnomalyEpisode) -> None:
        value = self.episode_serializer(episode.model_dump(), SerializationContext(GROUND_TRUTH_TOPIC, MessageField.VALUE))
        key = self.key_serializer(episode.episode_id, SerializationContext(GROUND_TRUTH_TOPIC, MessageField.KEY))
        self._produce(GROUND_TRUTH_TOPIC, key, value)

    def close(self, elapsed: float) -> None:
        remaining = self.producer.flush(30.0)
        print(f"done: sent={self.sent} delivered={self.delivered} failed={self.failed} "
              f"unflushed={remaining} avg_value={self.value_bytes / max(self.sent, 1):.1f}B "
              f"avg_key={self.key_bytes / max(self.sent, 1):.1f}B rate={self.sent / elapsed:.0f}/s")
