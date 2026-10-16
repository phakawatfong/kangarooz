"""Kafka + Schema Registry I/O for trades (fx.trades) and ground truth (fx.trades.ground_truth)."""

from __future__ import annotations

from confluent_kafka import KafkaError, Message, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer

from .avro_local import schema_text
from .config import EPISODE_SCHEMA, GROUND_TRUTH_TOPIC, TRADE_SCHEMA, TRADES_TOPIC, Connection
from .model import AnomalyEpisode, Trade

# Schemas are registered explicitly by `python -m fx_producer.registry` (the contract is owned in
# /schemas, not invented by whichever producer runs first). Serialize with the latest registered version.
SERIALIZER_CONF = {"auto.register.schemas": False, "use.latest.version": True}


class FxProducer:
    """Writes trades and ground-truth episodes.

    Note: writing to two topics is a dual write. Acceptable here because this is a SIMULATOR
    and the ground truth is grading data, not business data. A real service would use one
    system of record + CDC/outbox (or Kafka transactions to make both writes atomic).
    """

    def __init__(self, conn: Connection | None = None):
        conn = conn or Connection()
        self.sr = SchemaRegistryClient({"url": conn.schema_registry_url})
        self.trade_ser = AvroSerializer(self.sr, schema_text(TRADE_SCHEMA), conf=SERIALIZER_CONF)
        self.episode_ser = AvroSerializer(self.sr, schema_text(EPISODE_SCHEMA), conf=SERIALIZER_CONF)
        self.key_ser = StringSerializer("utf_8")
        self.producer = Producer(
            {
                "bootstrap.servers": conn.bootstrap_servers,
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
        value = self.trade_ser(trade.to_avro(), SerializationContext(TRADES_TOPIC, MessageField.VALUE))
        # Record key = trade_id: even spread + duplicates land in the same partition.
        key = self.key_ser(trade.trade_id, SerializationContext(TRADES_TOPIC, MessageField.KEY))
        self.sent += 1
        self.value_bytes += len(value)
        self.key_bytes += len(key)
        self._produce(TRADES_TOPIC, key, value)

    def send_episode(self, ep: AnomalyEpisode) -> None:
        value = self.episode_ser(ep.to_avro(), SerializationContext(GROUND_TRUTH_TOPIC, MessageField.VALUE))
        key = self.key_ser(ep.episode_id, SerializationContext(GROUND_TRUTH_TOPIC, MessageField.KEY))
        self._produce(GROUND_TRUTH_TOPIC, key, value)

    def flush(self, timeout: float = 30.0) -> int:
        return self.producer.flush(timeout)
