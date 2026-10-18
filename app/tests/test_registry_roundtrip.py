"""Serializer path with a mock Schema Registry: Confluent wire format + decode via schema id."""

import random
from datetime import datetime, timezone

import pytest
from confluent_kafka.schema_registry import Schema
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from contract.avro_codec import TRADE_SCHEMA, schema_text
from contract.kafka import TRADES_TOPIC, value_subject
from fx_producer.adapters.kafka_sink import SERIALIZER_CONF
from fx_producer.config import SimConfig
from fx_producer.simulation.market import MarketSimulator

mock = pytest.importorskip("confluent_kafka.schema_registry._sync.mock_schema_registry_client")


def test_wire_format_and_decode():
    registry = mock.MockSchemaRegistryClient({"url": "mock://f1"})
    schema_id = registry.register_schema(value_subject(TRADES_TOPIC), Schema(schema_text(TRADE_SCHEMA), "AVRO"))
    serializer = AvroSerializer(registry, schema_text(TRADE_SCHEMA), conf=SERIALIZER_CONF)
    trade = MarketSimulator(SimConfig(seed=2), random.Random(2)).make_trade(datetime.now(timezone.utc))
    ctx = SerializationContext(TRADES_TOPIC, MessageField.VALUE)
    payload = serializer(trade.model_dump(), ctx)
    assert payload[0] == 0  # magic byte
    assert int.from_bytes(payload[1:5], "big") == schema_id  # schema id travels with every message
    assert AvroDeserializer(registry)(payload, ctx)["trade_id"] == trade.trade_id
