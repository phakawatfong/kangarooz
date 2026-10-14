"""Serializer path with a mock Schema Registry: Confluent wire format + decode via schema id."""

import random
from datetime import datetime, timezone

import pytest
from confluent_kafka.schema_registry import Schema
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from fx_producer.avro_local import schema_text
from fx_producer.config import SimConfig
from fx_producer.market import MarketSimulator

mock = pytest.importorskip("confluent_kafka.schema_registry._sync.mock_schema_registry_client")


def test_wire_format_and_decode():
    sr = mock.MockSchemaRegistryClient({"url": "mock://f1"})
    schema_id = sr.register_schema("fx.trades-value", Schema(schema_text("fx_trade.avsc"), "AVRO"))
    ser = AvroSerializer(sr, schema_text("fx_trade.avsc"), conf={"auto.register.schemas": False, "use.latest.version": True})
    trade = MarketSimulator(SimConfig(seed=2), random.Random(2)).make_trade(datetime.now(timezone.utc))
    ctx = SerializationContext("fx.trades", MessageField.VALUE)
    payload = ser(trade.to_avro(), ctx)
    assert payload[0] == 0  # magic byte
    assert int.from_bytes(payload[1:5], "big") == schema_id  # schema id travels with every message
    assert AvroDeserializer(sr)(payload, ctx)["trade_id"] == trade.trade_id
