"""Golden master: a seeded producer run must print the same output and send the same Kafka bytes.

Regenerate the expected files only on an intended behaviour change: UPDATE_GOLDEN=1 uv run pytest
"""

import hashlib
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from confluent_kafka.schema_registry import Schema

from contract.avro_codec import schema_text
from fx_producer import produce
from fx_producer.register_schemas import SCHEMA_FILE_BY_SUBJECT

mock = pytest.importorskip("confluent_kafka.schema_registry._sync.mock_schema_registry_client")

GOLDEN = Path(__file__).parent / "golden"
KAFKA_MODULE = "fx_producer.adapters.kafka_sink"
EPOCH = datetime(2026, 10, 14, 9, 0, tzinfo=timezone.utc)
ARGS = [
    "--seed", "42", "--duration", "12",
    "--price-spike-per-min", "30", "--fat-finger-per-min", "30",
    "--volume-burst-per-min", "10", "--account-burst-per-min", "10",
    "--late-pct", "0.2", "--duplicate-pct", "0.1",
]


def _check(name: str, actual: str) -> None:
    path = GOLDEN / name
    if os.environ.get("UPDATE_GOLDEN"):
        path.write_text(actual)
    assert actual == path.read_text()


@pytest.fixture
def fake_clock(monkeypatch):
    now = [0.0]

    class FakeDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return EPOCH + timedelta(seconds=now[0])

    monkeypatch.setattr(time, "monotonic", lambda: now[0])
    monkeypatch.setattr(time, "sleep", lambda s: now.__setitem__(0, now[0] + s))
    monkeypatch.setattr(produce, "datetime", FakeDatetime)


@pytest.fixture
def stub_kafka(monkeypatch):
    sent: list[tuple[str, bytes, bytes]] = []

    class StubProducer:
        def __init__(self, conf):
            pass

        def produce(self, topic, key, value, on_delivery):
            sent.append((topic, key, value))
            on_delivery(None, None)

        def poll(self, timeout):
            return 0

        def flush(self, timeout):
            return 0

    def registry(conf):
        client = mock.MockSchemaRegistryClient({"url": "mock://golden"})
        for subject, filename in SCHEMA_FILE_BY_SUBJECT.items():
            client.register_schema(subject, Schema(schema_text(filename), "AVRO"))
        return client

    monkeypatch.setenv("F1_BOOTSTRAP", "stub:9092")
    monkeypatch.setenv("F1_SCHEMA_REGISTRY", "mock://golden")
    monkeypatch.setattr(f"{KAFKA_MODULE}.Producer", StubProducer)
    monkeypatch.setattr(f"{KAFKA_MODULE}.SchemaRegistryClient", registry)
    return sent


def test_dry_run_output(fake_clock, capsys):
    produce.main([*ARGS, "--dry-run"])
    _check("dry_run.txt", capsys.readouterr().out)


def test_kafka_output_and_wire_bytes(fake_clock, stub_kafka, capsys):
    produce.main(ARGS)
    digest = hashlib.sha256()
    for record in stub_kafka:
        digest.update(repr(record).encode())
    topics = {t: sum(1 for r in stub_kafka if r[0] == t) for t in sorted({r[0] for r in stub_kafka})}
    _check("kafka.txt", f"{capsys.readouterr().out}records={topics}\nsha256={digest.hexdigest()}\n")


def test_help(monkeypatch, capsys):
    monkeypatch.setenv("COLUMNS", "80")
    monkeypatch.setattr(sys, "argv", ["produce.py"])
    with pytest.raises(SystemExit):
        produce.main(["--help"])
    _check("help.txt", capsys.readouterr().out)
