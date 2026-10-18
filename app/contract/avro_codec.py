"""Avro schemas from /schemas and offline encode/decode (no broker or registry needed)."""

from __future__ import annotations

import io
import json
from functools import lru_cache
from pathlib import Path

from fastavro import parse_schema, schemaless_reader, schemaless_writer

# This file lives in <repo>/app/contract/; the .avsc files are the source of truth in <repo>/schemas/.
SCHEMA_DIR = Path(__file__).resolve().parents[2] / "schemas"
TRADE_SCHEMA = "fx_trade.avsc"
EPISODE_SCHEMA = "anomaly_episode.avsc"

# Confluent wire format: magic byte 0x00 + 4-byte schema id, then the Avro body.
REGISTRY_HEADER_BYTES = 5


def registry_header(schema_id: int) -> bytes:
    return b"\x00" + schema_id.to_bytes(4, "big")


def schema_text(filename: str) -> str:
    return (SCHEMA_DIR / filename).read_text()


@lru_cache(maxsize=None)
def parsed_schema(filename: str) -> dict:
    return parse_schema(json.loads(schema_text(filename)))


def encode(record: dict, filename: str) -> bytes:
    buffer = io.BytesIO()
    schemaless_writer(buffer, parsed_schema(filename), record)
    return buffer.getvalue()


def decode(payload: bytes, filename: str) -> dict:
    return schemaless_reader(io.BytesIO(payload), parsed_schema(filename))
