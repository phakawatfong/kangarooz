"""Offline Avro helpers (no broker / registry needed): used by --dry-run and tests."""

from __future__ import annotations

import io
import json
from functools import lru_cache

from fastavro import parse_schema, schemaless_reader, schemaless_writer

from .config import SCHEMA_DIR

# Confluent wire format: magic byte 0x00 + 4-byte schema id, then the Avro body.
REGISTRY_HEADER_BYTES = 5


def schema_text(filename: str) -> str:
    return (SCHEMA_DIR / filename).read_text()


@lru_cache(maxsize=None)
def parsed_schema(filename: str) -> dict:
    return parse_schema(json.loads(schema_text(filename)))


def encode(record: dict, filename: str) -> bytes:
    buf = io.BytesIO()
    schemaless_writer(buf, parsed_schema(filename), record)
    return buf.getvalue()


def decode(payload: bytes, filename: str) -> dict:
    return schemaless_reader(io.BytesIO(payload), parsed_schema(filename))
