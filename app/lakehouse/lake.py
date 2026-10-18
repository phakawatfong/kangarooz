"""DuckLake access: connection, tables, Kafka-batch decoding and the exactly-once batch writer."""

from __future__ import annotations

import logging

import duckdb
import pyarrow as pa

from contract.avro_codec import REGISTRY_HEADER_BYTES, TRADE_SCHEMA, decode, registry_header

from .config import LakeConfig

log = logging.getLogger(__name__)

TRADE_ARROW = pa.schema([
    ("trade_id", pa.string()),
    ("user_id", pa.string()),
    ("currency_pair", pa.string()),
    ("side", pa.string()),
    ("rate", pa.decimal128(18, 8)),
    ("base_amount", pa.decimal128(18, 8)),
    ("event_timestamp", pa.timestamp("ms", "UTC")),
])
TRADE_COLUMNS = ", ".join(TRADE_ARROW.names)
TRADE_DDL = """
    trade_id VARCHAR NOT NULL, user_id VARCHAR NOT NULL, currency_pair VARCHAR NOT NULL, side VARCHAR NOT NULL,
    rate DECIMAL(18,8) NOT NULL, base_amount DECIMAL(18,8) NOT NULL, event_timestamp TIMESTAMPTZ NOT NULL"""

DDL = f"""
CREATE TABLE IF NOT EXISTS lake.bronze_fx_trade ({TRADE_DDL},
    kafka_partition INTEGER NOT NULL, kafka_offset BIGINT NOT NULL, kafka_timestamp TIMESTAMPTZ NOT NULL,
    etl_loaded_at TIMESTAMPTZ NOT NULL, batch_id BIGINT NOT NULL);
CREATE TABLE IF NOT EXISTS lake.silver_fx_trade ({TRADE_DDL}, etl_loaded_at TIMESTAMPTZ NOT NULL);
CREATE TABLE IF NOT EXISTS lake.ingest_batch (
    batch_id BIGINT NOT NULL, bronze_rows BIGINT NOT NULL, committed_at TIMESTAMPTZ NOT NULL);
"""

# `incoming` is the decoded micro-batch, registered as a view for the duration of write_batch.
BRONZE_INSERT = "INSERT INTO lake.bronze_fx_trade BY NAME SELECT *, now() AS etl_loaded_at, ?::BIGINT AS batch_id FROM incoming"

# Insert-if-absent on trade_id (chaos duplicates are identical copies, so nothing to update).
# Not MERGE INTO: in DuckDB 1.5.6 a MERGE that inserts writes a Parquet file every time, bypassing inlining.
# ponytail: anti-join scans all of silver per batch; add an event_timestamp bound + partitioning when it gets slow.
SILVER_INSERT = f"""
INSERT INTO lake.silver_fx_trade
SELECT {TRADE_COLUMNS}, now() AS etl_loaded_at
FROM (SELECT DISTINCT ON (trade_id) * FROM incoming ORDER BY trade_id, kafka_partition, kafka_offset) candidate
WHERE NOT EXISTS (SELECT 1 FROM lake.silver_fx_trade existing WHERE existing.trade_id = candidate.trade_id)
"""


def _quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def connect(lake_config: LakeConfig) -> duckdb.DuckDBPyConnection:
    """A DuckDB connection with the lake attached as `lake`."""
    log.debug("attaching lake: %s", lake_config.describe())
    db = duckdb.connect()
    if lake_config.s3_key_id:
        options = {"KEY_ID": lake_config.s3_key_id, "SECRET": lake_config.s3_secret, "REGION": lake_config.s3_region,
                   "ENDPOINT": lake_config.s3_endpoint, "URL_STYLE": lake_config.s3_url_style}
        settings = ", ".join(f"{k} {_quote(v)}" for k, v in options.items() if v is not None)
        db.execute(f"CREATE SECRET lake_s3 (TYPE s3, {settings}, USE_SSL {str(lake_config.s3_use_ssl).lower()})")
    db.execute(f"ATTACH {_quote('ducklake:' + lake_config.catalog)} AS lake "
               f"(DATA_PATH {_quote(lake_config.data_path)}, DATA_INLINING_ROW_LIMIT {lake_config.inlining_row_limit})")
    return db


def create_tables(db: duckdb.DuckDBPyConnection) -> None:
    db.execute(DDL)


def decode_batch(kafka_rows: pa.Table, schema_id: int) -> pa.Table:
    """Kafka columns (value, kafka_partition, kafka_offset, kafka_timestamp) -> trade columns + lineage.

    Fails loudly on any record not written with `schema_id`: quarantine arrives in Phase 3.
    """
    header = registry_header(schema_id)
    rows = []
    for value, offset in zip(kafka_rows["value"].to_pylist(), kafka_rows["kafka_offset"].to_pylist()):
        if value[:REGISTRY_HEADER_BYTES] != header:
            raise ValueError(f"offset {offset}: header {value[:REGISTRY_HEADER_BYTES].hex()} != {header.hex()}")
        rows.append(decode(value[REGISTRY_HEADER_BYTES:], TRADE_SCHEMA))
    trades = pa.Table.from_pylist(rows, schema=TRADE_ARROW)
    for name in ("kafka_partition", "kafka_offset", "kafka_timestamp"):
        trades = trades.append_column(name, kafka_rows[name])
    return trades


def write_batch(db: duckdb.DuckDBPyConnection, batch_id: int, incoming: pa.Table) -> bool:
    """Land one micro-batch in bronze + silver + the ledger, atomically. False = batch_id already landed."""
    db.register("incoming", incoming)
    db.execute("BEGIN")
    try:
        if db.execute("SELECT 1 FROM lake.ingest_batch WHERE batch_id = ?", [batch_id]).fetchone():
            db.execute("ROLLBACK")
            log.debug("batch %d: already in the ledger, skipped", batch_id)
            return False
        db.execute(BRONZE_INSERT, [batch_id])
        silver_rows = db.execute(SILVER_INSERT).fetchone()[0]
        db.execute("INSERT INTO lake.ingest_batch VALUES (?, ?, now())", [batch_id, incoming.num_rows])
        db.execute("COMMIT")
        log.debug("batch %d: bronze +%d, silver +%d (%d duplicates dropped)",
                  batch_id, incoming.num_rows, silver_rows, incoming.num_rows - silver_rows)
        return True
    except BaseException:
        db.execute("ROLLBACK")
        raise
    finally:
        db.unregister("incoming")
