"""DuckLake access: config, connection, tables, Kafka-batch decoding and the exactly-once batch writer."""

from __future__ import annotations

import os

import duckdb
import pyarrow as pa
from pydantic import BaseModel, ConfigDict

from fx_producer.adapters.avro_local import REGISTRY_HEADER_BYTES, decode
from fx_producer.config import TRADE_SCHEMA

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

BRONZE_INSERT = "INSERT INTO lake.bronze_fx_trade BY NAME SELECT *, now() AS etl_loaded_at, ?::BIGINT AS batch_id FROM src"

# Insert-if-absent on trade_id (chaos duplicates are identical copies, so nothing to update).
# Not MERGE INTO: in DuckDB 1.5.6 a MERGE that inserts writes a Parquet file every time, bypassing inlining.
# ponytail: anti-join scans all of silver per batch; add an event_timestamp bound + partitioning when it gets slow.
SILVER_INSERT = f"""
INSERT INTO lake.silver_fx_trade
SELECT {TRADE_COLUMNS}, now() AS etl_loaded_at
FROM (SELECT DISTINCT ON (trade_id) * FROM src ORDER BY trade_id, kafka_partition, kafka_offset) b
WHERE NOT EXISTS (SELECT 1 FROM lake.silver_fx_trade s WHERE s.trade_id = b.trade_id)
"""


class LakeConfig(BaseModel):
    """Where the lake lives. No endpoint or credential defaults: they only come from the environment."""

    model_config = ConfigDict(frozen=True)

    catalog: str                    # after "ducklake:", e.g. "postgres:host=... dbname=ducklake" or a file path
    data_path: str                  # "s3://f1-lake/" or a local directory
    inlining_row_limit: int = 5000  # DuckLake's default is 10; a 5 s micro-batch is ~2.5k rows
    s3_endpoint: str | None = None  # None = AWS S3
    s3_region: str | None = None
    s3_key_id: str | None = None
    s3_secret: str | None = None
    s3_use_ssl: bool = True
    s3_url_style: str = "vhost"

    @classmethod
    def from_env(cls) -> LakeConfig:
        env = {
            "catalog": "F1_LAKE_CATALOG", "data_path": "F1_LAKE_DATA_PATH",
            "inlining_row_limit": "F1_LAKE_INLINING_ROW_LIMIT",
            "s3_endpoint": "F1_S3_ENDPOINT", "s3_region": "F1_S3_REGION", "s3_key_id": "F1_S3_KEY_ID",
            "s3_secret": "F1_S3_SECRET", "s3_use_ssl": "F1_S3_USE_SSL", "s3_url_style": "F1_S3_URL_STYLE",
        }
        return cls(**{field: os.environ[var] for field, var in env.items() if var in os.environ})


def _quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def connect(cfg: LakeConfig) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    if cfg.s3_key_id:
        options = {"KEY_ID": cfg.s3_key_id, "SECRET": cfg.s3_secret, "REGION": cfg.s3_region,
                   "ENDPOINT": cfg.s3_endpoint, "URL_STYLE": cfg.s3_url_style}
        settings = ", ".join(f"{k} {_quote(v)}" for k, v in options.items() if v is not None)
        con.execute(f"CREATE SECRET lake_s3 (TYPE s3, {settings}, USE_SSL {str(cfg.s3_use_ssl).lower()})")
    con.execute(f"ATTACH {_quote('ducklake:' + cfg.catalog)} AS lake "
                f"(DATA_PATH {_quote(cfg.data_path)}, DATA_INLINING_ROW_LIMIT {cfg.inlining_row_limit})")
    return con


def create_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(DDL)


def decode_batch(kafka: pa.Table, schema_id: int) -> pa.Table:
    """Kafka columns (value, kafka_partition, kafka_offset, kafka_timestamp) -> trade columns + lineage.

    Fails loudly on any record not written with `schema_id`: quarantine arrives in Phase 3.
    """
    header = b"\x00" + schema_id.to_bytes(4, "big")
    rows = []
    for value, offset in zip(kafka["value"].to_pylist(), kafka["kafka_offset"].to_pylist()):
        if value[:REGISTRY_HEADER_BYTES] != header:
            raise ValueError(f"offset {offset}: header {value[:REGISTRY_HEADER_BYTES].hex()} != {header.hex()}")
        rows.append(decode(value[REGISTRY_HEADER_BYTES:], TRADE_SCHEMA))
    trades = pa.Table.from_pylist(rows, schema=TRADE_ARROW)
    for name in ("kafka_partition", "kafka_offset", "kafka_timestamp"):
        trades = trades.append_column(name, kafka[name])
    return trades


def write_batch(con: duckdb.DuckDBPyConnection, batch_id: int, src: pa.Table) -> bool:
    """Land one micro-batch in bronze + silver + the ledger, atomically. False = batch_id already landed."""
    con.register("src", src)
    con.execute("BEGIN")
    try:
        if con.execute("SELECT 1 FROM lake.ingest_batch WHERE batch_id = ?", [batch_id]).fetchone():
            con.execute("ROLLBACK")
            return False
        con.execute(BRONZE_INSERT, [batch_id])
        con.execute(SILVER_INSERT)
        con.execute("INSERT INTO lake.ingest_batch VALUES (?, ?, now())", [batch_id, src.num_rows])
        con.execute("COMMIT")
        return True
    except BaseException:
        con.execute("ROLLBACK")
        raise
    finally:
        con.unregister("src")
