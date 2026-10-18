"""DuckLake writer on a local file catalog: decode, exactly-once batches, silver dedupe, inlining."""

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pytest
from pydantic import ValidationError

from fx_producer.adapters.avro_local import encode
from fx_producer.config import TRADE_SCHEMA
from fx_producer.domain import Trade
from lakehouse import lake
from lakehouse.lake import LakeConfig, connect, create_tables, decode_batch, write_batch

SCHEMA_ID = 7
NOW = datetime(2026, 10, 17, 9, 0, tzinfo=timezone.utc)


def _trade(n: int, late_s: int = 0) -> Trade:
    return Trade(trade_id=f"t{n}", user_id="U00001", currency_pair="USD/THB", side="BUY",
                 rate=Decimal("33.50010000"), base_amount=Decimal("1000.00000000"),
                 event_timestamp=NOW - timedelta(seconds=late_s))


def _kafka(trades: list[Trade], first_offset: int = 0, schema_id: int = SCHEMA_ID) -> pa.Table:
    header = b"\x00" + schema_id.to_bytes(4, "big")
    return pa.table({
        "value": pa.array([header + encode(t.model_dump(), TRADE_SCHEMA) for t in trades], pa.binary()),
        "kafka_partition": pa.array([0] * len(trades), pa.int32()),
        "kafka_offset": pa.array(range(first_offset, first_offset + len(trades)), pa.int64()),
        "kafka_timestamp": pa.array([NOW] * len(trades), pa.timestamp("ms", "UTC")),
    })


def _lake(tmp_path: Path, limit: int = 100):
    con = connect(LakeConfig(catalog=str(tmp_path / "meta.ducklake"), data_path=str(tmp_path / "data") + "/",
                             inlining_row_limit=limit))
    create_tables(con)
    return con


def _counts(con) -> tuple[int, int, int]:
    return tuple(con.execute(f"SELECT count(*) FROM lake.{t}").fetchone()[0]
                 for t in ("bronze_fx_trade", "silver_fx_trade", "ingest_batch"))


def _parquet_files(tmp_path: Path) -> int:
    return len(list((tmp_path / "data").rglob("*.parquet")))


def test_decode_round_trips_trade_and_lineage():
    t = _trade(1)
    row = decode_batch(_kafka([t], first_offset=41), SCHEMA_ID).to_pylist()[0]
    assert row == {**t.model_dump(), "kafka_partition": 0, "kafka_offset": 41, "kafka_timestamp": NOW}


def test_decode_rejects_other_schema_id():
    with pytest.raises(ValueError, match="offset 0: header 0000000008 != 0000000007"):
        decode_batch(_kafka([_trade(1)], schema_id=8), SCHEMA_ID)


def test_batches_land_once_and_silver_dedupes(tmp_path):
    con = _lake(tmp_path)
    late, dup = _trade(2, late_s=90), _trade(1)
    assert write_batch(con, 0, decode_batch(_kafka([_trade(1), dup, late]), SCHEMA_ID))
    assert _counts(con) == (3, 2, 1)

    assert not write_batch(con, 0, decode_batch(_kafka([_trade(1), dup, late]), SCHEMA_ID))
    assert _counts(con) == (3, 2, 1)

    assert write_batch(con, 1, decode_batch(_kafka([_trade(2), _trade(3)], first_offset=3), SCHEMA_ID))
    assert _counts(con) == (5, 3, 2)
    late_ms = con.execute("SELECT epoch_ms(event_timestamp) FROM lake.silver_fx_trade WHERE trade_id = 't2'").fetchone()
    assert late_ms == (int((NOW - timedelta(seconds=90)).timestamp() * 1000),)


def test_failed_batch_leaves_nothing_behind(tmp_path, monkeypatch):
    con = _lake(tmp_path)
    monkeypatch.setattr(lake, "SILVER_INSERT", "INSERT INTO lake.silver_fx_trade SELECT 1")
    with pytest.raises(Exception):
        write_batch(con, 0, decode_batch(_kafka([_trade(1)]), SCHEMA_ID))
    assert _counts(con) == (0, 0, 0)


def test_small_batches_are_inlined_until_flush(tmp_path):
    con = _lake(tmp_path, limit=100)
    write_batch(con, 0, decode_batch(_kafka([_trade(n) for n in range(50)]), SCHEMA_ID))
    assert _parquet_files(tmp_path) == 0
    con.execute("CALL ducklake_flush_inlined_data('lake')")
    assert _parquet_files(tmp_path) == 3  # one per table: bronze, silver, ledger


def test_inlining_off_writes_parquet_per_batch(tmp_path):
    con = _lake(tmp_path, limit=0)
    write_batch(con, 0, decode_batch(_kafka([_trade(n) for n in range(50)]), SCHEMA_ID))
    assert _parquet_files(tmp_path) == 3  # bronze, silver, ledger


def test_lake_config_needs_catalog_and_data_path(monkeypatch):
    for var in ("F1_LAKE_CATALOG", "F1_LAKE_DATA_PATH"):
        monkeypatch.delenv(var, raising=False)
    with pytest.raises(ValidationError):
        LakeConfig.from_env()
