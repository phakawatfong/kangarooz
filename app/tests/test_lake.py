"""DuckLake writer on a local file catalog: decode, exactly-once batches, silver dedupe, inlining."""

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pytest

from contract.avro_codec import TRADE_SCHEMA, encode, registry_header
from fx_producer.domain import Trade
from lakehouse import compact, lake
from lakehouse.config import LakeConfig
from lakehouse.lake import connect, create_tables, decode_batch, write_batch

SCHEMA_ID = 7
NOW = datetime(2026, 10, 17, 9, 0, tzinfo=timezone.utc)


def _trade(n: int, late_s: int = 0) -> Trade:
    return Trade(trade_id=f"t{n}", user_id="U00001", currency_pair="USD/THB", side="BUY",
                 rate=Decimal("33.50010000"), base_amount=Decimal("1000.00000000"),
                 event_timestamp=NOW - timedelta(seconds=late_s))


def _kafka(trades: list[Trade], first_offset: int = 0, schema_id: int = SCHEMA_ID) -> pa.Table:
    return pa.table({
        "value": pa.array([registry_header(schema_id) + encode(t.model_dump(), TRADE_SCHEMA) for t in trades], pa.binary()),
        "kafka_partition": pa.array([0] * len(trades), pa.int32()),
        "kafka_offset": pa.array(range(first_offset, first_offset + len(trades)), pa.int64()),
        "kafka_timestamp": pa.array([NOW] * len(trades), pa.timestamp("ms", "UTC")),
    })


def _lake(tmp_path: Path, limit: int = 100):
    db = connect(LakeConfig(catalog=str(tmp_path / "meta.ducklake"), data_path=str(tmp_path / "data") + "/",
                            inlining_row_limit=limit, s3_endpoint=None, s3_region=None, s3_key_id=None,
                            s3_secret=None, s3_use_ssl=False, s3_url_style="path"))
    create_tables(db)
    return db


def _counts(db) -> tuple[int, int, int]:
    return tuple(db.execute(f"SELECT count(*) FROM lake.{table}").fetchone()[0]
                 for table in ("bronze_fx_trade", "silver_fx_trade", "ingest_batch"))


def _parquet_files(tmp_path: Path) -> int:
    return len(list((tmp_path / "data").rglob("*.parquet")))


def test_decode_round_trips_trade_and_lineage():
    trade = _trade(1)
    row = decode_batch(_kafka([trade], first_offset=41), SCHEMA_ID).to_pylist()[0]
    assert row == {**trade.model_dump(), "kafka_partition": 0, "kafka_offset": 41, "kafka_timestamp": NOW}


def test_decode_rejects_other_schema_id():
    with pytest.raises(ValueError, match="offset 0: header 0000000008 != 0000000007"):
        decode_batch(_kafka([_trade(1)], schema_id=8), SCHEMA_ID)


def test_batches_land_once_and_silver_dedupes(tmp_path):
    db = _lake(tmp_path)
    late, duplicate = _trade(2, late_s=90), _trade(1)
    assert write_batch(db, 0, decode_batch(_kafka([_trade(1), duplicate, late]), SCHEMA_ID))
    assert _counts(db) == (3, 2, 1)

    assert not write_batch(db, 0, decode_batch(_kafka([_trade(1), duplicate, late]), SCHEMA_ID))
    assert _counts(db) == (3, 2, 1)

    assert write_batch(db, 1, decode_batch(_kafka([_trade(2), _trade(3)], first_offset=3), SCHEMA_ID))
    assert _counts(db) == (5, 3, 2)
    late_ms = db.execute("SELECT epoch_ms(event_timestamp) FROM lake.silver_fx_trade WHERE trade_id = 't2'").fetchone()
    assert late_ms == (int((NOW - timedelta(seconds=90)).timestamp() * 1000),)


def test_failed_batch_leaves_nothing_behind(tmp_path, monkeypatch):
    db = _lake(tmp_path)
    monkeypatch.setattr(lake, "SILVER_INSERT", "INSERT INTO lake.silver_fx_trade SELECT 1")
    with pytest.raises(Exception):
        write_batch(db, 0, decode_batch(_kafka([_trade(1)]), SCHEMA_ID))
    assert _counts(db) == (0, 0, 0)


def test_small_batches_are_inlined_until_flush(tmp_path):
    db = _lake(tmp_path, limit=100)
    write_batch(db, 0, decode_batch(_kafka([_trade(n) for n in range(50)]), SCHEMA_ID))
    assert _parquet_files(tmp_path) == 0
    db.execute("CALL ducklake_flush_inlined_data('lake')")
    assert _parquet_files(tmp_path) == 3  # one per table: bronze, silver, ledger


def test_inlining_off_writes_parquet_per_batch(tmp_path):
    db = _lake(tmp_path, limit=0)
    write_batch(db, 0, decode_batch(_kafka([_trade(n) for n in range(50)]), SCHEMA_ID))
    assert _parquet_files(tmp_path) == 3  # bronze, silver, ledger


def test_lake_config_names_the_missing_variable(monkeypatch):
    monkeypatch.delenv("F1_LAKE_CATALOG", raising=False)
    with pytest.raises(KeyError, match="F1_LAKE_CATALOG"):
        LakeConfig.from_env()


def test_partitions_are_utc_days(tmp_path):
    db = _lake(tmp_path, limit=0)
    write_batch(db, 0, decode_batch(_kafka([_trade(1, late_s=-11 * 3600)]), SCHEMA_ID))  # 2026-10-17 20:00 UTC
    silver_files = [path.as_posix() for path in (tmp_path / "data").rglob("*.parquet") if "silver" in path.as_posix()]
    assert len(silver_files) == 1 and "/year=2026/month=10/day=17/" in silver_files[0]


def test_compaction_merges_small_files_and_deletes_replaced_ones(tmp_path):
    db = _lake(tmp_path, limit=0)
    for batch_id in range(3):
        write_batch(db, batch_id, decode_batch(_kafka([_trade(batch_id)], first_offset=batch_id), SCHEMA_ID))
    assert compact.file_counts(db) == {"bronze_fx_trade": 3, "silver_fx_trade": 3, "ingest_batch": 3}

    compact.compact(db, keep=timedelta(0))
    assert compact.file_counts(db) == {"bronze_fx_trade": 1, "silver_fx_trade": 1, "ingest_batch": 1}
    assert _parquet_files(tmp_path) == 3  # the 9 replaced files are gone from storage
    assert _counts(db) == (3, 3, 3)


def test_compaction_leaves_files_at_target_size_alone(tmp_path, monkeypatch):
    db = _lake(tmp_path, limit=0)
    for batch_id in range(3):
        write_batch(db, batch_id, decode_batch(_kafka([_trade(batch_id)], first_offset=batch_id), SCHEMA_ID))
    monkeypatch.setattr(compact, "TARGET_FILE_BYTES", 1)  # every file is already "big"
    compact.compact(db, keep=timedelta(0))
    assert compact.file_counts(db) == {"bronze_fx_trade": 3, "silver_fx_trade": 3, "ingest_batch": 3}
