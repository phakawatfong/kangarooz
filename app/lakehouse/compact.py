"""Compaction job: merge small Parquet files into big ones, then delete the files they replaced.

    uv run python -m lakehouse.compact                    # one run (what a scheduler calls, e.g. hourly)
    uv run python -m lakehouse.compact --every-min 60     # keep running next to the stream
    uv run python -m lakehouse.compact --keep-hours 0     # delete replaced files right away (dev)

Safe to run while the stream writes: each step is its own catalog transaction.
"""

from __future__ import annotations

import argparse
import logging
import time
from datetime import datetime, timedelta, timezone

import duckdb

from logs import setup_logging

from .config import LakeConfig
from .lake import connect

# Files at least this big are never rewritten, so a run only touches small files (bounded write amplification).
TARGET_FILE_BYTES = 128 * 1024 * 1024 # 1GB
TABLES = ("bronze_fx_trade", "silver_fx_trade", "ingest_batch")

log = logging.getLogger(__name__)


def file_counts(db: duckdb.DuckDBPyConnection) -> dict[str, int]:
    return {table: db.execute(f"SELECT count(*) FROM ducklake_list_files('lake', '{table}')").fetchone()[0]
            for table in TABLES}


def compact(db: duckdb.DuckDBPyConnection, keep: timedelta) -> None:
    """Merge small files, expire snapshots older than `keep` (time travel window), delete unreferenced files."""
    started = time.monotonic()
    before = file_counts(db)
    db.execute("CALL ducklake_merge_adjacent_files('lake', max_file_size => ?)", [TARGET_FILE_BYTES])
    merged = time.monotonic()
    cutoff = datetime.now(timezone.utc) - keep
    db.execute("CALL ducklake_expire_snapshots('lake', older_than => ?)", [cutoff])
    deleted = len(db.execute("CALL ducklake_cleanup_old_files('lake', older_than => ?)", [cutoff]).fetchall())
    log.debug("merge %.1f s, expire + cleanup %.1f s", merged - started, time.monotonic() - merged)
    print(f"compacted in {time.monotonic() - started:.1f} s: files {before} -> {file_counts(db)}, "
          f"deleted {deleted} replaced files older than {cutoff:%Y-%m-%d %H:%M} UTC", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Merge small lake files and delete the ones they replaced")
    parser.add_argument("--keep-hours", type=float, default=24,
                        help="keep replaced files and snapshots this long for time travel (default: %(default)s)")
    parser.add_argument("--every-min", type=float, default=0,
                        help="repeat every N minutes; 0 = run once (default: %(default)s)")
    args = parser.parse_args()
    setup_logging()
    db = connect(LakeConfig.from_env())
    while True:
        compact(db, timedelta(hours=args.keep_hours))
        if not args.every_min:
            return
        time.sleep(args.every_min * 60)


if __name__ == "__main__":
    main()
