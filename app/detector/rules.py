"""Alert rules as DataFrame -> DataFrame functions: the same code runs on the Kafka stream and on a test batch.

Every rule ends in `_alerts`, so every rule emits exactly the columns of schemas/fx_alert.avsc.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro, to_avro

from contract.avro_codec import ALERT_SCHEMA, REGISTRY_HEADER_BYTES, TRADE_SCHEMA, registry_header, schema_text

WATERMARK = "30 seconds"
LATE_AFTER_MS = 30_000

# ponytail: fixed threshold. The simulator draws every pair's size from one log-normal (mean 5000), so a per-pair
# table would hold the same number six times. Upgrade path: per-pair baseline from silver.
FAT_FINGER_TYPICAL_AMOUNT = 5000.0
FAT_FINGER_MULTIPLE = 20

ACCOUNT_BURST_WINDOW = "10 seconds"
ACCOUNT_BURST_MIN_TRADES = 50


def decode_trades(kafka: DataFrame, schema_id: int) -> DataFrame:
    """Kafka rows -> trade columns + arrived_at (the broker timestamp). Fails on a record not written with schema_id.

    OSS from_avro reads plain Avro, not the Confluent wire format, so the 5-byte header is checked and stripped here.
    """
    header = registry_header(schema_id)
    trade = (F.when(F.substring("value", 1, REGISTRY_HEADER_BYTES) != F.lit(header),
                    F.raise_error(F.lit(f"record not written with schema id {schema_id}")))
             .otherwise(from_avro(F.expr(f"substring(value, {REGISTRY_HEADER_BYTES + 1})"), schema_text(TRADE_SCHEMA))))
    return kafka.select(trade.alias("trade"), F.col("timestamp").alias("arrived_at")).select("trade.*", "arrived_at")


def _arrived_late() -> Column:
    """Judged per record against its own arrival time, so a replay judges every trade exactly as live did."""
    return (F.unix_millis("arrived_at") - F.unix_millis("event_timestamp")) > LATE_AFTER_MS


def _alerts(df: DataFrame, *, anomaly_type: str, scope: str, scope_key: Column, window_start: Column,
            window_end: Column, rule_version: int, is_late: Column, observed_value: Column,
            baseline: float | None, threshold: float, trigger_trade_id: Column | None) -> DataFrame:
    return df.select(
        F.sha2(F.concat_ws("|", F.lit(anomaly_type), scope_key, F.unix_millis(window_start).cast("string"),
                           F.lit(str(rule_version))), 256).alias("alert_id"),
        F.lit(anomaly_type).alias("anomaly_type"),
        F.lit(scope).alias("scope"),
        scope_key.alias("scope_key"),
        window_start.alias("window_start"),
        window_end.alias("window_end"),
        F.current_timestamp().alias("detected_at"),
        F.lit(rule_version).alias("rule_version"),
        is_late.alias("is_late"),
        observed_value.cast("double").alias("observed_value"),
        F.lit(baseline).cast("double").alias("baseline"),
        F.lit(float(threshold)).alias("threshold"),
        (trigger_trade_id if trigger_trade_id is not None else F.lit(None)).cast("string").alias("trigger_trade_id"),
    )


def fat_finger(trades: DataFrame, rule_version: int) -> DataFrame:
    """One trade far above the typical size. Stateless, so late trades are still evaluated (and flagged)."""
    threshold = FAT_FINGER_TYPICAL_AMOUNT * FAT_FINGER_MULTIPLE
    return _alerts(trades.where(F.col("base_amount") > threshold),
                   anomaly_type="fat_finger", scope="TRADE", scope_key=F.col("trade_id"),
                   window_start=F.col("event_timestamp"), window_end=F.col("event_timestamp"),
                   rule_version=rule_version, is_late=_arrived_late(), observed_value=F.col("base_amount"),
                   baseline=FAT_FINGER_TYPICAL_AMOUNT, threshold=threshold, trigger_trade_id=F.col("trade_id"))


def account_burst(trades: DataFrame, rule_version: int, slide: str | None = None) -> DataFrame:
    """Trades per user per event-time window. slide=None is tumbling; e.g. "2 seconds" is sliding.

    Run in update mode: a window is emitted at the first trigger where it crosses the threshold, then again each
    time its count grows. Every re-emission carries the same alert_id.

    Late trades are dropped per record before counting. The watermark alone drops them only between micro-batches,
    so a replay (one large batch) would count trades the live run dropped and fire extra alerts. The watermark is
    kept to evict old windows from state.
    """
    counts = (trades.where(~_arrived_late()).withWatermark("event_timestamp", WATERMARK)
              .groupBy(F.window("event_timestamp", ACCOUNT_BURST_WINDOW, slide), "user_id").count()
              .where(F.col("count") >= ACCOUNT_BURST_MIN_TRADES))
    return _alerts(counts, anomaly_type="account_burst", scope="USER", scope_key=F.col("user_id"),
                   window_start=F.col("window.start"), window_end=F.col("window.end"),
                   rule_version=rule_version, is_late=F.lit(False), observed_value=F.col("count"),
                   baseline=None, threshold=ACCOUNT_BURST_MIN_TRADES, trigger_trade_id=None)


def to_kafka(alerts: DataFrame, schema_id: int) -> DataFrame:
    """Alert rows -> Kafka key (alert_id) + value in the Confluent wire format (header + Avro body)."""
    body = to_avro(F.struct(*alerts.columns), schema_text(ALERT_SCHEMA))
    return alerts.select(F.col("alert_id").alias("key"), F.concat(F.lit(registry_header(schema_id)), body).alias("value"))
