"""Detector rules on batch DataFrames shaped like the Kafka source, and grading's matching."""

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from contract.avro_codec import ALERT_SCHEMA, REGISTRY_HEADER_BYTES, TRADE_SCHEMA, decode, encode, registry_header
from detector.grade import grade
from detector.rules import account_burst, decode_trades, fat_finger, to_kafka
from fx_producer.domain import Trade
from lakehouse.stream import spark_session

TRADE_ID, ALERT_ID = 7, 9
NOW = datetime(2026, 10, 27, 9, 0, 0, tzinfo=timezone.utc)


@pytest.fixture(scope="module")
def spark():
    return spark_session("f1-test")


def _trade(n: int, user: str = "U00001", amount: str = "1000", late_s: int = 0) -> Trade:
    return Trade(trade_id=f"t{n}", user_id=user, currency_pair="USD/THB", side="BUY", rate=Decimal("33.5"),
                 base_amount=Decimal(amount), event_timestamp=NOW + timedelta(milliseconds=n) - timedelta(seconds=late_s))


def _trades(spark, trades: list[Trade], arrived_at: datetime = NOW, schema_id: int = TRADE_ID):
    rows = [(bytearray(registry_header(schema_id) + encode(t.model_dump(), TRADE_SCHEMA)), arrived_at) for t in trades]
    return decode_trades(spark.createDataFrame(rows, "value binary, timestamp timestamp"), TRADE_ID)


def test_fat_finger_fires_above_20x_typical_and_flags_late_arrivals(spark):
    trades = [_trade(1, amount="99999"), _trade(2, amount="100001"), _trade(3, amount="500000", late_s=40)]
    alerts = {a.scope_key: a for a in fat_finger(_trades(spark, trades), rule_version=1).collect()}
    assert sorted(alerts) == ["t2", "t3"]
    assert (alerts["t2"].scope, alerts["t2"].trigger_trade_id, alerts["t2"].observed_value) == ("TRADE", "t2", 100001.0)
    assert (alerts["t2"].is_late, alerts["t3"].is_late) == (False, True)


def test_account_burst_fires_at_50_trades_per_window(spark):
    trades = [_trade(i, user="U1") for i in range(50)] + [_trade(100 + i, user="U2") for i in range(49)]
    alerts = account_burst(_trades(spark, trades), rule_version=1).collect()
    assert [(a.scope, a.scope_key, a.observed_value) for a in alerts] == [("USER", "U1", 50.0)]
    # collect() hands back naive local-time datetimes, whatever the session time zone.
    window = tuple(t.astimezone(timezone.utc) for t in (alerts[0].window_start, alerts[0].window_end))
    assert window == (NOW, NOW + timedelta(seconds=10))



def test_account_burst_ignores_trades_that_arrived_late(spark):
    on_time = _trades(spark, [_trade(i, user="U1") for i in range(49)])
    late = _trades(spark, [_trade(49, user="U1")], arrived_at=NOW + timedelta(seconds=31))
    assert account_burst(on_time.union(late), rule_version=1).collect() == []


def test_alert_id_is_stable_on_replay_and_changes_with_rule_version(spark):
    trades = _trades(spark, [_trade(1, amount="200000")])
    ids = [fat_finger(trades, version).collect()[0].alert_id for version in (1, 1, 2)]
    assert ids[0] == ids[1] != ids[2]


def test_kafka_value_is_wire_format_the_contract_decodes(spark):
    row = to_kafka(fat_finger(_trades(spark, [_trade(1, amount="200000")]), rule_version=3), ALERT_ID).collect()[0]
    value = bytes(row.value)
    assert value[:REGISTRY_HEADER_BYTES] == registry_header(ALERT_ID)
    alert = decode(value[REGISTRY_HEADER_BYTES:], ALERT_SCHEMA)
    assert alert["alert_id"] == row.key
    assert {k: alert[k] for k in ("anomaly_type", "scope", "scope_key", "rule_version", "is_late", "observed_value",
                                  "baseline", "threshold", "trigger_trade_id")} == {
        "anomaly_type": "fat_finger", "scope": "TRADE", "scope_key": "t1", "rule_version": 3, "is_late": False,
        "observed_value": 200000.0, "baseline": 5000.0, "threshold": 100000.0, "trigger_trade_id": "t1"}
    assert alert["window_start"] == NOW + timedelta(milliseconds=1)


def test_decode_rejects_other_schema_id(spark):
    with pytest.raises(Exception, match="record not written with schema id 7"):
        _trades(spark, [_trade(1)], schema_id=8).collect()


def _alert(alert_id: str, anomaly_type: str, key: str, start_s: int, end_s: int) -> dict:
    return {"alert_id": alert_id, "anomaly_type": anomaly_type, "scope_key": key,
            "window_start": NOW + timedelta(seconds=start_s), "window_end": NOW + timedelta(seconds=end_s)}


def _episode(anomaly_type: str, key: str, start_s: int, end_s: int) -> dict:
    return {"anomaly_type": anomaly_type, "scope_key": key,
            "start_time": NOW + timedelta(seconds=start_s), "end_time": NOW + timedelta(seconds=end_s)}


def test_grade_matches_type_key_and_overlapping_window():
    alerts = [_alert("a1", "account_burst", "U1", 0, 10), _alert("a2", "account_burst", "U1", 10, 20),
              _alert("a3", "account_burst", "U2", 0, 10), _alert("f1", "fat_finger", "t9", 5, 5)]
    episodes = [_episode("account_burst", "U1", 4, 14), _episode("account_burst", "U3", 0, 10),
                _episode("fat_finger", "t9", 5, 5), _episode("fat_finger", "t8", 5, 5)]
    assert grade(alerts, episodes) == {
        "account_burst": {"alerts": 3, "true_alerts": 2, "episodes": 2, "found_episodes": 1},
        "fat_finger": {"alerts": 1, "true_alerts": 1, "episodes": 2, "found_episodes": 1},
    }
