"""Grade fx.alerts against the ground truth: precision/recall per rule, latency, replay/duplicate counts.

    uv run python -m detector.grade [--rule-version 1]

Reads all of fx.alerts, fx.trades.ground_truth and fx.trades, so grade a fresh run (`make reset` first).
Latency (D4): the triggering trade's Kafka timestamp -> the alert's Kafka timestamp (first emission).
"""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import datetime

from confluent_kafka import Message
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

from contract.kafka import ALERTS_TOPIC, GROUND_TRUTH_TOPIC, TRADES_TOPIC, KafkaConfig, consume

from .rules import ACCOUNT_BURST_MIN_TRADES, LATE_AFTER_MS

RULES = ("fat_finger", "account_burst")


def _overlaps(alert: dict, episode: dict) -> bool:
    return alert["window_start"] <= episode["end_time"] and episode["start_time"] <= alert["window_end"]


def grade(alerts: list[dict], episodes: list[dict]) -> dict[str, dict[str, int]]:
    """Match distinct alerts to episodes on anomaly_type + scope_key + overlapping windows, per rule."""
    episodes_by_key = defaultdict(list)
    for episode in episodes:
        episodes_by_key[episode["anomaly_type"], episode["scope_key"]].append(episode)
    result = {}
    for rule in RULES:
        rule_alerts = [a for a in alerts if a["anomaly_type"] == rule]
        found = set()
        true_alerts = 0
        for alert in rule_alerts:
            hits = [id(e) for e in episodes_by_key[rule, alert["scope_key"]] if _overlaps(alert, e)]
            true_alerts += bool(hits)
            found.update(hits)
        result[rule] = {"alerts": len(rule_alerts), "true_alerts": true_alerts,
                        "episodes": sum(e["anomaly_type"] == rule for e in episodes), "found_episodes": len(found)}
    return result


def _millis(message: Message) -> int:
    return message.timestamp()[1]


def _pct(values: list[int], q: float) -> int:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(q * len(ordered)))]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rule-version", type=int, default=1)
    parser.add_argument("--since", type=datetime.fromisoformat,
                        help="latency only for trades that arrived after this (e.g. 2026-10-27T01:05+07:00): "
                             "skips backlog catch-up, which measures replay speed, not latency")
    args = parser.parse_args()
    since_ms = int(args.since.timestamp() * 1000) if args.since else 0
    kafka_config = KafkaConfig.from_env()
    deserializer = AvroDeserializer(SchemaRegistryClient({"url": kafka_config.schema_registry_url}))

    def read(topic: str) -> list[tuple[dict, int]]:
        return [(deserializer(m.value(), SerializationContext(topic, MessageField.VALUE)), _millis(m))
                for m in consume(kafka_config, topic, max_messages=10**8, idle_timeout=3.0)]

    alert_records = read(ALERTS_TOPIC)
    episodes = [episode for episode, _ in read(GROUND_TRUTH_TOPIC)]
    trades = read(TRADES_TOPIC)
    print(f"read {len(alert_records)} alert records, {len(episodes)} episodes, {len(trades)} trades")

    # Q3: replays and update-mode re-emissions add records, never new ids.
    ids_by_version = defaultdict(set)
    for alert, _ in alert_records:
        ids_by_version[alert["rule_version"]].add(alert["alert_id"])
    for version in sorted(ids_by_version):
        for rule in RULES:
            ids = [a["alert_id"] for a, _ in alert_records if a["rule_version"] == version and a["anomaly_type"] == rule]
            print(f"rule_version {version} {rule:<14} records={len(ids):<6} distinct alert_ids={len(set(ids))}")
        shared = sum(len(ids_by_version[version] & ids) for other, ids in ids_by_version.items() if other != version)
        print(f"rule_version {version} ids shared with other versions: {shared}")

    # Same key -> same partition -> read in produce order, so the first record per alert_id is the first emission.
    first: dict[str, tuple[dict, int]] = {}
    for alert, arrived in alert_records:
        if alert["rule_version"] == args.rule_version:
            first.setdefault(alert["alert_id"], (alert, arrived))
    alerts = [alert for alert, _ in first.values()]

    print(f"\n== rule_version {args.rule_version}: {len(alerts)} distinct alerts")
    for rule, counts in grade(alerts, episodes).items():
        precision = counts["true_alerts"] / counts["alerts"] if counts["alerts"] else float("nan")
        recall = counts["found_episodes"] / counts["episodes"] if counts["episodes"] else float("nan")
        print(f"{rule:<14} precision={precision:.2f} ({counts['true_alerts']}/{counts['alerts']})  "
              f"recall={recall:.2f} ({counts['found_episodes']}/{counts['episodes']})")

    arrival_by_trade: dict[str, int] = {}
    trades_by_user: dict[str, list[tuple[int, int]]] = defaultdict(list)
    for trade, arrived in trades:
        arrival_by_trade.setdefault(trade["trade_id"], arrived)
        trades_by_user[trade["user_id"]].append((int(trade["event_timestamp"].timestamp() * 1000), arrived))

    def triggered_at(alert: dict) -> int | None:
        """Kafka timestamp of the trade that made the rule true."""
        if alert["anomaly_type"] == "fat_finger":
            return arrival_by_trade.get(alert["trigger_trade_id"])
        start, end = (int(alert[k].timestamp() * 1000) for k in ("window_start", "window_end"))
        # Same per-record late rule as the detector.
        counted = sorted(arrived for event, arrived in trades_by_user[alert["scope_key"]]
                         if start <= event < end and arrived - event <= LATE_AFTER_MS)
        return counted[ACCOUNT_BURST_MIN_TRADES - 1] if len(counted) >= ACCOUNT_BURST_MIN_TRADES else None

    latency, batch_gap = defaultdict(list), defaultdict(list)
    for alert, arrived in first.values():
        trigger = triggered_at(alert)
        if trigger is not None and trigger >= since_ms:
            latency[alert["anomaly_type"]].append(arrived - trigger)
        batch_gap[alert["anomaly_type"]].append(arrived - int(alert["detected_at"].timestamp() * 1000))
    print("\nlatency, trade arrival -> alert arrival (ms); target p95 < 5000")
    for rule in RULES:
        if latency[rule]:
            print(f"{rule:<14} n={len(latency[rule]):<5} p50={_pct(latency[rule], 0.5):<6} "
                  f"p95={_pct(latency[rule], 0.95):<6} max={max(latency[rule]):<6} "
                  f"(alert arrival - detected_at: p95={_pct(batch_gap[rule], 0.95)})")
    print(f"\nlate fat_finger alerts: {sum(a['is_late'] for a in alerts if a['anomaly_type'] == 'fat_finger')}")
    print(f"top account_burst users: {Counter(a['scope_key'] for a in alerts if a['anomaly_type'] == 'account_burst').most_common(5)}")


if __name__ == "__main__":
    main()
