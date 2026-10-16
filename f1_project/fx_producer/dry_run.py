"""Stand-in for Kafka (--dry-run): prints ground truth and sample trades, measures Avro sizes."""

from __future__ import annotations

from .avro_local import REGISTRY_HEADER_BYTES, encode
from .config import TRADE_SCHEMA
from .model import AnomalyEpisode, Trade


class DryRunSink:
    def __init__(self) -> None:
        self.sent = 0
        self.value_bytes = 0

    def send_episode(self, ep: AnomalyEpisode) -> None:
        print(f"[ground truth] {ep.anomaly_type:<14} {ep.scope}:{ep.scope_key} {ep.params}")

    def send_trade(self, trade: Trade) -> None:
        self.value_bytes += REGISTRY_HEADER_BYTES + len(encode(trade.model_dump(), TRADE_SCHEMA))
        if self.sent < 3:
            print(f"[sample trade] {trade.model_dump()}  quote_amount={trade.quote_amount}")
        self.sent += 1

    def close(self, elapsed: float) -> None:
        print(f"done (dry run): trades={self.sent} avg_value={self.value_bytes / max(self.sent, 1):.1f}B "
              f"(incl. {REGISTRY_HEADER_BYTES}B registry header)")
