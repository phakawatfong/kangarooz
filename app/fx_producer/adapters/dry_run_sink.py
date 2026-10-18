"""Stand-in for Kafka (--dry-run): prints ground truth and sample trades, measures Avro sizes."""

from __future__ import annotations

from contract.avro_codec import REGISTRY_HEADER_BYTES, TRADE_SCHEMA, encode
from ..domain import AnomalyEpisode, Trade


class DryRunSink:
    def __init__(self) -> None:
        self.sent = 0
        self.value_bytes = 0

    def send_episode(self, episode: AnomalyEpisode) -> None:
        print(f"[ground truth] {episode.anomaly_type:<14} {episode.scope}:{episode.scope_key} {episode.params}")

    def send_trade(self, trade: Trade) -> None:
        self.value_bytes += REGISTRY_HEADER_BYTES + len(encode(trade.model_dump(), TRADE_SCHEMA))
        if self.sent < 3:
            print(f"[sample trade] {trade.model_dump()}  quote_amount={trade.quote_amount}")
        self.sent += 1

    def close(self, elapsed: float) -> None:
        print(f"done (dry run): trades={self.sent} avg_value={self.value_bytes / max(self.sent, 1):.1f}B "
              f"(incl. {REGISTRY_HEADER_BYTES}B registry header)")
