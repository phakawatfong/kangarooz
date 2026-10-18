"""Run the synthetic FX producer.

    uv run python -m fx_producer.produce --rate 500 --duration 60
    uv run python -m fx_producer.produce --dry-run --duration 5     # no broker: print samples + sizes
"""

from __future__ import annotations

import argparse
import logging
import time
from collections import Counter
from datetime import datetime, timezone
from typing import Protocol

from logs import setup_logging

from contract.kafka import KafkaConfig
from .adapters.dry_run_sink import DryRunSink
from .config import SimConfig
from .domain import AnomalyEpisode, Trade
from .simulation.simulator import Simulator

TICK_SECONDS = 0.1

log = logging.getLogger(__name__)


class EventSink(Protocol):
    """Where trades and ground truth go: Kafka (adapters.kafka_sink.KafkaSink) or the console (adapters.dry_run_sink.DryRunSink)."""

    sent: int
    value_bytes: int

    def send_episode(self, episode: AnomalyEpisode) -> None: ...
    def send_trade(self, trade: Trade) -> None: ...
    def close(self, elapsed: float) -> None: ...


def parse_args(argv: list[str] | None = None) -> tuple[SimConfig, bool]:
    """Return (simulator settings, dry_run). Flag defaults come from SimConfig."""
    default = SimConfig()
    parser = argparse.ArgumentParser(description="F1 synthetic FX trade producer (Phase 1)")
    parser.add_argument("--rate", type=float, default=default.rate, help="trades per second (default: %(default)s)")
    parser.add_argument("--duration", type=float, default=default.duration,
                        help="seconds to run; 0 = until Ctrl+C (default: %(default)s)")
    parser.add_argument("--n-users", type=int, default=default.n_users, help="distinct users (default: %(default)s)")
    parser.add_argument("--volatility-per-sec", type=float, default=default.volatility_per_sec,
                        help="random-walk step of the mid per second (default: %(default)s)")
    parser.add_argument("--mean-base-amount", type=float, default=default.mean_base_amount,
                        help="average trade size in base currency (default: %(default)s)")
    parser.add_argument("--late-pct", type=float, default=default.late_pct,
                        help="share of trades with an old event time (default: %(default)s)")
    parser.add_argument("--duplicate-pct", type=float, default=default.duplicate_pct,
                        help="share of trades sent twice (default: %(default)s)")
    parser.add_argument("--price-spike-per-min", type=float, default=default.price_spike_per_min,
                        help="injected price spikes per minute (default: %(default)s)")
    parser.add_argument("--fat-finger-per-min", type=float, default=default.fat_finger_per_min,
                        help="injected oversized trades per minute (default: %(default)s)")
    parser.add_argument("--volume-burst-per-min", type=float, default=default.volume_burst_per_min,
                        help="injected pair volume bursts per minute (default: %(default)s)")
    parser.add_argument("--account-burst-per-min", type=float, default=default.account_burst_per_min,
                        help="injected single-user bursts per minute (default: %(default)s)")
    parser.add_argument("--seed", type=int, default=default.seed,
                        help="random seed; same seed = same trades (default: %(default)s)")
    parser.add_argument("--dry-run", action="store_true", help="don't connect to Kafka; print samples and Avro sizes")
    args = parser.parse_args(argv)
    sim_config = SimConfig(
        rate=args.rate,
        duration=args.duration,
        n_users=args.n_users,
        volatility_per_sec=args.volatility_per_sec,
        mean_base_amount=args.mean_base_amount,
        late_pct=args.late_pct,
        duplicate_pct=args.duplicate_pct,
        price_spike_per_min=args.price_spike_per_min,
        fat_finger_per_min=args.fat_finger_per_min,
        volume_burst_per_min=args.volume_burst_per_min,
        account_burst_per_min=args.account_burst_per_min,
        seed=args.seed,
    )
    return sim_config, args.dry_run


def run(simulator: Simulator, sink: EventSink) -> None:
    """Drive the simulator in real time, TICK_SECONDS per step, until duration elapses or Ctrl+C."""
    sim_config = simulator.sim_config
    episode_counts: Counter[str] = Counter()
    start = time.monotonic()
    next_report = start + 5.0
    try:
        while True:
            loop_start = time.monotonic()
            if sim_config.duration > 0 and loop_start - start >= sim_config.duration:
                break
            trades, episodes = simulator.step(datetime.now(timezone.utc), TICK_SECONDS)
            for episode in episodes:
                log.debug("anomaly %s on %s:%s %s", episode.anomaly_type, episode.scope, episode.scope_key,
                          episode.params)
                episode_counts[episode.anomaly_type] += 1
                sink.send_episode(episode)
            for trade in trades:
                sink.send_trade(trade)

            if loop_start >= next_report:
                elapsed = loop_start - start
                print(f"t={elapsed:5.1f}s sent={sink.sent:>8} ({sink.sent / elapsed:6.0f}/s) "
                      f"avg_value={sink.value_bytes / max(sink.sent, 1):5.1f}B episodes={dict(episode_counts)}")
                next_report += 5.0
            time.sleep(max(0.0, TICK_SECONDS - (time.monotonic() - loop_start)))
    except KeyboardInterrupt:
        print("\nstopping...")
    finally:
        sink.close(max(time.monotonic() - start, 1e-9))
        print(f"ground-truth episodes: {dict(episode_counts)}")


def main(argv: list[str] | None = None) -> None:
    setup_logging()
    sim_config, dry_run = parse_args(argv)
    log.debug("simulator settings: %s", sim_config)
    sink: EventSink
    if dry_run:
        sink = DryRunSink()
    else:
        from .adapters.kafka_sink import KafkaSink  # imported lazily so --dry-run works without a broker

        sink = KafkaSink(KafkaConfig.from_env())
    print(f"producing ~{sim_config.rate:.0f} trades/s for {sim_config.duration:.0f}s "
          f"({'dry run' if dry_run else 'kafka'}); Ctrl+C to stop")
    run(Simulator(sim_config), sink)


if __name__ == "__main__":
    main()
