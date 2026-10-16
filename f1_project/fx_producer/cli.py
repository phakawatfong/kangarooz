"""Run the synthetic FX producer.

    uv run python -m fx_producer.cli --rate 500 --duration 60
    uv run python -m fx_producer.cli --dry-run --duration 5     # no broker: print samples + sizes
"""

from __future__ import annotations

import argparse
import random
import time
from collections import Counter
from dataclasses import fields
from datetime import datetime, timezone
from typing import Protocol

from .anomalies import AnomalyInjector
from .chaos import apply_chaos
from .config import SimConfig
from .dry_run import DryRunSink
from .market import MarketSimulator
from .model import AnomalyEpisode, Trade

TICK_SECONDS = 0.1


class EventSink(Protocol):
    """Where trades and ground truth go: Kafka (kafka_io.FxProducer) or the console (dry_run.DryRunSink)."""

    sent: int
    value_bytes: int

    def send_episode(self, ep: AnomalyEpisode) -> None: ...
    def send_trade(self, trade: Trade) -> None: ...
    def close(self, elapsed: float) -> None: ...


def parse_args(argv: list[str] | None = None) -> tuple[SimConfig, argparse.Namespace]:
    defaults = SimConfig()
    p = argparse.ArgumentParser(description="F1 synthetic FX trade producer (Phase 1)")
    for f in fields(SimConfig):
        default = getattr(defaults, f.name)
        flag = "--" + f.name.replace("_", "-")
        typ = int if f.name in ("n_users", "seed") else float
        p.add_argument(flag, type=typ, default=default, help=f"default: {default}")
    p.add_argument("--dry-run", action="store_true", help="don't connect to Kafka; print samples and Avro sizes")
    args = p.parse_args(argv)
    cfg = SimConfig(**{f.name: getattr(args, f.name) for f in fields(SimConfig)})
    return cfg, args


def main(argv: list[str] | None = None) -> None:
    cfg, args = parse_args(argv)
    rng = random.Random(cfg.seed)
    market = MarketSimulator(cfg, rng)
    injector = AnomalyInjector(cfg, market, rng)

    sink: EventSink
    if args.dry_run:
        sink = DryRunSink()
    else:
        from .kafka_io import FxProducer  # imported lazily so --dry-run works without a broker

        sink = FxProducer()

    episode_counts: Counter[str] = Counter()
    carry = 0.0
    start = time.monotonic()
    next_report = start + 5.0
    print(f"producing ~{cfg.rate:.0f} trades/s for {cfg.duration:.0f}s "
          f"({'dry run' if args.dry_run else 'kafka'}); Ctrl+C to stop")
    try:
        while True:
            loop_start = time.monotonic()
            if cfg.duration > 0 and loop_start - start >= cfg.duration:
                break
            now = datetime.now(timezone.utc)
            market.advance(TICK_SECONDS)

            carry += cfg.rate * TICK_SECONDS
            n = int(carry)
            carry -= n
            trades = [market.make_trade(now) for _ in range(n)]
            trades, episodes = injector.tick(now, TICK_SECONDS, trades)
            trades = apply_chaos(trades, cfg, rng)

            for ep in episodes:
                episode_counts[ep.anomaly_type] += 1
                sink.send_episode(ep)
            for t in trades:
                sink.send_trade(t)

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

if __name__ == "__main__":
    main()
