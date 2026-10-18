"""Run the synthetic FX producer.

    uv run python -m fx_producer.cli --rate 500 --duration 60
    uv run python -m fx_producer.cli --dry-run --duration 5     # no broker: print samples + sizes
"""

from __future__ import annotations

import argparse
import time
from collections import Counter
from datetime import datetime, timezone
from typing import Protocol, get_args

from .config import Connection, SimConfig
from .adapters.dry_run_sink import DryRunSink
from .domain import AnomalyEpisode, Trade
from .simulation.simulator import Simulator

TICK_SECONDS = 0.1


class EventSink(Protocol):
    """Where trades and ground truth go: Kafka (adapters.kafka_sink.KafkaSink) or the console (adapters.dry_run_sink.DryRunSink)."""

    sent: int
    value_bytes: int

    def send_episode(self, ep: AnomalyEpisode) -> None: ...
    def send_trade(self, trade: Trade) -> None: ...
    def close(self, elapsed: float) -> None: ...


def parse_args(argv: list[str] | None = None) -> tuple[SimConfig, argparse.Namespace]:
    p = argparse.ArgumentParser(description="F1 synthetic FX trade producer (Phase 1)")
    for name, field in SimConfig.model_fields.items():
        typ = (get_args(field.annotation) or [field.annotation])[0]  # `int | None` -> int
        p.add_argument("--" + name.replace("_", "-"), type=typ, default=field.default, help=f"default: {field.default}")
    p.add_argument("--dry-run", action="store_true", help="don't connect to Kafka; print samples and Avro sizes")
    args = p.parse_args(argv)
    cfg = SimConfig(**{name: getattr(args, name) for name in SimConfig.model_fields})
    return cfg, args


def run(sim: Simulator, sink: EventSink) -> None:
    """Drive the simulator in real time, TICK_SECONDS per step, until duration elapses or Ctrl+C."""
    cfg = sim.cfg
    episode_counts: Counter[str] = Counter()
    start = time.monotonic()
    next_report = start + 5.0
    try:
        while True:
            loop_start = time.monotonic()
            if cfg.duration > 0 and loop_start - start >= cfg.duration:
                break
            trades, episodes = sim.step(datetime.now(timezone.utc), TICK_SECONDS)
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


def main(argv: list[str] | None = None) -> None:
    cfg, args = parse_args(argv)
    sink: EventSink
    if args.dry_run:
        sink = DryRunSink()
    else:
        from .adapters.kafka_sink import KafkaSink  # imported lazily so --dry-run works without a broker

        sink = KafkaSink(Connection.from_env())
    print(f"producing ~{cfg.rate:.0f} trades/s for {cfg.duration:.0f}s "
          f"({'dry run' if args.dry_run else 'kafka'}); Ctrl+C to stop")
    run(Simulator(cfg), sink)

if __name__ == "__main__":
    main()
