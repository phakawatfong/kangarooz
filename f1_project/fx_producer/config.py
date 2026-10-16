"""Static configuration: currency pairs, defaults, connection settings."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

# Repo root = .../f1_project (outer). This file lives in f1_project/f1_project/fx_producer/.
REPO_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_DIR = Path(os.getenv("F1_SCHEMA_DIR", REPO_ROOT / "schemas"))

TRADES_TOPIC = "fx.trades"
GROUND_TRUTH_TOPIC = "fx.trades.ground_truth"
TRADE_SCHEMA = "fx_trade.avsc"
EPISODE_SCHEMA = "anomaly_episode.avsc"


@dataclass(frozen=True)
class PairSpec:
    """A currency pair in market convention (base/quote).

    mid: synthetic starting mid rate (quote per 1 base). Not real market data.
    weight: share of total trade flow (USD/SGD dominates on purpose: hot-pair realism).
    decimals: quoting precision (pips); JPY pairs quote fewer decimals.
    """

    pair: str
    mid: float
    weight: float
    decimals: int


PAIRS: tuple[PairSpec, ...] = (
    PairSpec("USD/SGD", 1.3000, 0.30, 5),
    PairSpec("EUR/USD", 1.1000, 0.25, 5),
    PairSpec("USD/THB", 33.500, 0.15, 4),
    PairSpec("USD/JPY", 148.00, 0.12, 3),
    PairSpec("GBP/USD", 1.3000, 0.10, 5),
    PairSpec("AUD/USD", 0.6600, 0.08, 5),
)


@dataclass(frozen=True)
class Connection:
    bootstrap_servers: str = os.getenv("F1_BOOTSTRAP", "localhost:19092")
    schema_registry_url: str = os.getenv("F1_SCHEMA_REGISTRY", "http://localhost:18081")


@dataclass
class SimConfig:
    """Knobs for the simulator. Defaults = laptop dev profile from the Phase 1 sizing card."""

    rate: float = 500.0             # trades/sec sustained (dev profile; prod target is 1-5k/s)
    duration: float = 60.0          # seconds; <= 0 means run until Ctrl+C
    n_users: int = 1000
    volatility_per_sec: float = 0.00005  # ~0.5 bp/sec random walk on the mid
    mean_base_amount: float = 5000.0
    # Chaos (tests late-data handling and idempotent sinks)
    late_pct: float = 0.02          # share of trades with an old event_timestamp
    duplicate_pct: float = 0.005    # share of trades sent twice (same trade_id)
    # Anomaly injection: expected episodes per minute
    price_spike_per_min: float = 2.0
    fat_finger_per_min: float = 1.0
    volume_burst_per_min: float = 0.5
    account_burst_per_min: float = 0.5
    seed: int | None = None
