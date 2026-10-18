"""One simulation tick: move the market, generate normal flow, inject anomalies, apply chaos."""

from __future__ import annotations

import random
from datetime import datetime

from .anomalies import AnomalyInjector
from .chaos import apply_chaos
from ..config import SimConfig
from .market import MarketSimulator
from ..domain import AnomalyEpisode, Trade


class Simulator:
    """Market, injector and chaos draw from one seeded RNG, so a seed reproduces a run exactly."""

    def __init__(self, cfg: SimConfig):
        self.cfg = cfg
        self.rng = random.Random(cfg.seed)
        self.market = MarketSimulator(cfg, self.rng)
        self.injector = AnomalyInjector(cfg, self.market, self.rng)
        self._carry = 0.0

    def step(self, now: datetime, dt: float) -> tuple[list[Trade], list[AnomalyEpisode]]:
        """Return (trades to send, newly started ground-truth episodes) for dt seconds of market time."""
        self.market.advance(dt)
        self._carry += self.cfg.rate * dt
        n = int(self._carry)
        self._carry -= n
        trades = [self.market.make_trade(now) for _ in range(n)]
        trades, episodes = self.injector.tick(now, dt, trades)
        return apply_chaos(trades, self.cfg, self.rng), episodes
