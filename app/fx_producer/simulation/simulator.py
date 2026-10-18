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

    def __init__(self, sim_config: SimConfig):
        self.sim_config = sim_config
        self.rng = random.Random(sim_config.seed)
        self.market = MarketSimulator(sim_config, self.rng)
        self.injector = AnomalyInjector(sim_config, self.market, self.rng)
        self._carry = 0.0

    def step(self, now: datetime, dt: float) -> tuple[list[Trade], list[AnomalyEpisode]]:
        """Return (trades to send, newly started ground-truth episodes) for dt seconds of market time."""
        self.market.advance(dt)
        self._carry += self.sim_config.rate * dt
        n = int(self._carry)
        self._carry -= n
        trades = [self.market.make_trade(now) for _ in range(n)]
        trades, episodes = self.injector.tick(now, dt, trades)
        return apply_chaos(trades, self.sim_config, self.rng), episodes
