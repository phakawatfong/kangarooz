"""Synthetic FX market: random-walk mid rates, skewed users, log-normal trade sizes."""

from __future__ import annotations

import math
import random
import uuid
from datetime import datetime
from typing import get_args

from ..config import PAIRS, PairSpec, SimConfig
from ..domain import Side, Trade, to_decimal


def rng_uuid(rng: random.Random) -> uuid.UUID:
    return uuid.UUID(int=rng.getrandbits(128), version=4)


class MarketSimulator:
    def __init__(self, sim_config: SimConfig, rng: random.Random):
        self.sim_config = sim_config
        self.rng = rng
        self.mids = {p.pair: p.mid for p in PAIRS}
        self._pair_list = list(PAIRS)
        self._pair_weights = [p.weight for p in PAIRS]
        # Zipf-like user activity: a few "whales" trade a lot. They are NOT anomalies,
        # which is exactly what makes naive per-user volume rules produce false positives.
        self.users = [f"U{i:05d}" for i in range(sim_config.n_users)]
        self._user_weights = [1.0 / (i + 1) ** 1.1 for i in range(sim_config.n_users)]
        # Log-normal sizes with the configured mean.
        self._amount_sigma = 1.0
        self._amount_mu = math.log(sim_config.mean_base_amount) - self._amount_sigma**2 / 2

    def advance(self, dt: float) -> None:
        """Move every mid one geometric-random-walk step of dt seconds."""
        step = self.sim_config.volatility_per_sec * math.sqrt(dt)
        for pair, mid in self.mids.items():
            self.mids[pair] = mid * math.exp(step * self.rng.gauss(0.0, 1.0))

    def pick_pair(self) -> PairSpec:
        return self.rng.choices(self._pair_list, weights=self._pair_weights, k=1)[0]

    def pick_user(self) -> str:
        return self.rng.choices(self.users, weights=self._user_weights, k=1)[0]

    def make_trade(
        self,
        now: datetime,
        pair: PairSpec | None = None,
        user: str | None = None,
        rate_override: float | None = None,
        amount_multiplier: float = 1.0,
    ) -> Trade:
        spec = pair or self.pick_pair()
        mid = self.mids[spec.pair]
        # Tiny execution noise around the mid (~0.2 bp). Same convention for BUY and SELL.
        rate = rate_override if rate_override is not None else mid * (1 + self.rng.gauss(0.0, 0.00002))
        amount = self.rng.lognormvariate(self._amount_mu, self._amount_sigma) * amount_multiplier
        return Trade(
            trade_id=str(rng_uuid(self.rng)),
            user_id=user or self.pick_user(),
            currency_pair=spec.pair,
            side=self.rng.choice(get_args(Side)),
            rate=to_decimal(rate, spec.decimals),
            base_amount=to_decimal(amount, 2),
            event_timestamp=now,
        )
