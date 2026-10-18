"""Anomaly injection = the simulator's answer key.

Every injected anomaly produces an AnomalyEpisode (ground truth) that goes to a SEPARATE
topic, so the detector can never read the labels. Two kinds:
- point anomalies (scope TRADE): price_spike, fat_finger -> one bad trade
- collective anomalies (scope USER / PAIR): volume_burst, account_burst -> every trade looks
  normal on its own; only the group over a window is abnormal
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import datetime, timedelta

from ..config import PairSpec, SimConfig
from .market import MarketSimulator, rng_uuid
from ..domain import AnomalyEpisode, Trade


@dataclass
class _Burst:
    episode: AnomalyEpisode
    per_sec: float
    pair: PairSpec | None = None
    user: str | None = None
    carry: float = 0.0


class AnomalyInjector:
    def __init__(self, sim_config: SimConfig, market: MarketSimulator, rng: random.Random):
        self.sim_config = sim_config
        self.market = market
        self.rng = rng
        self.active: list[_Burst] = []

    def _hit(self, per_min: float, dt: float) -> bool:
        return per_min > 0 and self.rng.random() < per_min * dt / 60.0

    def _new_id(self) -> str:
        return f"E-{rng_uuid(self.rng).hex[:12]}"

    # ---------- point anomalies ----------
    def price_spike(self, now: datetime) -> tuple[Trade, AnomalyEpisode]:
        """One trade printed 1-3% away from the mid (huge for FX). The mid itself doesn't move."""
        spec = self.market.pick_pair()
        deviation = self.rng.uniform(0.01, 0.03) * self.rng.choice((-1, 1))
        trade = self.market.make_trade(now, pair=spec, rate_override=self.market.mids[spec.pair] * (1 + deviation))
        episode = AnomalyEpisode(
            episode_id=self._new_id(), anomaly_type="price_spike", scope="TRADE", scope_key=trade.trade_id,
            start_time=now, end_time=now,
            params={"deviation_pct": f"{deviation * 100:.2f}", "currency_pair": spec.pair},
        )
        return trade, episode

    def fat_finger(self, now: datetime) -> tuple[Trade, AnomalyEpisode]:
        """One trade with a 50-200x normal size."""
        mult = self.rng.uniform(50, 200)
        trade = self.market.make_trade(now, amount_multiplier=mult)
        episode = AnomalyEpisode(
            episode_id=self._new_id(), anomaly_type="fat_finger", scope="TRADE", scope_key=trade.trade_id,
            start_time=now, end_time=now,
            params={"multiplier": f"{mult:.1f}", "currency_pair": trade.currency_pair},
        )
        return trade, episode

    # ---------- collective anomalies ----------
    def start_volume_burst(self, now: datetime) -> AnomalyEpisode:
        """Extra flow on one pair: 5-10x its normal rate for 10-30s."""
        spec = self.market.pick_pair()
        duration = self.rng.uniform(10, 30)
        mult = self.rng.uniform(5, 10)
        per_sec = max(20.0, self.sim_config.rate * spec.weight * mult)
        episode = AnomalyEpisode(
            episode_id=self._new_id(), anomaly_type="volume_burst", scope="PAIR", scope_key=spec.pair,
            start_time=now, end_time=now + timedelta(seconds=duration),
            params={"multiplier": f"{mult:.1f}", "trades_per_sec": f"{per_sec:.0f}", "duration_s": f"{duration:.0f}"},
        )
        self.active.append(_Burst(episode, per_sec, pair=spec))
        return episode

    def start_account_burst(self, now: datetime) -> AnomalyEpisode:
        """One (usually ordinary) user suddenly sends 10-30 trades/sec for 10s."""
        user = self.rng.choice(self.market.users)  # uniform, not weighted -> mostly non-whales
        per_sec = self.rng.uniform(10, 30)
        duration = 10.0
        episode = AnomalyEpisode(
            episode_id=self._new_id(), anomaly_type="account_burst", scope="USER", scope_key=user,
            start_time=now, end_time=now + timedelta(seconds=duration),
            params={"trades_per_sec": f"{per_sec:.0f}", "duration_s": f"{duration:.0f}"},
        )
        self.active.append(_Burst(episode, per_sec, user=user))
        return episode

    # ---------- per tick ----------
    def tick(self, now: datetime, dt: float, trades: list[Trade]) -> tuple[list[Trade], list[AnomalyEpisode]]:
        """Return (trades incl. injected ones, newly started episodes)."""
        out = list(trades)
        episodes: list[AnomalyEpisode] = []
        if self._hit(self.sim_config.price_spike_per_min, dt):
            trade, episode = self.price_spike(now)
            out.append(trade)
            episodes.append(episode)
        if self._hit(self.sim_config.fat_finger_per_min, dt):
            trade, episode = self.fat_finger(now)
            out.append(trade)
            episodes.append(episode)
        if self._hit(self.sim_config.volume_burst_per_min, dt):
            episodes.append(self.start_volume_burst(now))
        if self._hit(self.sim_config.account_burst_per_min, dt):
            episodes.append(self.start_account_burst(now))

        still_active = []
        for burst in self.active:
            if now >= burst.episode.end_time:
                continue
            burst.carry += burst.per_sec * dt
            n = int(burst.carry)
            burst.carry -= n
            for _ in range(n):
                out.append(self.market.make_trade(now, pair=burst.pair, user=burst.user))
            still_active.append(burst)
        self.active = still_active
        return out, episodes
