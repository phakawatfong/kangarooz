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
    def __init__(self, cfg: SimConfig, market: MarketSimulator, rng: random.Random):
        self.cfg = cfg
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
        ep = AnomalyEpisode(
            episode_id=self._new_id(), anomaly_type="price_spike", scope="TRADE", scope_key=trade.trade_id,
            start_time=now, end_time=now,
            params={"deviation_pct": f"{deviation * 100:.2f}", "currency_pair": spec.pair},
        )
        return trade, ep

    def fat_finger(self, now: datetime) -> tuple[Trade, AnomalyEpisode]:
        """One trade with a 50-200x normal size."""
        mult = self.rng.uniform(50, 200)
        trade = self.market.make_trade(now, amount_multiplier=mult)
        ep = AnomalyEpisode(
            episode_id=self._new_id(), anomaly_type="fat_finger", scope="TRADE", scope_key=trade.trade_id,
            start_time=now, end_time=now,
            params={"multiplier": f"{mult:.1f}", "currency_pair": trade.currency_pair},
        )
        return trade, ep

    # ---------- collective anomalies ----------
    def start_volume_burst(self, now: datetime) -> AnomalyEpisode:
        """Extra flow on one pair: 5-10x its normal rate for 10-30s."""
        spec = self.market.pick_pair()
        duration = self.rng.uniform(10, 30)
        mult = self.rng.uniform(5, 10)
        per_sec = max(20.0, self.cfg.rate * spec.weight * mult)
        ep = AnomalyEpisode(
            episode_id=self._new_id(), anomaly_type="volume_burst", scope="PAIR", scope_key=spec.pair,
            start_time=now, end_time=now + timedelta(seconds=duration),
            params={"multiplier": f"{mult:.1f}", "trades_per_sec": f"{per_sec:.0f}", "duration_s": f"{duration:.0f}"},
        )
        self.active.append(_Burst(ep, per_sec, pair=spec))
        return ep

    def start_account_burst(self, now: datetime) -> AnomalyEpisode:
        """One (usually ordinary) user suddenly sends 10-30 trades/sec for 10s."""
        user = self.rng.choice(self.market.users)  # uniform, not weighted -> mostly non-whales
        per_sec = self.rng.uniform(10, 30)
        duration = 10.0
        ep = AnomalyEpisode(
            episode_id=self._new_id(), anomaly_type="account_burst", scope="USER", scope_key=user,
            start_time=now, end_time=now + timedelta(seconds=duration),
            params={"trades_per_sec": f"{per_sec:.0f}", "duration_s": f"{duration:.0f}"},
        )
        self.active.append(_Burst(ep, per_sec, user=user))
        return ep

    # ---------- per tick ----------
    def tick(self, now: datetime, dt: float, trades: list[Trade]) -> tuple[list[Trade], list[AnomalyEpisode]]:
        """Return (trades incl. injected ones, newly started episodes)."""
        out = list(trades)
        episodes: list[AnomalyEpisode] = []
        c = self.cfg
        if self._hit(c.price_spike_per_min, dt):
            t, ep = self.price_spike(now)
            out.append(t)
            episodes.append(ep)
        if self._hit(c.fat_finger_per_min, dt):
            t, ep = self.fat_finger(now)
            out.append(t)
            episodes.append(ep)
        if self._hit(c.volume_burst_per_min, dt):
            episodes.append(self.start_volume_burst(now))
        if self._hit(c.account_burst_per_min, dt):
            episodes.append(self.start_account_burst(now))

        still_active = []
        for b in self.active:
            if now >= b.episode.end_time:
                continue
            b.carry += b.per_sec * dt
            n = int(b.carry)
            b.carry -= n
            for _ in range(n):
                out.append(self.market.make_trade(now, pair=b.pair, user=b.user))
            still_active.append(b)
        self.active = still_active
        return out, episodes
