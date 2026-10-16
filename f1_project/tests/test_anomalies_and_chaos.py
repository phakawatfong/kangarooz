"""Injected anomalies produce correct ground truth; chaos behaves as documented."""

import random
from datetime import datetime, timedelta, timezone

from fx_producer.simulation.anomalies import AnomalyInjector
from fx_producer.simulation.chaos import apply_chaos
from fx_producer.config import SimConfig
from fx_producer.simulation.market import MarketSimulator

NOW = datetime(2026, 10, 14, 9, 0, tzinfo=timezone.utc)


def _setup(**kw):
    cfg = SimConfig(seed=11, **kw)
    rng = random.Random(11)
    market = MarketSimulator(cfg, rng)
    return cfg, rng, market, AnomalyInjector(cfg, market, rng)


def test_price_spike_is_point_anomaly_on_that_trade():
    _, _, market, inj = _setup()
    trade, ep = inj.price_spike(NOW)
    assert (ep.scope, ep.scope_key, ep.anomaly_type) == ("TRADE", trade.trade_id, "price_spike")
    mid = market.mids[trade.currency_pair]
    assert abs(float(trade.rate) / mid - 1) >= 0.0099


def test_fat_finger_is_big():
    _, _, _, inj = _setup(mean_base_amount=5000)
    trade, ep = inj.fat_finger(NOW)
    assert ep.scope == "TRADE" and ep.scope_key == trade.trade_id
    assert float(ep.params["multiplier"]) >= 50


def test_account_burst_is_collective_and_windowed():
    cfg, _, _, inj = _setup(price_spike_per_min=0, fat_finger_per_min=0, volume_burst_per_min=0, account_burst_per_min=0)
    ep = inj.start_account_burst(NOW)
    assert ep.scope == "USER" and ep.end_time == NOW + timedelta(seconds=10)
    burst_trades = []
    t = NOW
    while t < ep.end_time:
        out, _ = inj.tick(t, 0.1, [])
        burst_trades += out
        t += timedelta(seconds=0.1)
    assert burst_trades and all(x.user_id == ep.scope_key for x in burst_trades)
    per_sec = len(burst_trades) / 10
    assert abs(per_sec - float(ep.params["trades_per_sec"])) <= 2
    assert inj.tick(t, 0.1, [])[0] == []  # burst over


def test_volume_burst_targets_one_pair():
    _, _, _, inj = _setup(price_spike_per_min=0, fat_finger_per_min=0, volume_burst_per_min=0, account_burst_per_min=0)
    ep = inj.start_volume_burst(NOW)
    out, _ = inj.tick(NOW + timedelta(seconds=1), 1.0, [])
    assert ep.scope == "PAIR" and out and all(x.currency_pair == ep.scope_key for x in out)


def test_chaos_late_and_duplicates():
    cfg, rng, market, _ = _setup(late_pct=1.0, duplicate_pct=1.0)
    trades = [market.make_trade(NOW) for _ in range(100)]
    out = apply_chaos(trades, cfg, rng)
    assert len(out) == 200  # every trade duplicated
    assert all(out[i].trade_id == out[i + 1].trade_id for i in range(0, 200, 2))
    delays = [(NOW - t.event_timestamp).total_seconds() for t in out[::2]]
    assert all(5 <= d <= 120 for d in delays)
    assert any(d > 30 for d in delays) and any(d < 30 for d in delays)  # both sides of the watermark
