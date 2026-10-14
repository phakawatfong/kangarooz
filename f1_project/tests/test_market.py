"""Rate convention, derived amounts, decimals."""

import random
from datetime import datetime, timezone
from decimal import Decimal

from fx_producer.config import PAIRS, SimConfig
from fx_producer.market import MarketSimulator
from fx_producer.model import SCALE, Trade, to_decimal

NOW = datetime(2026, 10, 14, 9, 0, tzinfo=timezone.utc)


def test_rate_convention_independent_of_side():
    """BUY and SELL on the same pair are quoted the same way (quote per 1 base)."""
    cfg = SimConfig(seed=1)
    market = MarketSimulator(cfg, random.Random(1))
    usdthb = next(p for p in PAIRS if p.pair == "USD/THB")
    trades = [market.make_trade(NOW, pair=usdthb) for _ in range(300)]
    assert {t.side for t in trades} == {"BUY", "SELL"}
    for t in trades:
        assert Decimal("30") < t.rate < Decimal("37")  # never the inverted 0.03 quote


def test_quote_amount_is_multiplication_and_direction_is_derived():
    t = Trade("t1", "U00001", "USD/THB", "BUY", to_decimal("36.1234", 4), to_decimal("1000", 2), NOW)
    assert t.quote_amount == Decimal("36123.40000000")
    assert (t.bought_ccy, t.sold_ccy) == ("USD", "THB")
    s = Trade("t2", "U00001", "USD/THB", "SELL", t.rate, t.base_amount, NOW)
    assert (s.bought_ccy, s.sold_ccy) == ("THB", "USD")


def test_decimals_fit_avro_decimal_18_8():
    cfg = SimConfig(seed=3)
    market = MarketSimulator(cfg, random.Random(3))
    for _ in range(500):
        t = market.make_trade(NOW, amount_multiplier=200)  # fat-finger sized
        for d in (t.rate, t.base_amount):
            assert d == d.quantize(SCALE)
            assert len(d.as_tuple().digits) <= 18


def test_whales_exist():
    cfg = SimConfig(seed=5)
    market = MarketSimulator(cfg, random.Random(5))
    users = [market.pick_user() for _ in range(5000)]
    top = max(users.count(u) for u in set(users))
    assert top > 5000 * 0.05  # one user >5% of flow: normal heavy trader, not an anomaly
