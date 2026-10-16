"""The Avro contract: records validate, round-trip, stay small, and carry no labels."""

import json
import random
from datetime import datetime, timezone
from decimal import Decimal

from fastavro.validation import validate

from fx_producer.anomalies import AnomalyInjector
from fx_producer.avro_local import REGISTRY_HEADER_BYTES, decode, encode, parsed_schema, schema_text
from fx_producer.config import EPISODE_SCHEMA, TRADE_SCHEMA, SimConfig
from fx_producer.market import MarketSimulator

NOW = datetime(2026, 10, 14, 9, 0, tzinfo=timezone.utc)


def _sim(seed=7):
    cfg = SimConfig(seed=seed)
    rng = random.Random(seed)
    market = MarketSimulator(cfg, rng)
    return cfg, rng, market


def test_trade_validates_and_round_trips():
    _, _, market = _sim()
    for _ in range(200):
        t = market.make_trade(NOW)
        rec = t.to_avro()
        assert validate(rec, parsed_schema(TRADE_SCHEMA))
        back = decode(encode(rec, TRADE_SCHEMA), TRADE_SCHEMA)
        assert back["trade_id"] == t.trade_id
        assert back["rate"] == t.rate and isinstance(back["rate"], Decimal)
        assert back["event_timestamp"] == NOW


def test_trade_contract_has_no_labels():
    """Leakage guard: the detector's input must never contain ground truth."""
    names = {f["name"] for f in json.loads(schema_text(TRADE_SCHEMA))["fields"]}
    assert names == {"trade_id", "user_id", "currency_pair", "side", "rate", "base_amount", "event_timestamp"}
    assert not any("anomal" in n or "label" in n or "inject" in n for n in names)


def test_encoded_size_matches_sizing_card():
    _, _, market = _sim()
    sizes = [REGISTRY_HEADER_BYTES + len(encode(market.make_trade(NOW).to_avro(), TRADE_SCHEMA)) for _ in range(500)]
    avg = sum(sizes) / len(sizes)
    print(f"avg encoded value size incl. header: {avg:.1f} B")
    # Value only (key + record overhead come on top). Sizing card estimated ~117 B before key/overhead.
    assert 60 < avg < 130


def test_episode_validates():
    cfg, rng, market = _sim()
    inj = AnomalyInjector(cfg, market, rng)
    eps = [inj.price_spike(NOW)[1], inj.fat_finger(NOW)[1], inj.start_volume_burst(NOW), inj.start_account_burst(NOW)]
    for ep in eps:
        assert validate(ep.to_avro(), parsed_schema(EPISODE_SCHEMA))
        assert decode(encode(ep.to_avro(), EPISODE_SCHEMA), EPISODE_SCHEMA)["episode_id"] == ep.episode_id
