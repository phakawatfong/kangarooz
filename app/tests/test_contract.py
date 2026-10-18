"""The Avro contract: records validate, round-trip, stay small, and carry no labels."""

import json
import random
from datetime import datetime, timezone
from decimal import Decimal

import pytest
from fastavro.validation import validate
from pydantic import ValidationError

from fx_producer.simulation.anomalies import AnomalyInjector
from contract.avro_codec import (EPISODE_SCHEMA, REGISTRY_HEADER_BYTES, TRADE_SCHEMA, decode, encode, parsed_schema,
                                 schema_text)
from fx_producer.config import SimConfig
from fx_producer.simulation.market import MarketSimulator
from fx_producer.domain import AnomalyEpisode, Trade

NOW = datetime(2026, 10, 14, 9, 0, tzinfo=timezone.utc)


def _sim(seed=7):
    sim_config = SimConfig(seed=seed)
    rng = random.Random(seed)
    market = MarketSimulator(sim_config, rng)
    return sim_config, rng, market


def test_trade_validates_and_round_trips():
    _, _, market = _sim()
    for _ in range(200):
        t = market.make_trade(NOW)
        rec = t.model_dump()
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
    sizes = [REGISTRY_HEADER_BYTES + len(encode(market.make_trade(NOW).model_dump(), TRADE_SCHEMA)) for _ in range(500)]
    avg = sum(sizes) / len(sizes)
    print(f"avg encoded value size incl. header: {avg:.1f} B")
    # Value only (key + record overhead come on top). Sizing card estimated ~117 B before key/overhead.
    assert 60 < avg < 130


def test_episode_validates():
    sim_config, rng, market = _sim()
    injector = AnomalyInjector(sim_config, market, rng)
    episodes = [injector.price_spike(NOW)[1], injector.fat_finger(NOW)[1], injector.start_volume_burst(NOW),
                injector.start_account_burst(NOW)]
    for episode in episodes:
        assert validate(episode.model_dump(), parsed_schema(EPISODE_SCHEMA))
        assert decode(encode(episode.model_dump(), EPISODE_SCHEMA), EPISODE_SCHEMA)["episode_id"] == episode.episode_id


def test_models_mirror_avro_schemas():
    """model_dump() is the Avro record, so model fields must match the .avsc fields, in order."""
    for model, schema in ((Trade, TRADE_SCHEMA), (AnomalyEpisode, EPISODE_SCHEMA)):
        assert list(model.model_fields) == [f["name"] for f in json.loads(schema_text(schema))["fields"]]


def test_trade_rejects_values_outside_decimal_18_8():
    _, _, market = _sim()
    t = market.make_trade(NOW)
    for bad in (Decimal("1.123456789"), Decimal("12345678901")):
        with pytest.raises(ValidationError, match="rate"):
            Trade(**{**t.model_dump(), "rate": bad})
