"""Event models that mirror the Avro contracts in /schemas."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import ROUND_HALF_EVEN, Decimal

# Avro decimal(18, 8): every Decimal must be quantized to exactly 8 places.
SCALE = Decimal("0.00000001")


def to_decimal(value: float | str | Decimal, decimals: int | None = None) -> Decimal:
    """Convert to a Decimal fit for decimal(18,8), optionally rounding to `decimals` first.

    Money/rates are never floats on the wire: floats can't represent 0.1 exactly.
    """
    d = Decimal(str(value))
    if decimals is not None:
        d = d.quantize(Decimal(1).scaleb(-decimals), ROUND_HALF_EVEN)
    return d.quantize(SCALE, ROUND_HALF_EVEN)


@dataclass(frozen=True)
class Trade:
    """One executed FX trade (raw facts only, no labels)."""

    trade_id: str
    user_id: str
    currency_pair: str  # "BASE/QUOTE", market convention
    side: str  # "BUY" | "SELL" of the BASE currency
    rate: Decimal  # quote per 1 base, independent of side
    base_amount: Decimal  # in base currency
    event_timestamp: datetime  # tz-aware UTC, when the trade executed

    @property
    def base_ccy(self) -> str:
        return self.currency_pair.split("/")[0]

    @property
    def quote_ccy(self) -> str:
        return self.currency_pair.split("/")[1]

    @property
    def quote_amount(self) -> Decimal:
        """Derived, never stored: base_amount * rate (always multiplication thanks to the convention)."""
        return (self.base_amount * self.rate).quantize(SCALE, ROUND_HALF_EVEN)

    @property
    def bought_ccy(self) -> str:
        return self.base_ccy if self.side == "BUY" else self.quote_ccy

    @property
    def sold_ccy(self) -> str:
        return self.quote_ccy if self.side == "BUY" else self.base_ccy

    def to_avro(self) -> dict:
        return {
            "trade_id": self.trade_id,
            "user_id": self.user_id,
            "currency_pair": self.currency_pair,
            "side": self.side,
            "rate": self.rate,
            "base_amount": self.base_amount,
            "event_timestamp": self.event_timestamp,
        }


@dataclass(frozen=True)
class AnomalyEpisode:
    """Ground truth for one injected anomaly (point or collective)."""

    episode_id: str
    anomaly_type: str  # price_spike | fat_finger | volume_burst | account_burst
    scope: str  # TRADE | USER | PAIR
    scope_key: str  # trade_id | user_id | currency_pair
    start_time: datetime
    end_time: datetime
    params: dict[str, str] = field(default_factory=dict)

    def to_avro(self) -> dict:
        return {
            "episode_id": self.episode_id,
            "anomaly_type": self.anomaly_type,
            "scope": self.scope,
            "scope_key": self.scope_key,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "params": dict(self.params),
        }
