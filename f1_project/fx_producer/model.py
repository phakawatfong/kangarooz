"""Event models that mirror the Avro contracts in /schemas."""

from __future__ import annotations

from decimal import ROUND_HALF_EVEN, Decimal
from typing import Annotated, Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

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


Side = Literal["BUY", "SELL"]
Scope = Literal["TRADE", "USER", "PAIR"]
AnomalyType = Literal["price_spike", "fat_finger", "volume_burst", "account_burst"]
AvroDecimal = Annotated[Decimal, Field(max_digits=18, decimal_places=8)]


class Trade(BaseModel):
    """One executed FX trade (raw facts only, no labels). Fields mirror schemas/fx_trade.avsc."""

    model_config = ConfigDict(frozen=True)

    trade_id: str
    user_id: str
    currency_pair: str  # "BASE/QUOTE", market convention
    side: Side  # of the BASE currency
    rate: AvroDecimal  # quote per 1 base, independent of side
    base_amount: AvroDecimal  # in base currency
    event_timestamp: AwareDatetime  # when the trade executed

    # Derived values are plain properties, never computed_field: they must stay out of model_dump(),
    # which is the Avro record.
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


class AnomalyEpisode(BaseModel):
    """Ground truth for one injected anomaly (point or collective). Mirrors schemas/anomaly_episode.avsc."""

    model_config = ConfigDict(frozen=True)

    episode_id: str
    anomaly_type: AnomalyType
    scope: Scope
    scope_key: str  # trade_id | user_id | currency_pair
    start_time: AwareDatetime
    end_time: AwareDatetime
    params: dict[str, str] = Field(default_factory=dict)
