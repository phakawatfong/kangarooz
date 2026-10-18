"""Deliberate messiness: late events and duplicates.

- Late: event_timestamp pushed 5-25s back (inside the 30s watermark) or 40-120s back (beyond it,
  so the stream drops it and the batch layer must correct it).
- Duplicate: the SAME trade (same trade_id) sent twice, like an app-level retry. The producer's
  idempotence only removes broker-level retry duplicates, so the sink must dedupe on trade_id.
"""

from __future__ import annotations

import random
from datetime import timedelta

from ..config import SimConfig
from ..domain import Trade


def apply_chaos(trades: list[Trade], cfg: SimConfig, rng: random.Random) -> list[Trade]:
    out: list[Trade] = []
    for t in trades:
        if rng.random() < cfg.late_pct:
            delay = rng.uniform(5, 25) if rng.random() < 0.5 else rng.uniform(40, 120)
            t = t.model_copy(update={"event_timestamp": t.event_timestamp - timedelta(seconds=delay)})
        out.append(t)
        if rng.random() < cfg.duplicate_pct:
            out.append(t)
    return out
