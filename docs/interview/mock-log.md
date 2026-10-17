# Mock interview log

## Round 1 — 2026-10-17 (tough) · "Walk me through your platform"

| Area | Score | Notes |
|---|---|---|
| Structure | 4/5 | Requirements → components → decision criteria. But the 2 minutes went on requirements + broker; the full architecture (Kappa alerts / Lambda analytics, lakehouse) was never covered. |
| Clarity | 3/5 | Q1 answer went back to defending Redpanda instead of answering the question. |
| Trade-offs | 3/5 | Excellent on Avro vs Protobuf vs JSON. Weak in follow-ups: accepted duplicate alerts without naming the cost. |
| Depth under pressure | 2/5 | Treated acks as handling lateness; `trade_id` as identity for window alerts; answered the dedupe question with "trace data lineage". |
| Ownership | 4/5 | Sounded like his own design; cost/operations reasoning convincing. |

**One thing to fix:** answer the exact question with a concrete mechanism (watermark, alert key, layer). If not designed yet: "I haven't designed that yet. Here's how I would…"

**Slips to avoid:**
- Acks ensure durability, not lateness handling. Lateness = watermark + batch layer.
- Modern Kafka (KRaft) has no ZooKeeper.

**Model answers to rehearse:**
- *Late trade (2 min late):* accuracy vs speed is per use case. Alert path: 30 s watermark, late trade excluded from windows, alerts stay ≤5 s p95 (single-trade rules may still fire, flagged late). Reports: trade is in the raw table; next batch run recomputes the window; reconciliation measures the gap.
- *Duplicate alerts:* `alert_id = hash(rule, scope_key, window_start)`; notifier dedupes on sent IDs; exactly-once ends at the system boundary.
- *Dedupe layer:* bronze keeps raw (may duplicate); silver dedupes at write via MERGE on `trade_id`; analysts query silver/gold only.

**Next opening structure (2 min):** problem (1 line) → requirements (3 lines) → end-to-end flow (alerts path + reports path) → 2–3 key decisions with what I gave up → what changes at scale.

## Round 2 — (planned)
