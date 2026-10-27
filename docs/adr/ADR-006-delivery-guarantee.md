# ADR-006: Alert delivery — at-least-once + dedupe on `alert_id`

Status: Proposed    Date: 2026-10-28    Owner: Kids

## Decision

**At-least-once from trade to notification. Duplicates are expected and made harmless, never prevented.**

- **One identity per alert:** `alert_id = sha256(anomaly_type | scope_key | window_start | rule_version)`, also the Kafka key of `fx.alerts`. Every duplicate of an alert carries the same id.
- **Dedupe where the side effect happens:** the notifier keeps a store of sent `alert_id`s and skips repeats. `fx.alerts` and the lake keep every record.
- **Send, then record:** the notifier sends first and writes the id to its store second, and passes `alert_id` as the idempotency key to receivers that accept one.
- **Replays reproduce live:** window rules drop trades that arrived > 30 s after their event time, judged per record, so a replay with the same `rule_version` emits the same set of ids. A new `rule_version` emits new ids.

**In one line: not every anomaly is guaranteed to be found, but every alert found is guaranteed to be sent.**

## Scope: three guarantees, this ADR owns one

| Question | Guarantee | Why this one | Where decided |
|---|---|---|---|
| Does every acknowledged trade reach the lake exactly once? | **Exactly-once** (effect): bronze, silver and the batch ledger in one DuckLake transaction | The lake is inside our system, so one transaction can cover it | ADR-003, Phase 2 |
| Which trades does a window rule count? | **Bounded:** trades up to 30 s late; later ones are left out | Window state must be released at some point; the watermark trades completeness for memory | Phase 3 doc (watermark, Q3) |
| Once an alert exists, does it reach a person? | **At-least-once** | The phone is outside any transaction, and a miss costs more than a duplicate | **This ADR** |

The bounded detection is not a hole in the system. The batch layer recounts every trade, late ones included, and reconciling it against `fx.alerts` on `alert_id` finds what the stream missed (ADR-007).

## 1. Driving requirement

- **A missed fraud alert costs more than a duplicate page.** A duplicate is noise; a miss is a loss nobody sees.
- **No acknowledged trade is lost; duplicates are possible** (C7). The broker and the detector are at-least-once by design: Spark's Kafka sink has no transactional write, and a crash replays the batch since the last checkpoint.
- **Replay for backtests** (C8). The same log is read again, so the same alerts are produced again.
- **The side effect is outside our system.** No transaction spans our store and a phone or a paging service, so exactly-once ends at the system boundary.

Duplicates come from four places, all measured or designed in Phase 3:

| Source | Volume |
|---|---|
| Update-mode re-emission while a window's count grows (D3) | ~10 records per alert (6,337 records / 650 ids) |
| Crash before the checkpoint commits | The batch since the last checkpoint (`kill -9`: 0 alerts lost) |
| Replay from earliest, same `rule_version` | The whole history (650 = 650 ids after the late-trade fix) |
| Active-active detectors, if availability needs them | Every alert, twice |

## 2. Alternatives considered

| | At-most-once | Exactly-once end to end | **At-least-once + dedupe on `alert_id`** ✅ |
|---|---|---|---|
| How | Commit offsets / record before sending | Kafka transactions from detector to `fx.alerts`, transactional consumer | Every hop may repeat; the last hop before the outside world dedupes |
| Crash between send and record | Alert **lost** | Still possible at the notifier: a phone can't join the transaction | Alert **sent twice** |
| Covers re-emissions and replays | No, they're new records | No, they're new records | Yes, same id |
| Works with Spark's Kafka sink | Yes | No transactional sink | Yes |
| Cost | Silent misses | A mechanism that stops one hop short of where it matters | A dedupe store in the notifier |

**Notifier ordering** (the one choice inside the notifier):

| | Record, then send | **Send, then record** ✅ |
|---|---|---|
| Crash in between | Alert lost (at-most-once) | Alert sent twice (at-least-once) |
| Shrink the gap | n/a | Pass `alert_id` as the receiver's idempotency key |

The outbox pattern doesn't apply: it solves "write the DB and publish" atomically, is itself at-least-once, and leaves the same send-vs-record gap at the notifier.

**Where to dedupe** (D3): fire-once state in Spark (`transformWithStateInPandas`) would remove update-mode re-emissions but not crash or replay duplicates, so the notifier would need the store anyway. One mechanism at the boundary covers all four sources.

## 3. Deciding axis

**The cost of a missed alert vs the cost of a duplicate.** A duplicate is cheap and is absorbed by one mechanism that has to exist anyway, because at-least-once delivery forces an idempotent consumer. A miss is silent and unrecoverable. Exactly-once can't reach the phone, so paying for it buys nothing at the boundary that matters.

## 4. What I gave up

- **Rare duplicate pages:** a crash between send and record pages twice, unless the receiver honours the idempotency key.
- **A stateful notifier.** Sizing at the dev rate: ~1.8 distinct alerts/s ≈ 1.8 × 10⁵ ids/day; kept for the 7-day replay window ≈ 1.3M ids × ~100 B ≈ 130 MB. It must outlive restarts, so it can't be in memory.
- **Every consumer of `fx.alerts` must be idempotent,** not just the notifier: the lake merges on `alert_id` and keeps the last count.
- **Window rules ignore trades > 30 s late,** judged by arrival time. Completeness comes from the batch layer and reconciliation (ADR-007), not from the stream.
- **Ordering per user or pair,** because the key is `alert_id` (see the alert contract).

## 5. What would change my mind

- **A receiver that can't take duplicates and has no idempotency key,** e.g. an automated action like freezing an account that isn't idempotent. Then that action needs its own idempotency, or record-intent-then-send with reconciliation.
- **Duplicate pages cause alert fatigue in practice.** Then tighten the window between send and record before revisiting the guarantee.
- **A regulator requires an exactly-once audit trail of notifications.** Then the store of sent ids becomes the audit record, written transactionally with the send log.

## Backtests never reach a person (decided 2026-10-28)

A backtest with a new `rule_version` produces new ids for old trades. In `fx.alerts` the notifier would page for history.

| | **Own topic** ✅ | Notifier filters by active `rule_version` | Backtests skip Kafka |
|---|---|---|---|
| How | Backtests write to `fx.alerts.backtest` (a job flag); the notifier never reads it | One topic; notifier config lists the live version per rule | Backtests write to the lake or a file |
| Failure mode | Forgetting the flag pages for history, visible at once | Config out of step with a rollout: pages for history, or **silently drops live alerts** | None for paging |
| Same output path as live | Yes | Yes | No (no Kafka sink, no `to_avro`) |

**Chosen: own topic.** Where an alert lands says whether it is live, and the failure is loud, never silent.

- **Promoting a version to live:** the new live job starts from the **latest** offset, not the earliest, or it replays history into `fx.alerts`. During the cutover both versions run briefly and emit different ids for the same burst; the notifier's cooldown per (`anomaly_type`, `scope_key`) absorbs the overlap.
- **Not built in the spike:** there is no notifier yet, so the spike's backtests share `fx.alerts` and are told apart by `rule_version`.
