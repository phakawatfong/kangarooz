# Phase 3 — Stream Processing & Alerts (Week 3)

**Status:** spike done (Q0–Q3 answered) · ADR-006 next · **DDIA:** Ch 11

## Goal
Turn the trade stream into real-time alerts (Kappa path) within p95 < 5 s.

## Questions to answer
- Event time vs processing time; tumbling vs sliding windows; watermarks; state.
- Window sizes and thresholds for each rule (price spike, fat finger, volume burst, account burst).
- What happens to a trade that is 2 minutes late?

## Alert contract (design session, 2026-10-26)

Designed from its three consumers: the **notifier** (skip duplicates, tell a human why), **grading** (match to ground-truth episodes: same `anomaly_type` + same `scope_key` + overlapping windows), and the **lake** (`anomaly_event`, audit and history). Mirrors `anomaly_episode` so grading compares like with like.

| Field | Type | Description | Used by |
|---|---|---|---|
| `alert_id` | string | Deterministic `hash(anomaly_type, scope_key, window_start, rule_version)`, so a replay produces the same id | Notifier (dedupe), lake (merge key) |
| `anomaly_type` | string | `price_spike` / `fat_finger` / `volume_burst` / `account_burst`. String, not enum (enum trap, see ADR-002) | Grading, notifier, lake |
| `scope` | enum `TRADE` / `USER` / `PAIR` | What the alert is attached to (same enum as `anomaly_episode`) | Grading, notifier |
| `scope_key` | string | `trade_id`, `user_id` or `currency_pair`, depending on scope | Grading, notifier |
| `window_start`, `window_end` | timestamp-millis | Event-time window; both equal the trade's time for single-trade rules | Grading (overlap), lake |
| `detected_at` | timestamp-millis | Processing time when the alert was produced | Latency measurement, lake |
| `rule_version` | int | Bumped when a rule's logic or threshold changes; part of `alert_id` so backtests don't collide with old ids | Backtests, lake |
| `is_late` | boolean | Trade arrived after the 30 s watermark (single-trade rules only) | Grading, audit |
| `observed_value`, `baseline`, `threshold` | double | Why it fired | Notifier message, audit |
| `trigger_trade_id` | string, nullable | The trade that fired a single-trade rule | Notifier, audit |

**Kafka key of `fx.alerts`: `alert_id`.** Even spread across partitions; replays of the same alert land on the same key, so a compacted topic or the notifier can collapse them. Gives up per-scope ordering.

## Rules: initial windows and thresholds (tune in the spike)

Derived from how the simulator injects each anomaly. Every threshold is a starting point, to be tuned against precision/recall.

| Rule | In plain words | Scope / key | State | Initial window and threshold |
|---|---|---|---|---|
| `fat_finger` | A typo in the amount: one trade far bigger than normal (5,000 typed as 500,000) | TRADE / `trade_id` | Baseline amount per pair | `base_amount` > 20× the pair's rolling typical amount (injected at 50–200×) |
| `price_spike` | A trade at a wrong price: far from the market while the market itself didn't move | TRADE / `trade_id` | Rolling mid / volatility per pair | Rate deviates from the pair's rolling mid by more than a z-score or % threshold (injected at 1–3%) |
| `volume_burst` | A currency pair suddenly gets very busy: each trade is normal, only the total is odd | PAIR / `currency_pair` | Count per pair vs baseline | 10 s tumbling window; count ≥ 3× the pair's rolling baseline (injected at 5–10× for 10–30 s) |
| `account_burst` | One user suddenly trades very fast: each trade is normal, only the speed is odd | USER / `user_id` | Count per user | 10 s tumbling window; ≥ 50 trades (injected at 10–30 trades/s for 10 s). Check against Zipf "whale" users for false positives |

The first two judge one trade at a time. The last two judge a group of trades in a window, so only they need a watermark, and only they can miss late trades.

**Window type: tumbling first** (decided 2026-10-26). Each trade lands in one window, so one alert per window per key, and 5× less state than a 10 s window sliding every 2 s. Known risk, the **boundary problem**: a burst that straddles a window boundary is split, and each half may miss the threshold. With the injected patterns the loss should be small (an account burst at 10 trades/s splits at worst 50/50, right at the threshold; volume bursts over 20 s always cover a full window), but it must be measured: run the same data with tumbling and with sliding (10 s / 2 s) and compare recall, alert count and state size. Switch to sliding if the recall loss matters. Session windows (one window per burst, closed after an inactivity gap) are a third option.

Option to evaluate: compute baselines in DuckDB from silver every few minutes and broadcast them to the stream, instead of maintaining them as streaming state.

## Design findings to resolve in the spike

- **Watermark vs the 5 s SLA.** In append mode, a window's result is emitted only after the watermark passes the window end, which adds ≥ 30 s. That breaks p95 < 5 s for window rules. Candidate: **update mode** plus a threshold filter, so the alert is emitted at the first trigger where the threshold is crossed. Re-emissions as the count grows carry the same `alert_id` (same window), so they deduplicate downstream.
- **Repeated alerts per burst.** With sliding windows one burst falls into up to 5 overlapping windows; with tumbling windows a 30 s burst still spans 3 windows. Each window has a different `window_start`, so a different `alert_id`. **Decided: cooldown in the notifier**, per (`anomaly_type`, `scope_key`), so it covers users (account burst) and pairs (volume burst). Initial cooldown 60 s (longer than the 10–30 s bursts). `fx.alerts` and the lake keep every alert; only notifications are suppressed, and suppressed alerts are logged. Cooldown state lives in the single notifier instance; with `alert_id` as the key, alerts for one user can land on different partitions, so scaling the notifier to several instances needs either key = `scope_key` or a shared cooldown store.

## Open questions (design review, 2026-10-17)
- **Where duplicates stop:** the notifier keeps a store of sent `alert_id`s and skips repeats (idempotent consumer). Redpanda transactions give exactly-once *inside* the broker, but a page/email is an external side effect. Exactly-once ends at the system boundary.
- **Late trades in the alert path:** trades that arrived more than 30 s after their event time are excluded from window statistics (window not reopened). Decided per record, not by the watermark; see Results (Q3). Single-trade rules can still evaluate late trades and emit an alert with `is_late = true`.
- **Crash before offset commit:** the batch is reprocessed (at-least-once). Verify that the deterministic `alert_id` + notifier dedupe makes this harmless.

## Decisions (ADRs)
- [ADR-005](../adr/ADR-005-stream-engine.md) Stream engine: Spark Structured Streaming — Accepted
- [ADR-006](../adr/ADR-006-delivery-guarantee.md) Delivery guarantee: at-least-once + dedupe on `alert_id` — Proposed

## Spike design decisions (design session, 2026-10-27)

Decided before any spike code. Each decision gives how the mechanism works, the options and what was chosen, and names the spike question it serves.

### D1. Detector runs as its own Spark process (Q1, Q3)
**How it works:** a Kafka topic is a log. Any number of consumer groups read it independently, so a second reader of `fx.trades` costs one more sequential read, not twice the broker work. A Spark app is one driver JVM, and all its queries share the driver and the `local[*]` cores.

| | **Separate process** ✅ | Second query in the lake-sink app |
|---|---|---|
| Memory (16 GB) | One more JVM (~1–2 GB) | One JVM |
| Failure / restart | Independent: the Q3 kill test leaves ingest alone | Killing it kills the lake sink too |
| Latency (Q1) | Clean measurement | The lake sink's Python `foreachBatch` competes for the driver; needs FAIR scheduler pools |
| Rule change | Ingest keeps running | Ingest restarts |

**Why:** Q1 and Q3 both need isolation, and one JVM is an affordable price for it. Matches `detector-worker` in the overview.

### D2. Native `from_avro` / `to_avro` + Spark's Kafka sink (Q0)
**How it works:** `groupBy(window)` needs the trade fields as columns, so decoding must be a Spark expression. It can't live inside `foreachBatch`. OSS Spark reads and writes raw Avro, not the Confluent wire format (magic byte + 4-byte schema id + body; Databricks handles it, OSS doesn't). So the 5-byte header is stripped (`substring(value, 6)`, plus a check of the schema id) and prepended (`concat(header, to_avro(...))`) by hand.

| | **Native** ✅ | Python UDF decode + `foreachBatch` producer |
|---|---|---|
| Codec | Spark's Avro mapping, pinned by one test that decodes Spark's bytes with `contract.avro_codec` | `contract.avro_codec` everywhere |
| Data path | Rows stay in the JVM | Every row crosses JVM ↔ Python: fine at 500 tps, costly at 5k |
| Producing | Spark's Kafka sink: retries and flushing built in, at-least-once | Our own producer: flush, delivery errors, collect to the driver |
| Cost | One more jar (`spark-avro`) | More code that we own |

**Why:** it's easier to maintain. We own fewer parts, there is one schema file, and one test pins compatibility.

### D3. Update output mode + threshold filter for window rules (Q1, Q3)
**How it works:** the output mode decides *when* an aggregate row leaves the stream.
- **Append** waits until the watermark passes the window end (10 s window + 30 s watermark = ≥ 40 s), so it fails the SLA.
- **Update** emits every row that changed in the trigger.
- **Complete** emits the whole table every trigger, which doesn't scale.

| | **Update + `count ≥ 50`** ✅ | Fire-once state (`transformWithStateInPandas`) |
|---|---|---|
| Latency | First trigger after the crossing | Same |
| Records per alert | One per trigger while the count grows: same `alert_id`, growing `observed_value` | Exactly one |
| Code | Built-in `groupBy(window).count()` | Per-key "fired" flag, eviction timers, pandas UDF path |
| Duplicates downstream | Deduped on `alert_id` | Crash duplicates still need deduping on `alert_id` |

**Why:** at-least-once delivery already forces an idempotent consumer keyed on `alert_id`. That one mechanism absorbs both crash duplicates and update-mode re-emissions, so fire-once state buys nothing we don't already pay for. Grading and the lake take the first emission and the last emission per `alert_id` respectively.

### D4. Latency = trade arrival → alert arrival, rebuilt by the grader (Q1)
**How it works:** four clocks are in play.
- `event_timestamp`: business time. A late trade's can be 2 minutes old.
- The trade's Kafka timestamp: when it reached the broker.
- `current_timestamp()`: in Spark this is **frozen at the start of the micro-batch**, so `detected_at` comes before the alert actually leaves.
- The alert's Kafka timestamp: when consumers can see it.

**SLA definition:**
- **Start = the trade's Kafka timestamp.** Event time would charge us for the producer's lateness.
- **End = the alert's Kafka timestamp.** It is reported next to `detected_at`; the gap between them is the batch duration.

**Finding the trade that made the rule true:**
- `fat_finger`: `trigger_trade_id`.
- `account_burst`: the 50th arrival for that user in that window.

| | **Grader rebuilds it from `fx.trades`** ✅ | `max(arrival)` in the Spark aggregate | New contract field |
|---|---|---|---|
| Accuracy | Exact | Understates by up to one batch of arrivals | Exact |
| Cost | Grader reads every trade (~150k per 5 min run, seconds) | Free, but has no home except logs | Changes a public contract for a measurement |

### D5. Trigger: as fast as possible (default), interval kept as a knob (Q1)
**How it works:** micro-batch latency ≈ waiting for the next trigger + the batch duration.
- The default trigger starts the next batch when the last one ends, which adds ~0 s and keeps the CPU busy.
- `processingTime=1s` adds up to 1 s.
- `5s` (as in the lake sink) adds up to 5 s and puts p95 at risk.

Record p95 for the default trigger and for 1 s, to know what the SLA costs in CPU.

### Smaller choices
- **`is_late` = trade arrival − `event_timestamp` > 30 s.** Spark doesn't expose the current watermark as a column, and `current_timestamp()` would mark every trade late during a replay from earliest. That would break the comparison in Q3.
- **State size (Q2):** taken from the query progress, `stateOperators[].numRowsTotal` / `memoryUsedBytes`. `numRowsDroppedByWatermark` shows that the 2-minute-late trades are excluded.
- **Replay (Q3):** a fresh checkpoint directory with the same `rule_version` replays from the earliest offset. `rule_version` is a CLI flag.

### Expected misfires (predicted from the simulator, to be confirmed)
- **`fat_finger` false positives.** Every pair draws `base_amount` from one log-normal (mean 5000, σ = 1), so the "per-pair typical amount" is the same everywhere. At 20 × 5000 = 100k the tail is P(z > 3.5) ≈ 2.3e-4. At 500 tps that's ~7 false alerts/min against 1 injected/min.
- **`fat_finger` recall ceiling.** Injection *multiplies a random-size trade*, so ~45 % of fat fingers stay under 250k. Raising the threshold to fix the false positives therefore caps recall.
- **`account_burst` whales.** Users are Zipf(1.1) over 1000. At 500 tps the top ~14 users do ≥ 50 trades per 10 s all the time, so a flat threshold fires on them every window: ~80 false alerts/min against 0.5 injected/min. Accepted for the spike. The fix is a per-user baseline, decided together with `price_spike` / `volume_burst`.

### Design review findings (grilling, 2026-10-27)
- **Duplicates stop at the side effect.** Only the notifier, the last hop before the outside world, sees every duplicate: update-mode re-emissions, crash replays and redeliveries. It dedupes on `alert_id`, and the re-emitted records are still kept in `fx.alerts` and the lake (the lake keeps the last count).
- **Sending and recording can't be atomic.** No transaction spans our store and a phone, so the notifier picks an order:
  - Record, then send: a crash in between **loses** the alert (at-most-once).
  - Send, then record: a crash in between **duplicates** it (at-least-once).

  **Chosen: send, then record.** A missed fraud alert costs more than a duplicate page. The outbox pattern doesn't help here: it solves the DB + publish dual write and is itself at-least-once. To shrink the duplicate window, pass `alert_id` as an **idempotency key** to a receiver that supports one (paging services usually have a dedup key). This is the same idea as the Phase 2 batch ledger, one hop further out.
- **10× load (5k trades/s) breaks nothing in the detector.**
  - Rough numbers (a day ≈ 10⁵ s): ~0.75 MB/s, ~75 GB/day on the broker. That's a retention sizing item, not a failure.
  - 5,000 trades/s over 6 partitions ≈ 830 msg/s each.
  - Window state is ~5k rows (~0.5 MB).
  - The first throughput limit in the system is the lake sink's silver dedupe scan (the `ponytail:` comment in `lake.py`).
- **The detector's real gap is availability: it's a single point of failure.**
  - **Durability:** the checkpoint goes to object storage, and a supervisor restarts the process.
  - **Health:** checked by **progress** (time since the last batch, or consumer lag), not "is the process alive". A stuck query passes a liveness check.
  - **Durability isn't availability.** A restart loses nothing but delays alerts by 30–60 s or more, breaking the SLA during failover.
  - **If that's unacceptable, run active-active:** two detectors with their own checkpoints. The deterministic `alert_id` gives both copies the same id, and the notifier collapses them.
- **The SLA covers only what we own: p95 < 5 s broker to broker,** measured from the trade's Kafka timestamp to the alert's Kafka timestamp, over a stated window (e.g. per hour).
  - End to end = upstream + 5 s + delivery, so the business target is split into budgets per team.
  - Upstream delay can still be **monitored** without owning it: Kafka timestamp − `event_timestamp`.

## Spike plan (to implement with Claude Code)

**Purpose:** answer the three design questions below with the minimum code. This is a spike, not a product: no generalisation, no frameworks, no polish. **Stop when the checks for Q1–Q3 pass.** Every task should name the question it answers; if it answers none, it is out of scope.

**Pipeline:** Spark reads `fx.trades` → evaluates the rule → writes Avro alerts to `fx.alerts` (Confluent wire format, key = `alert_id`, schema `schemas/fx_alert.avsc` following the alert contract above, registered with BACKWARD compatibility). Keep the Phase 2 stream working.

### Q0. End-to-end path (sanity check)
- **Build:** `fat_finger` with a fixed threshold (`base_amount` > 20× a fixed per-pair typical amount). Schema → topic → one alert per injected fat-finger trade.
- **Check:**
  - [x] Alerts appear in `fx.alerts`, decodable via the registry, with all contract fields filled (the grader decodes every alert with the registry's deserializer; `test_detector.py` pins that Spark's `to_avro` bytes decode with `contract.avro_codec`)

### Q1. Can Spark hold p95 < 5 s for a window rule? (ADR-005 assumption)
- **Build:** `account_burst`: 10 s tumbling window per `user_id`, fixed threshold (≥ 50 trades), **update output mode** with a threshold filter, 30 s watermark on `event_timestamp`. Record the time the threshold was crossed and `detected_at`.
- **Check:**
  - [x] p95 reported for `account_burst` and `fat_finger`; target < 5 s. Measured to the alert's Kafka timestamp, not `detected_at` (D4); see Results

### Q2. How much recall does tumbling lose at window boundaries? (window decision)
- **Build:** a minimal grading script: reads `fx.alerts` + `fx.trades.ground_truth`, matches on same `anomaly_type` + `scope_key` + overlapping windows, prints precision and recall per `anomaly_type`. Run `account_burst` twice on the same data: tumbling (10 s) and sliding (10 s / 2 s).
- **Check:**
  - [x] Precision and recall per type reported
  - [x] Tumbling vs sliding: recall, alert count and state size side by side

### Q3. Does a deterministic `alert_id` make replays and crashes harmless? (ADR-006)
- **Build:** no new code beyond Q0–Q1; run the experiments.
- **Check:**
  - [x] Replay from the earliest offset with the same `rule_version` → identical set of `alert_id`s. First run: `account_burst` was a superset (+9 ids). Fixed with a per-record late filter; rerun identical (see Results)
  - [x] Bumped `rule_version` → new `alert_id`s, no collisions with the old run
  - [x] Kill and restart the job mid-run → no lost alerts; any duplicates carry the same `alert_id`

### Deliberately not built (decisions, not gaps)
- `price_spike` and `volume_burst`: same patterns as `fat_finger` and `account_burst`, so no new design question
- The notifier service: the cooldown stays a design (see findings above), or a stub at most
- The `anomaly_event` table in DuckLake
- Rolling baselines in streaming state: fixed thresholds only
- Config frameworks, retries, dashboards

### Record while building
Surprises and measured numbers (latency, precision/recall, state size, threshold misfires on whale users, update-mode quirks). They feed ADR-006 and the threshold tuning.

### Results (spike run, 2026-10-27)

**Setup:**
- One laptop, default Spark `local[*]`, `spark.sql.shuffle.partitions = 6`, as-fast-as-possible trigger.
- Run 1: 5 min at ~490 trades/s with default injection rates.
- Run 2: 6 min at ~590 trades/s with `--account-burst-per-min 6 --fat-finger-per-min 6`. Two detectors (tumbling v1, sliding v2) and the producer ran side by side.
- Grader: `make grade`, with `--since` to measure latency only on live data.

**Q1 latency** (trade's Kafka timestamp → alert's Kafka timestamp, first emission):

| Run | `fat_finger` p95 | `account_burst` p95 | Alert arrival − `detected_at`, p95 |
|---|---|---|---|
| Run 1, one detector | 671 ms | 1,331 ms | 370 / 715 ms |
| Run 2, sliding v2, two JVMs + producer competing | 1,967 ms | 3,758 ms | 1,859 / 4,077 ms |
| Run 2, tumbling v1 with `kill -9` of its JVM mid-run | 29.4 s | 19.6 s (max 37.8 s) | n/a |

- **The ADR-005 assumption holds.** Spark meets p95 < 5 s for both kinds of rule. Headroom shrinks when the laptop is shared (run 2), and the third row is the cost of a crash.
- **`detected_at` understates latency by the batch duration,** as predicted in D4. In run 2 that gap (~4 s) is nearly the whole latency.
- **A crash costs latency, not data.** The driver exited ~11 s after its JVM died; restarted from the checkpoint, the alerts arrived late (max 37.8 s) but none were lost. This is the durability-vs-availability point from the design review, now measured.
- Batches took 0.3–0.7 s in run 1 and 1.3–2.3 s in run 2.

**Q2 precision/recall, and tumbling vs sliding** (run 1 + run 2 data):

| | Tumbling 10 s (v1) | Sliding 10 s / 2 s (v2) |
|---|---|---|
| `account_burst` recall | **1.00** (39/39) | 1.00 (39/39) |
| `account_burst` precision | 0.06 (58/991) | 0.06 (285/4,958) |
| Distinct alerts / records | 991 / 6,667 | 4,958 / 17,730 |
| State rows / bytes (steady) | ~2.3k / ~1 MB | ~13k / ~3.5 MB |

- **The boundary problem cost zero recall at this threshold.** Sliding pays 5× the alerts and ~5.7× the state for nothing. **Decision: stay tumbling.**
- **`account_burst` precision is 0.06 because of the whales,** as predicted. Run 1: 395 alerts in 5 min ≈ 79/min, against the ~80/min predicted. The top users (U00000–U00006) fire in every window.
- **Update mode re-emits ~7–10 records per alert** (6,667 records / 991 ids).
- **`fat_finger` recall is 0.88** (35/40), close to the ~0.85 the log-normal predicts at a 100k threshold.
- **`fat_finger` precision depends on how often anomalies are injected:** 0.10 in run 1, 0.32 in run 2. The false positives run at a constant ~7/min (predicted 7/min), so precision rises when anomalies are injected more often. Precision on simulated data is a property of the simulator's mix, not only of the rule.

**Q3 replay and crash:**
- **Bumped `rule_version`:** v1 and v2 share **0** ids.
- **Kill and restart:**
  - **No loss:** v1 still found 39/39 bursts.
  - **Same `fat_finger` ids:** the same 111 as the uninterrupted v2 run and as the later replay.
  - **Duplicates:** for `account_burst`, crash duplicates can't be told apart from update-mode re-emissions; both carry the same `alert_id`.
- **Same-`rule_version` replay from earliest:**
  - **`fat_finger`:** identical (111 records → 222, still 111 ids).
  - **`account_burst`:** a strict superset, 991 → **1,000** ids, with 0 live-only ids.
  - **Cause (verified):** all 9 new windows sit at 50–51 trades and contain 1–3 trades that arrived > 30 s late. The watermark advances only *between* micro-batches. Live, those late trades were dropped (count 48–49, no alert). The replay read the whole backlog as one batch (357k rows), where the watermark dropped nothing.
  - **What this means:** a deterministic `alert_id` makes the same window produce the same id, but **whether a window fires depends on micro-batch boundaries** whenever late data sits near the threshold.

**Decision (for ADR-006):** make window rules replay-deterministic?

| | Accept it | **Drop late trades per record** ✅ | Cap replay batch size |
|---|---|---|---|
| How | Document: a replay can add alerts for near-threshold windows with late trades | Before `groupBy`, filter trades whose arrival − `event_timestamp` > 30 s (the same rule as `is_late`); the watermark still evicts state | `maxOffsetsPerTrigger` on replay |
| Deterministic | No | **Yes**: per-record, independent of batching | No: only closer to live |
| Cost | Backtests can disagree with live | Lateness is judged against arrival time, not the watermark; one line of code | Slower replays, still not exact |

- **Why:** a backtest is only useful if it reproduces live. With the watermark alone, which trades count depends on batch boundaries, so detector downtime, a restart or a replay all change the answer. Judging each trade by its own arrival lag gives the same answer every time. The watermark stays, only to evict old windows from state.
- **Not freshness vs completeness:** dropping trades > 30 s late from the stream was already decided (the watermark did it). This choice only makes the drop deterministic. Completeness comes from the Lambda side: the batch model recounts with late trades included (C5), and reconciling it against `fx.alerts` (Phase 4) finds the bursts the stream missed.
- **Rerun (rule_version 3, fresh 6-minute run, 2026-10-28):** live then replay from earliest. The replay read all 235,085 trades in one batch, the case that failed before. `fat_finger` 97 = 97 ids, `account_burst` 650 = 650 ids, 0 only-live, 0 only-replay. Recall unchanged (fat_finger 41/44, account_burst 28/28); p95 latency 896 ms / 1,659 ms.

**Smaller surprises:**
- `collect()` returns naive datetimes in the machine's local zone, whatever the Spark session time zone is.
- Spark warns at startup about non-nullable Avro fields fed by nullable Catalyst columns; it throws only if a null is actually written.
- The producer's stdout is block-buffered when redirected to a file, so its progress lines can't be used as a timer.
- The shell's `JAVA_HOME` pointed at a missing Homebrew JDK. `make setup` catches this; the spike ran with SDKMAN's Java 17.
- `test_golden.py` failed intermittently once the Spark tests existed. **Cause:** its fake clock patched `time.sleep` process-wide, and PySpark keeps a thread that sleeps every second (even after `spark.stop()`), which advanced the fake clock. **Fix:** patch only `produce.time`. Proven with a background sleeping thread: the old fixture fails 2 of 3 golden tests, the new one passes.

## Exit criteria
- [x] ADR-005 written
- [ ] ADR-006 written
- [x] Alert schema defined
- [x] Latency measured against the 5 s target
- [x] Precision/recall reported for the implemented rules
