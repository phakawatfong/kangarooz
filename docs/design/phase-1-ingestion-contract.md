# Phase 1 — Ingestion Contract & Synthetic Producer (Week 1)

**Status:** contract + sizing decided, producer built · ADR-001 accepted · ADR-002 to write
**7-step method coverage:** steps 1–3 (requirements, estimates, data contract)

## 1. Goal

Define what one trade looks like on the wire, size the system from it, and build a producer
that emits realistic trades **plus an answer key** (ground truth) so later phases can be graded.

**Exit criteria**
- [x] Trade event contract with a reason for every field
- [x] Kafka record key decided, with its trade-off
- [x] Ground-truth model that supports point and collective anomalies
- [x] Sizing: five numbers, each with the decision it drives
- [x] Producer running against Redpanda; design assumptions verifiable (`make verify`)
- [x] ADR-001 (overall architecture) accepted — owner: Kids
- [ ] ADR-002 (broker + schema) written — owner: Kids

## 2. Trade event contract — `schemas/fx_trade.avsc` (topic `fx.trades`)

Raw facts only. No derived metrics, no labels.

| Field | Avro type | Why it's here |
|---|---|---|
| `trade_id` | string | Dedupe at the sink on retry / backfill; also the record key |
| `user_id` | string | Account-level rules (account burst). Thin reference, no user details |
| `currency_pair` | string | Base/quote in market convention, e.g. `USD/THB` |
| `side` | enum `[BUY, SELL]` | Customer buys or sells the **base** currency |
| `rate` | decimal(18,8) | Quote per 1 base (THB per 1 USD), **same convention for both sides** |
| `base_amount` | decimal(18,8) | Amount in base currency; quote amount = `base_amount × rate` (derived downstream) |
| `event_timestamp` | timestamp-millis | When the trade executed (business time); drives windowing |

**Design decisions**
- **Pair + side, not sell/buy currency + direction-dependent rate.** A direction-dependent rate
  mixes 36.12 and 0.0277 for the same pair and breaks every z-score.
- **`base_amount`, not `buy_amount`.** With `side`, a buy amount would be USD for BUY and THB for
  SELL; summing volume per pair would add USD to THB.
- **Decimals, never floats.** Floating point can't represent money exactly.
- **`currency_pair` is a string, `side` is an enum.** Pairs can grow; an Avro enum that gains a
  symbol passes BACKWARD compatibility yet breaks lagging consumers. `{BUY, SELL}` never grows.
- **Not in the event:** `etl_loaded_at` (stamped by the sink in DuckLake — the producer can't know
  consume time); produce time (already in the Kafka record timestamp); labels (see §4).
- **Origin (production note):** real trades would come from an OLTP service via CDC or a
  transactional outbox — never dual writes. The simulator is log-first.

## 3. Kafka record key

> **Key = `trade_id`:** even partition load and partition-local dedupe (a retried trade lands in
> the same partition as the original). I give up per-pair ordering, which is acceptable because
> detection uses event-time windows with a watermark. I'd switch to `currency_pair` if a consumer
> needed strict per-pair order.

The key is not a field in the value: a Kafka record is `key | value | headers | timestamp`.
The key is serialized as a plain UTF-8 string.

## 4. Ground truth — `schemas/anomaly_episode.avsc` (topic `fx.trades.ground_truth`)

The detector is the student; ground truth is the answer key and is **never in the exam paper**.
It lives in a separate topic the detector doesn't subscribe to (no label leakage), and it is
modelled per **episode**, because collective anomalies (bursts) belong to a group of trades.

| Field | Meaning |
|---|---|
| `episode_id` | One injected anomaly |
| `anomaly_type` | `price_spike` · `fat_finger` · `volume_burst` · `account_burst` (string: types will grow) |
| `scope` / `scope_key` | `TRADE`→trade_id · `USER`→user_id · `PAIR`→currency_pair |
| `start_time` / `end_time` | Episode window (equal for single-trade anomalies) |
| `params` | Injection settings (deviation %, multiplier, trades/s…) |

**Matching rule (for Phase 4 grading):** an alert is a hit if it has the same `anomaly_type` and
`scope_key` and its window overlaps the episode window.
Precision = hit alerts ÷ all alerts. Recall = matched episodes ÷ all episodes.

**Labels come from the producer at injection time, never from aggregating the data** — otherwise
the detector would be graded with its own answers.

## 5. Sizing (from the Week 1 sizing card)

| # | Number | Assumption | → Decision |
|---|---|---|---|
| 1 | ~150 B/message | 7 Avro fields + 5 B header + key + record overhead | Message size isn't a constraint |
| 2 | 58.5 KB/s sustained · 2.34 MB/s burst | 500/s dev rate, 20k/s bursts | **One broker is enough**; throughput isn't the bottleneck |
| 3 | ~15 GB/day → ~105 GB / 7 days | 95% sustained, 5% burst | Bursts are ~2/3 of disk; **24 h retention in dev** + compression |
| 4 | 6 partitions | Throughput needs 1 (~10 MB/s per partition) | Count driven by **Spark parallelism**, room to grow |
| 5 | ~150 GB/month in DuckLake | Parquet ~3× vs Avro | **DuckLake has no retention** → dev cleanup policy needed |

**Measured (producer dry run / unit test):** Avro value ≈ **76 B including the 5 B header**, key
(UUID string) = **36 B**. The value is smaller than estimated (Avro decimals and timestamps are
compact varint/byte encodings; `user_id` is short), the key larger (UUID, not 20 chars). Net
≈ 120–130 B on the wire before compression — same order of magnitude, decisions unchanged.

## 6. Topics (dev profile)

| Topic | Partitions | RF | Retention | Notes |
|---|---|---|---|---|
| `fx.trades` | 6 | 1 | 24 h | `compression.type=producer` (producer uses lz4) |
| `fx.trades.ground_truth` | 1 | 1 | 7 days | tiny; kept longer for grading |

Created by the `init-topics` one-shot service in `docker-compose.yml`. **Phase 5 moves topic
definitions into OpenTofu.**

## 7. Producer design (`f1_project/fx_producer/`)

| Module | Responsibility |
|---|---|
| `domain.py` | `Trade` / `AnomalyEpisode` Pydantic models mirroring the `.avsc` contracts |
| `simulation/market.py` | Random-walk mid per pair; skewed pair mix (USD/SGD 30%); Zipf-skewed users ("whales" = natural false positives for naive per-user rules); log-normal sizes |
| `simulation/anomalies.py` | Injects anomalies **and** emits their ground-truth episodes |
| `simulation/chaos.py` | Late events (5–25 s = inside watermark, 40–120 s = beyond) and app-level duplicates |
| `simulation/simulator.py` | One tick: market move, normal flow, anomalies, chaos |
| `adapters/kafka_sink.py` | Avro serialization via Schema Registry; key = `trade_id`; `acks=all`, idempotent producer, lz4 |
| `adapters/dry_run_sink.py` | `--dry-run` console output and Avro sizing |
| `registry.py` | Registers both schemas explicitly, sets BACKWARD compatibility |
| `cli.py` | Rate-controlled loop (100 ms ticks), all knobs as flags, `--dry-run` without a broker |
| `verify.py` | Reads back the topic and checks the design assumptions |

**Injected anomalies (defaults)**

| Type | Scope | What happens | Default rate |
|---|---|---|---|
| `price_spike` | TRADE | one trade 1–3% off mid (mid unchanged) | 2/min |
| `fat_finger` | TRADE | one trade 50–200× normal size | 1/min |
| `volume_burst` | PAIR | one pair at 5–10× its flow for 10–30 s | 0.5/min |
| `account_burst` | USER | one ordinary user at 10–30 trades/s for 10 s | 0.5/min |

**Two kinds of duplicates (worth saying in an interview):** the idempotent producer removes
duplicates caused by *broker-level* retries; it cannot remove *application-level* duplicates
(same trade sent twice), which `simulation/chaos.py` injects on purpose. Only the sink's dedupe on
`trade_id` handles those.

**Known simplification:** the simulator writes trades and episodes to two topics — a dual write.
Acceptable for grading data in a simulator; a real system would use one system of record
(or Kafka transactions).

## 8. Validation checklist (run `make verify`)

- [ ] Real value size vs the ~150 B estimate (expect ~76 B value + 36 B key)
- [ ] Every message starts with magic byte `00` + a 4-byte schema id
- [ ] Partition counts roughly even across 6 partitions (key = `trade_id`)
- [ ] Repeated `trade_id`s present (deliberate duplicates)
- [ ] Ground-truth episodes by type
- [ ] Console (http://localhost:8080) decodes Avro using the registry
- [ ] **Schema evolution experiment** (feeds ADR-002):
  1. Add an optional field (`["null","string"]`, default null) → register → accepted under BACKWARD.
  2. Add a symbol to the `Side` enum → accepted under BACKWARD → consume with the *old* schema
     and observe the failure. That is the "registry approved it, production still broke" story.

## 9. Open items

- ADR-001 and ADR-002 (owner: Kids) — see `docs/adr/`.
- Phase 2 consumes `fx.trades` into DuckLake; `etl_loaded_at` is added there.
