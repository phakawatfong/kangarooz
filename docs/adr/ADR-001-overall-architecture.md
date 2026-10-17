# ADR-001: Overall architecture — Kappa for alerts, Lambda for analytics

Status: Accepted    Date: 2026-10-17    Owner: Kids
Diagram: [app.diagrams.net](https://app.diagrams.net/?splash=0#G1SUDHafiNVQ9RdXx4oAUvopTkh_22hbRN#%7B%22pageId%22%3A%22nN-299ytY8vxR4gZ_9Z2%22%7D)

Topic names in the diagram map to the build: `event_topic` = `fx.trades`, `anomaly_topic` = `fx.alerts`.

## Context: requirements

**Functional**
1. Alert on trade anomalies per defined rules.
2. Analyse trade behaviour:
   - query historical trade behaviour per pair
   - produce analytical reports

**Non-functional**
1. Reliability
   - No acknowledged trade is lost (at-least-once delivery, dedupe on `trade_id`).
   - Trades more than 30 s late must still count in reports.
2. Regulatory: trading is global, so data must comply with each region's rules (data residency).
3. Performance
   - Alerts: p95 under 5 s (needs a streaming path).
   - Analytics freshness: ~60 s.

## Decision

- **Kappa for alerts:** `detector-worker` reads `event_topic`, writes to `anomaly_topic`, and `notifier-worker` sends the alert.
- **Lambda for analytics** in the lakehouse:
  - raw trades and fired alerts land as an immutable record (`raw_event`, `anomaly_event`)
  - a scheduled batch model recomputes exact results, late trades included
  - a speed view, computed as an incremental micro-batch over recent raw events, gives fresh, approximate results
  - results are served by use case: reports read batch; freshness dashboards and the real-time app read speed

## 1. Driving requirements
- Alerts: p95 under 5 s
- Analytics freshness: ~60 s
- Trades more than 30 s late must still count in reports
- No acknowledged trade is lost (at-least-once delivery, dedupe on `trade_id`)
- Replay for rule backtesting

## 2. Alternatives considered
- **Pure Kappa:** one stream computes alerts and aggregates. Rejected: stream aggregates drop trades that arrive after the 30 s watermark, so reports would be wrong. A longer watermark means more state to hold and slower results.
- **Pure Lambda:** batch plus speed for everything, alerts included. Rejected: an alert is a decision made at a point in time, so recomputing it in batch adds nothing except duplicated logic.
- **Hybrid (chosen).**

## 3. Deciding axis: timeliness vs completeness
An alert that's late but correct is useless, so alerts optimise for timeliness. A report that's fast but wrong is worse than one that's slower but exact, so reports optimise for completeness. One path can't optimise both, so each use case gets the path that matches it.

## 4. What I gave up
- **Two code paths for the same metrics** (speed view and batch model), which can drift apart. Mitigation: shared rule definitions, plus a reconciliation job that measures the speed-vs-batch difference.
- **Two serving stores.** Consumers must know which one to read, and a fresh dashboard can disagree with a report until the next batch run. Mitigation: label freshness on each dashboard.
- **Late-trade corrections appear only after the next batch run.**
- **More moving parts than pure Kappa** (a scheduler, two views), so more to operate.
- **Production risk accepted:** no outbox, so the trade-app writes the DB and the queue separately. That can cause phantom alerts (DB rolled back after publish) or missed alerts (DB committed, publish failed). The outbox is a known improvement.

## 5. What would change my mind
- If late trades were negligible, or reports could tolerate them, I'd drop the batch layer and go pure Kappa, with analysts querying raw data.
- If reconciliation regularly showed drift between speed and batch, I'd put both on one engine or move to Kappa.
- If alerts started triggering automatic actions (e.g. blocking trades), I'd implement the outbox first.

*Production note:* trades would originate in an OLTP service and reach `event_topic` via a transactional outbox, never dual writes. The build is log-first: the producer writes only to Redpanda, so it has no dual write.
