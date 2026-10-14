# ADR-001: Overall architecture — Kappa for alerts, Lambda for analytics

Status: To write (owner: Kids)    Date: —

DECISION: <write it>

1. DRIVING REQUIREMENT
   <!-- Hint: p95 < 5 s alerts vs < 60 s analytics freshness; late events beyond the 30 s watermark. -->

2. ALTERNATIVES CONSIDERED
   <!-- Pure Kappa · pure Lambda · hybrid -->

3. THE DECIDING AXIS

4. WHAT I GAVE UP
   <!-- Hint: what does running both cost you? (logic drift, two code paths) -->

5. WHAT WOULD CHANGE MY MIND

Production note to include: trades would originate in an OLTP service and reach the log via
CDC or a transactional outbox, never dual writes.
