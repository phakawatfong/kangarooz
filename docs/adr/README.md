# Architecture Decision Records

One file per decision, written with the 5-step template below. The magic is in steps 4 and 5.

| ADR | Decision | Phase | Status |
|---|---|---|---|
| [001](ADR-001-overall-architecture.md) | Overall architecture: Kappa alerts + Lambda analytics | 1 | Accepted |
| [002](ADR-002-broker-and-schema.md) | Broker + schema: Redpanda vs Kafka, Avro + Schema Registry | 1 | Accepted |
| 003 | Table format: DuckLake vs Iceberg vs Delta | 2 | — |
| 004 | Object storage: Garage vs SeaweedFS vs local FS | 2 | — |
| 005 | Stream engine: Spark vs Flink vs Quix Streams | 3 | — |
| 006 | Delivery guarantee: at-least-once + idempotent sink | 3 | — |
| 007 | Lambda drift + reconciliation | 4 | — |
| 008 | Partitioning, keys, hot pairs | 5 | — |
| 009 | IaC scope + environments | 5 | — |
| 010 | MCP access model | 6 | — |

## Template

```
# ADR-NNN: <title>
Status: Proposed | Accepted | Superseded    Date: YYYY-MM-DD

DECISION: <what you chose>

1. DRIVING REQUIREMENT — what constraint forces a choice here?
2. ALTERNATIVES CONSIDERED — the 2–3 realistic options.
3. THE DECIDING AXIS — "It came down to X vs Y, and X mattered more because…"
4. WHAT I GAVE UP — what the chosen option is worse at.
5. WHAT WOULD CHANGE MY MIND — "I'd revisit this if <condition>."
```
