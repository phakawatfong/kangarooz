# Architecture Decision Records

One file per decision, written with the 5-step template below. The magic is in steps 4 and 5.

**Constraint first:** write step 1 (the constraints and numbers that apply, from the [constraint trace](../design/00-overview.md#3-constraints--decisions-trace)) *before* listing any options, then let the constraints filter the alternatives.

| ADR | Decision | Phase | Status |
|---|---|---|---|
| [001](ADR-001-overall-architecture.md) | Overall architecture: Kappa alerts + Lambda analytics | 1 | Accepted |
| [002](ADR-002-broker-and-schema.md) | Broker + schema: Redpanda vs Kafka, Avro + Schema Registry | 1 | Accepted |
| [003](ADR-003-table-format.md) | Table format: DuckLake (Postgres catalog) vs Iceberg vs Delta | 2 | Accepted |
| [004](ADR-004-object-storage.md) | Object storage: Garage locally → S3 on AWS | 2 | Accepted |
| [005](ADR-005-stream-engine.md) | Stream engine: Spark Structured Streaming vs Flink | 3 | Accepted |
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
