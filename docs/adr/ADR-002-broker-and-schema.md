# ADR-002: Broker + schema — Redpanda, Avro, Schema Registry

Status: Accepted    Date: 2026-10-17    Owner: Kids

## Decision

**Redpanda** as the broker, with **Avro** messages validated through its built-in Schema Registry.

## 1. Driving requirements

**Messaging queue for streaming**
1. Data persistence
2. Data replay
3. Easy to set up
4. Open source (self-hosted, no paid licence)

**Event format**
1. Schema enforcement
2. Handles schema evolution
3. Compact message size
4. Supports decimal data (money)

Context from Phase 1 sizing: one laptop, 2.34 MB/s peak, one operator.

## 2. Alternatives considered

### Broker (filtered by persistence + replay + no loss)

| Option | Persistence + replay + no loss? | Strengths | Weaknesses |
|---|---|---|---|
| **Redpanda** | ✅ | Single binary, no JVM; Schema Registry and Console built in; low RAM; Kafka API | BSL licence; not real Kafka internals |
| **Apache Kafka (KRaft)** | ✅ | Truly open source, the industry standard, biggest ecosystem | JVM and heavier to run; needs a separate open-source registry (Confluent's isn't open source) |
| **Apache Pulsar** | ✅ | Tiered storage, multi-tenancy, geo-replication | Several components (brokers + BookKeeper); overkill for one laptop |
| **NATS JetStream** | ✅ | Very light | No Kafka API; weak Spark integration |
| **RabbitMQ (classic queues)** | ❌ | Simple work queues | Messages are deleted once acknowledged, so no replay |
| **Managed (MSK, Kinesis)** | ✅ | No operations work | Not self-hosted; a production option on AWS |

### Message format (filtered by compact + schema enforcement + evolution)

| Option | Meets it? | Strengths | Weaknesses |
|---|---|---|---|
| **Avro + registry** | ✅ | Compact (76 B measured); built-in decimal and timestamp types (money-safe); flexible schema-evolution rules; the default for Kafka and Spark (`from_avro`); no code generation in Python | Not human-readable without the registry; the registry becomes critical; the enum evolution trap |
| **Protobuf + registry** | ✅ | Compact, strongly typed, great for gRPC and services | No native decimal type (money needs strings or integer units); code generation |
| **JSON (+ JSON Schema)** | ⚠️ | Human-readable, easy to debug | Field names in every message (larger); schema checking is optional; decimals end up as floats or strings |

## 3–5. Broker: Redpanda

- **Deciding axis: operational weight vs licence and ecosystem.** It's one laptop, 2.3 MB/s and one operator, so ease of setup mattered most. Redpanda is a single binary with no JVM, and it has the Schema Registry and Console built in. Its Kafka API keeps the ecosystem (e.g. the Spark connector).
- **What I gave up:**
  - An OSI open-source licence (Redpanda is BSL, source-available)
  - Hands-on experience with Kafka's own internals
  - Kafka's full set of connectors
- **What would change my mind:**
  - A strict open-source policy → Apache Kafka (KRaft) with Apicurio or Karapace as the registry
  - Multi-region or multi-tenant needs → Pulsar
  - AWS production → MSK

## 3–5. Message format: Avro

- **Deciding axis: fit for a data pipeline vs developer convenience.**
  - Compact messages (76 B measured)
  - Exact decimals for money
  - Flexible schema-evolution rules
  - The default format for Kafka and Spark
- **What I gave up:**
  - Human readability (Console decodes via the registry)
  - The registry becomes critical infrastructure: delete a version and old messages become unreadable
  - The enum trap: adding a symbol passes BACKWARD compatibility but breaks lagging consumers. That's why `currency_pair` is a string and only `side` is an enum.
- **What would change my mind:**
  - Events also serving service-to-service calls (gRPC) → Protobuf
  - External partners consuming them → JSON Schema
