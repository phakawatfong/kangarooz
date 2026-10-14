# ADR-002: Broker + schema — Redpanda, Avro, Schema Registry

Status: To write (owner: Kids)    Date: —

DECISION: <write it>

1. DRIVING REQUIREMENT
   <!-- Hint: single laptop, Kafka API, schema evolution, sizing showed 2.34 MB/s peak. -->

2. ALTERNATIVES CONSIDERED
   <!-- Redpanda vs Apache Kafka (KRaft); Avro vs JSON vs Protobuf -->

3. THE DECIDING AXIS

4. WHAT I GAVE UP
   <!-- Hint: BSL licence (source-available, not OSI open source); Kafka-internals depth. -->

5. WHAT WOULD CHANGE MY MIND

Points to cover (you reasoned through all of these in Week 1):
- Why Avro over JSON: 5-byte header (magic + schema id) → no field names on the wire.
- Registry as critical infrastructure: delete a schema version → old messages unreadable.
- String vs enum: `currency_pair` string, `side` enum — and why.
- The BACKWARD trap: adding an enum symbol passes the check but breaks lagging consumers.
  Evidence: the schema evolution experiment in the Phase 1 validation checklist.
