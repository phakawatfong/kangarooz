# ADR-004: Object storage — Garage locally → S3 on AWS

Status: Accepted    Date: 2026-10-17    Owner: Kids

## Decision

**Garage locally → S3 on AWS.** Same S3 API, so switching is a config change (endpoint + credentials).

## 1. Driving requirement

Must-haves:
1. S3-compatible API
2. Move to the cloud (AWS S3) without code changes

Deciding requirement: **self-hosted at zero cost, light enough for a 16 GB laptop** — this is what rules out S3 for local development.

## 2. Alternatives considered

| Option | Strengths | Weaknesses |
|---|---|---|
| **Garage** | Lightweight, single container, S3 API; supports geo-distributed clusters | Implements a subset of the S3 API (e.g. no object versioning / object lock); small community |
| **SeaweedFS** | S3 gateway, scales well | More components to run (master + volume + filer servers) |
| **MinIO CE** | Most complete S3 compatibility | Community Edition archived (Apr 2026) → no fixes or security patches; commercial edition is paid |
| **Local filesystem** | Simplest | No S3 API → local and cloud code paths differ (fails must-have #2) |
| **AWS S3** | Managed, durable, scales | Costs money; not local — **it's the cloud target, not a competitor** |

## 3. Deciding axis

**It came down to operational simplicity vs S3 API completeness.** Simplicity mattered more: Garage is a single container that's light on RAM, and the lakehouse only uses basic S3 calls (PUT / GET / LIST / multipart upload) that Garage supports. SeaweedFS needs master + volume + filer servers.

## 4. What I gave up

1. Partial S3 API (no versioning / object lock, limited lifecycle rules)
2. Durability — single node on a laptop disk, no replication: if the disk dies, the lake is gone
3. Smaller community
4. Local ≠ cloud auth — Garage uses its own access keys, not IAM roles/policies, so the security model is only exercised on AWS

## 5. What would change my mind

1. Need object versioning → **S3**
2. Need auditability or regulatory retention (object lock) → **S3**
3. Laptop disk can't hold the data (~150 GB/month with no retention) → **S3**, or add retention
4. Need a shared environment for a team → skip local, **go straight to S3**
5. Spike shows Garage lacks an S3 call DuckDB/Spark needs → **SeaweedFS**
