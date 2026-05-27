# Block 7 — Retry Strategies and Error Handling

**Day 2 · 13:30–15:00**

Consumer error categories. Retry topics with increasing delays. Dead Letter
Topics — design, monitoring, recovery. Partition blocking prevention.
PHP-specific error handling (memory, signals, Doctrine cleanup).

**Vault reference:** `PHP-Kafka-Research/Block-07-Retry-Error-Handling.md`

## Goals

- Classify errors (retryable / poison / config) and route each correctly.
- Build a retry-topic chain with bounded delays without blocking partitions.
- Design a DLT recovery workflow operators will actually run.

## Demos (`examples/`)

_To be filled in._ Candidate: consumer reading from `orders.events`, on
failure publishing to `orders.events.retry.5s` / `…30s` / `…5m`, then to
`orders.events.dlt` with an investigation tool to inspect and re-publish.

## Exercises (`exercises/`)

_To be filled in._
