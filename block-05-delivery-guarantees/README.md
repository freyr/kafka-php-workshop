# Block 5 — Delivery Guarantees and Offset Management

**Day 2 · 09:00–10:30**

Producer acks and idempotency. Consumer offset strategies (at-most-once,
at-least-once, exactly-once). Manual offset management. Idempotent consumer
design — the pragmatic sweet spot for PHP.

**Vault reference:** `PHP-Kafka-Research/Block-05-Delivery-Guarantees.md`

## Goals

- Read `acks` + `enable.idempotence` + `max.in.flight` as one decision.
- Implement an idempotent consumer that survives crash-mid-handler.
- Recognize why true EOS is rarely worth it in PHP, and what to do instead.

## Demos (`examples/`)

_To be filled in._

## Exercises (`exercises/`)

_To be filled in._ Candidate: take a leaky at-least-once consumer, introduce
an idempotency table, kill the process mid-batch, verify no duplicates surface
in the downstream side-effect.
