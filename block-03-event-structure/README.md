# Block 3 — Event Structure Design

**Day 1 · 13:30–15:00**

Domain events vs. integration events vs. commands. Fat vs. thin events.
Envelope pattern. Required metadata fields. AVRO-specific schema design —
union types, logical types, enum gotchas.

**Vault reference:** `PHP-Kafka-Research/Block-03-Event-Structure-Design.md`

## Goals

- Distinguish domain/integration/command shapes and pick the right one.
- Design an envelope with the metadata your operators will actually need.
- Avoid the AVRO traps (nullable defaults, enum evolution, logical types).

## Demos (`examples/`)

_To be filled in._

## Exercises (`exercises/`)

_To be filled in._ Candidate: write an AVRO schema for one of the client's
real events and stress-test it against three plausible future changes.
