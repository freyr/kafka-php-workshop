# Block 4 — Schema Registry and Versioning

**Day 1 · 15:15–17:00**

Subject naming strategies. Confluent wire format. Compatibility rules
(BACKWARD, FORWARD, FULL, NONE) with concrete examples. Schema evolution
workflow. CI/CD integration. PHP tooling for Schema Registry.

**Vault reference:** `PHP-Kafka-Research/Block-04-Schema-Registry-Versioning.md`

## Goals

- Pick a subject naming strategy and understand its trade-offs.
- Walk through a concrete BACKWARD-compatible change end-to-end.
- See the Confluent wire format on the byte level (magic byte + schema id).

## Demos (`examples/`)

_To be filled in._ Candidate: register schema v1, produce a record, evolve to
v2 (add optional field), produce v2, consume both versions with the same
reader.

## Exercises (`exercises/`)

_To be filled in._ Candidate: classify five proposed schema changes against
BACKWARD / FORWARD / FULL compatibility and verify with the Registry's
compatibility endpoint.
