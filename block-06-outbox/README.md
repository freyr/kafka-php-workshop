# Block 6 — Communication Patterns and Outbox

**Day 2 · 10:45–12:30**

The dual-write problem. Transactional Outbox + Debezium (recommended
pattern). Direct publishing for non-critical events. Enqueue / rdkafka
transport specifics. Choreography vs. orchestration for eCommerce workflows.

**Vault reference:** `PHP-Kafka-Research/Block-06-Communication-Patterns-Outbox.md`

## Goals

- See the dual-write failure mode reproduced under load.
- Read a Debezium outbox config and explain each field.
- Pick choreography vs. orchestration for two concrete workflows.

## Demos (`examples/`)

_To be filled in._ Candidate: Symfony + Doctrine outbox table, Debezium
connector publishing to Kafka, consumer downstream — kill the publisher mid-
flight and verify no event loss.

## Exercises (`exercises/`)

_To be filled in._
