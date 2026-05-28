# Block 1 — Kafka Mental Model for PHP Teams

**Day 1 · 09:00–10:30**

Queue-based vs. log-based thinking. Consumer groups, offsets, replay.
Partitions as unit of parallelism. Where PHP fits — the request-lifecycle
mismatch and its consequences.

**Vault reference:** `PHP-Kafka-Research/Block-01-Kafka-Mental-Model.md`

## Goals

- Internalize the log abstraction (append-only, durable, replayable).
- Map consumer groups → partitions → offsets without ambiguity.
- See the PHP request-lifecycle mismatch live (worker model vs. request model).

## Demos (`examples/`)

CLI-driven, all shelling out to the `kpw-kafka` container. See
`examples/README.md` for the entry point.

1. **`01-two-consumer-groups/`** — two consumer groups read the same topic
   and maintain independent offsets. Makes "consuming is not destructive"
   concrete.
2. **`02-offset-reset-replay/`** — commit offsets, rewind them with
   `kafka-consumer-groups --reset-offsets`, replay. Separates "log" from
   "group bookmark."
3. **`03-keyed-vs-unkeyed/`** — produce with null and explicit keys to a
   4-partition topic, then inspect end-offset distribution and key →
   partition mapping. Grounds the per-entity-order contract.

## Exercises (`exercises/`)

_To be filled in._ Candidate tasks:

1. Predict which partition a set of keyed messages will land on; verify with a
   PHP consumer printing `(partition, offset, key)`.
2. Two student groups join the same consumer group — observe partition
   assignment as the group rebalances.
