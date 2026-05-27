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

_To be filled in._ Candidate demos:

1. Tail a topic with `kafka-console-consumer` from two consumer-group IDs and
   watch the offset advance independently.
2. Reset a consumer group to an earlier offset and replay.
3. Produce with and without keys; observe partition distribution.

## Exercises (`exercises/`)

_To be filled in._ Candidate tasks:

1. Predict which partition a set of keyed messages will land on; verify with a
   PHP consumer printing `(partition, offset, key)`.
2. Two student groups join the same consumer group — observe partition
   assignment as the group rebalances.
