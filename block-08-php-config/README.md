# Block 8 — Advanced PHP Client Configuration

**Day 2 · 15:15–16:45**

Producer config deep dive (batching, compression, timeouts). Consumer config
deep dive (offset control, rebalance strategy, fetch tuning). Operational
concerns — memory management, graceful shutdown, scaling, monitoring, Docker.

**Vault reference:** `PHP-Kafka-Research/Block-08-Advanced-PHP-Configuration.md`

## Goals

- Defend each non-default value in a production producer/consumer config.
- Implement a graceful-shutdown loop that does not lose in-flight work.
- Identify the three monitoring signals that catch the most outages early.

## Demos (`examples/`)

_To be filled in._ Candidate: reference producer and consumer configs (the
template referenced from the root README), plus a long-running consumer with
signal handling, memory cap, and structured log output.

## Exercises (`exercises/`)

_To be filled in._
