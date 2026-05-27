# Block 2 — Topic Architecture

**Day 1 · 10:45–12:30**

Topic design principles (per event type, per entity, per bounded context).
Partitioning strategy — choosing partition keys, ordering guarantees, how many
partitions. SaaS multi-tenancy considerations. Naming conventions.

**Vault reference:** `PHP-Kafka-Research/Block-02-Topic-Architecture.md`

## Goals

- Pick the right granularity for a topic in an eCommerce SaaS context.
- Choose partition keys that preserve the ordering guarantees you actually need.
- Reason about multi-tenancy: tenant-per-partition vs. tenant-in-key.

## Demos (`examples/`)

_To be filled in._

## Exercises (`exercises/`)

_To be filled in._ Candidate: design topics + partition strategy for a slice of
the client's eCommerce domain (orders, inventory, fulfillment events).
