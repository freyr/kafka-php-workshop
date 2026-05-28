# Demo 3 — Keyed vs. Unkeyed Production

**Point**: the partition is chosen by the producer, not the broker. Null
keys spread across partitions (sticky / round-robin); explicit keys are
hashed, so the same key always lands on the same partition. This is the
mechanism behind per-entity ordering.

## Run

```sh
./setup.sh                 # creates topic demo03-events with 4 partitions
./produce-unkeyed.sh       # produces 20 messages with null keys
./inspect.sh               # end offset per partition — roughly even spread
./produce-keyed.sh         # produces 20 messages keyed by alice/bob/carol/dave
./inspect.sh               # each key sticks to one partition; spread is now stable
./show-keys.sh             # reads back with key+partition printed, makes the mapping visible
./cleanup.sh
```

## What to point out

- After `produce-unkeyed.sh`, all four partitions have non-zero offsets but
  the distribution is not perfectly even. The default partitioner is
  **sticky** — it picks one partition per producer batch — so short bursts
  may concentrate in one partition.
- After `produce-keyed.sh`, the same key always maps to the same partition.
  Re-running the script does not change the mapping. This is the contract
  that lets you preserve per-entity order.
- 4 partitions = max parallelism of 4 consumers in one group. Partition
  count is a deployment-time decision; revisit in Block 2.

## Variations to try live

- Drop partitions from 4 to 1 (`./cleanup.sh && PARTITIONS=1 ./setup.sh`)
  and re-run `produce-keyed.sh` — global order, zero parallelism. Names the
  tradeoff.
- Re-key one of the producers (`./produce-keyed.sh` accepts an optional
  list) and watch how the partition assignment shifts. Useful to make the
  hash → modulo arithmetic feel concrete.
