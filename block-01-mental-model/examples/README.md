# Block 1 — Demos

Three CLI-driven demos that ground the log/partition/offset abstractions in
something visible. All scripts shell out to the `kpw-kafka` container, so they
work from any directory once the stack is up:

```sh
docker compose up -d
```

| # | Folder                          | Concept                                       |
|---|---------------------------------|-----------------------------------------------|
| 1 | `01-two-consumer-groups/`       | Consumer groups read independently            |
| 2 | `02-offset-reset-replay/`       | Replay by resetting committed offsets         |
| 3 | `03-keyed-vs-unkeyed/`          | Keys decide the partition; null keys round-robin |

Each demo is self-contained: `setup.sh` creates the topic and seeds data,
`cleanup.sh` deletes the topic and any consumer groups. Re-running `setup.sh`
is safe — it recreates state from scratch.

All scripts assume Kafka is reachable on `kafka:29092` from inside the
`kpw-kafka` container (its internal listener). No host-side Kafka CLI is
required.
