# Demo 2 — Replay by Resetting Offsets

**Point**: the log keeps every record up to its retention bound; the consumer
group's *committed offset* is the only thing standing between you and a
replay. Move it backwards and the next poll re-delivers history.

## Run

```sh
./setup.sh                 # creates topic demo02-events (1 partition), produces 10 messages
./consume.sh               # reads all 10, commits offset 10 under demo02-group
./describe.sh              # current-offset=10, lag=0
./reset.sh earliest        # rewind committed offset to 0
./consume.sh               # replays the same 10 messages
./reset.sh 5               # rewind to a specific offset
./consume.sh               # replays only offsets 5..9
./cleanup.sh
```

## What to point out

- The log is unchanged across all steps. Only the **consumer group's
  bookmark** moves.
- `kafka-consumer-groups --reset-offsets` refuses to run while the group is
  active. This is why `consume.sh` exits when the topic drains — the group
  needs to be idle for the reset to succeed.
- `--to-earliest` is not the same as "offset 0". With a retention-bounded
  topic where the head has been truncated, earliest is whatever offset still
  exists on disk. Worth saying out loud.

## Variations to try live

- Reset to `latest` after a fresh `setup.sh`, then produce more messages —
  shows that a group can be parked at the end of the log and only see future
  records.
- Open Kafka UI (http://localhost:8080) and inspect the consumer group
  between steps; the UI shows the committed offset advancing and being
  rewound.
