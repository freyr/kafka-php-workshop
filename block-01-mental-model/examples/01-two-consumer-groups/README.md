# Demo 1 — Two Consumer Groups, One Topic

**Point**: Kafka is a log, not a queue. Consuming does not remove the message.
Two consumer groups read the same partitions and maintain their own offsets.

## Run

```sh
./setup.sh                 # creates topic demo01-events (1 partition), produces 5 messages
./consume.sh group-a       # reads all 5 messages, commits offsets under group-a
./consume.sh group-b       # reads the same 5 messages, commits offsets under group-b
./describe.sh group-a      # shows current-offset / log-end-offset / lag for group-a
./describe.sh group-b      # same for group-b
./cleanup.sh
```

## What to point out

- Both groups receive the **same** messages. Reading is not destructive.
- After both runs, `describe.sh` shows two independent rows in
  `__consumer_offsets` — same topic-partition, different `current-offset`
  histories per group.
- Re-running `consume.sh group-a` returns nothing — its offset is at the log
  end. This is the difference between **physical position in the log** and
  **per-group bookmark**.

## Variations to try live

- Run `consume.sh group-a` from a second terminal **while** `setup.sh` reseeds
  data: the consumer keeps the same offset bookmark, so it sees only the new
  messages.
- Add `--from-beginning` mentally: the flag means "if no committed offset
  exists, start at the earliest" — it does **not** force a replay for an
  existing group.
