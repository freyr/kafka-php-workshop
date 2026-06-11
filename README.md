# PHP + Kafka Workshop

Runnable counterpart to a 2-day Kafka-for-PHP-teams workshop: demo CLI commands,
AVRO schemas, and a Docker stack (Kafka in KRaft mode, Confluent Schema Registry,
Kafka UI) plus a PHP 8.4 container. All Kafka I/O is **raw `php-rdkafka`** (no
queue-interop layer), wired through `symfony/console` + `symfony/dependency-injection`
with YAML config and `symfony/dotenv`. Per-block facilitator notes live locally in
`blocks/` (gitignored — pulled from the Consulting vault).

## Stack

```sh
make create                                   # start kafka + schema registry + kafka-ui (detached)
docker compose run --rm php composer install  # first run: install PHP deps in the php container
make destroy                                  # tear down and wipe the kafka-data volume
```

| Service         | URL / port                | Notes                          |
|-----------------|---------------------------|--------------------------------|
| Kafka broker    | `localhost:9092`          | KRaft mode, single broker      |
| Schema Registry | `http://localhost:8081`   | Confluent SR, AVRO schemas     |
| Kafka UI        | `http://localhost:8080`   | Kafbat UI                      |
| PHP console     | `bin/console list`        | run via `docker compose run --rm php` |

## Commands

Run everything through the php container:

```sh
docker compose run --rm php bin/console <command> [args]
```

**Produce AVRO events** — `kafka:produce:sample` streams enveloped AVRO against Schema Registry:

```sh
bin/console kafka:produce:sample [-c N] [--message-name NAME] [--pool N] [--interval MS] [--profile NAME]
```

It picks a random message from the catalog (`order.created`, `order.updated`,
`order.cancelled`, `order.audited`, `demo.order.evolved`) — or one pinned with
`--message-name` — keys each by an order id drawn from a reusable `--pool`, and
AVRO-encodes it, stamping the wire name and event id as the `message-name` and
`event-id` Kafka headers. With `-c N` it sends N and stops; without it, it streams
until Ctrl+C (flushing on exit). Schemas are **not** auto-registered — run
`kafka:schema:register --all` first.

`--profile` picks the producer's reliability tuning: `idempotent` (default —
`enable.idempotence` + `acks=all` + bounded in-flight: ordered, no retry duplicates)
or `simple` (untuned librdkafka defaults, which can reorder or duplicate on retry).

**Consume into the orders projection** — one parametrized consumer reads a topic,
AVRO-decodes each record, routes it by its `message-name` header to a read-model DTO,
and dispatches it to a generic projection handler. Idempotency and transactions are
bus middleware *outside* the handler (a simplified command-bus shape).

```sh
bin/console kafka:consume:setup                                    # provision orders + processed_events (idempotent)
bin/console kafka:consume <topic> [--profile NAME] [-g GROUP] [--from WHERE] [--idempotent] \
    [--interval MS] [-m MAX] [--ttl MS] [--drain]
```

`--profile` selects the consumer's Kafka configuration. It bundles the commit mode
and the rebalancing strategy — the two move together, so one name picks both:

| `--profile` | commit | rebalancing | membership | handles? |
|---|---|---|---|---|
| `ephemeral` (default) | never | — (lone member) | throwaway group | no — inspects only, prints each record's name/id from the headers, no decode |
| `default` | librdkafka background auto-commit | eager `range,roundrobin` (stop-the-world) | dynamic | yes |
| `modern` | explicit, after each handler | cooperative-sticky (incremental) | static (`group.instance.id`) | yes |

`--from` sets where a run starts, independent of the committed offset: `beginning`
(replay the whole log), `committed` (resume — the default), or `end` (only new
records). `ephemeral` ignores `--from` and always reads from the beginning.

By default the consumer **tails** — it polls forever, stopping only on Ctrl+C. Three
opt-in conditions bound a run, and any of them can combine: `-m/--max N` (stop after N
messages), `--ttl MS` (a lifetime cap — stop once the consumer has lived this long,
the time analogue of `--max`), and `--drain` (stop at the first empty poll — read the
backlog to the end, then exit). `--interval MS` throttles the pause between messages.

`--idempotent` is **orthogonal** to the profile: it records each `event_id` in
`processed_events` in the same DB transaction as the `orders` upsert, so a
redelivered event is a no-op — effectively-once. It is a handler/DB concern, not a
Kafka setting, so it layers onto `default` or `modern` (and is ignored by
`ephemeral`, which never reaches the handler).

Because dedup makes redelivery harmless, `modern --idempotent` also commits
**asynchronously** in the poll loop (no per-message broker round-trip) and does a
single **synchronous** commit on close to make the final offset durable — markedly
faster than the synchronous per-message commit `modern` uses on its own. It stays
at-least-once: a crash can lose an in-flight async commit and redeliver, but the
dedup turns that into a no-op. (The close-time commit targets the last *handled*
message, so a message whose handler threw is never committed and is reprocessed.)

`order.created/updated/cancelled` share the `enet.ecommerce.orders` topic, keyed by
order id. The `default`-vs-`modern` pair is the rebalancing contrast: eager revokes
the whole assignment on every join/leave, cooperative-sticky moves only the affected
partitions and a static member rejoins without a rebalance at all. A named `-g` group
keeps committed offsets across runs (default/modern); `ephemeral` always joins a
fresh throwaway group, so omit `-g` there.

**Schema evolution** (Block 4) — flow is **check → register → produce** (schemas are
**not** auto-registered on produce):

```sh
bin/console kafka:schema:check    <type> <schema-file>   # compatibility vs the LATEST version; non-zero exit on fail
bin/console kafka:schema:register <type> [schema-file]   # register a subject (registry assigns id, enforces the level)
bin/console kafka:schema:register --all                  # bootstrap every routed subject on a fresh stack
bin/console kafka:schema:versions <type>                 # registered version lineage
bin/console kafka:schema:compat   <type> [level]         # show or set the subject's compatibility level
bin/console kafka:schema:delete   <type> [version]       # delete a registered version (default latest)
bin/console kafka:consume <topic> --print [--reader writer|latest]   # also dump each record's RAW decoded fields (pre-DTO)
```

The exercise has each participant evolve a **dedicated throwaway event** —
`demo.order.evolved` (a deliberately flat record on its own topic `enet.demo.orders`,
isolated from the real `OrderCreated`) — *in place, as in production*, and watch what
the registry does and does not protect. Schemas are **not** shipped pre-evolved:
`schemas/demo/OrderEvolved.avsc` starts flat and you add the field yourself.

1. **A registry check is not a code check.** Add a field *with a default* to the
   schema and register it — BACKWARD-compatible, accepted. Produce *without* updating
   the message class: it does **not** crash. The Avro writer silently substitutes the
   default, so those records carry placeholder data the registry was perfectly happy
   with. The drift only surfaces downstream — the prod failure mode.
2. **A reader schema is what unifies a mixed-version stream.** Once old and new records
   coexist, add the field to the read-model DTO and consume. The evolved event's handler
   prints the *DTO* fields (whatever the read model captured); adding `--print` also dumps
   the *raw decoded record* (the wire fields, before the DTO) — so you see wire vs DTO side
   by side. With `--reader writer`, `kafka:consume enet.demo.orders --print --reader writer`
   shows each old record's raw fields and then *skips* it (the DTO can't be built — the
   field isn't on the wire), while `--reader latest` resolves every record against the
   latest schema, filling the default into the old ones so they all hydrate. The reader
   schema, not the writer, is what lets one consumer read a mixed-version log as a single
   shape.
3. **Expand-contract can't retire a field on a log.** Drop the default and re-register:
   it **passes** under the default non-transitive `BACKWARD` (the registry only compares
   to the latest version, which still has the field) but is **rejected** under
   `BACKWARD_TRANSITIVE`. With the no-default schema live, `--reader latest` now breaks on
   the old records — they have no value and no default, and the immutable log is exactly
   what a database migration would have rewritten.

See the full step-by-step facilitator runbook in the Consulting vault
(`Block-04-Demo-Runbook.md`).

**Transactional outbox** (Block 6) — one generic `outbox` table, two relay flavors.
The business write and its event are committed in **one MySQL transaction**; a relay
publishes the event to Kafka afterwards, so the broker is never on the business
write's critical path:

```sh
bin/console outbox:setup [--fresh] [--format json|avro]   # provision the outbox (and ensure orders)
bin/console outbox:place [-c N] [--message-name NAME] [--pool N] [--fail] [--format json|avro]
bin/console outbox:relay [--batch N] [--interval MS] [--once] [--profile NAME]
```

`outbox:place` simulates the producing service: per write it mutates the `orders`
row **and** appends the event (`order.created/updated/cancelled`, the same JSON
envelope the AVRO path uses) to `outbox` — atomically, no Kafka client involved.
`--fail` crashes each transaction right before COMMIT: afterwards neither table has
a trace, the beat that produce-then-commit cannot replicate. The relay is either:

- **PHP polling relay** — `outbox:relay` is a long-running process draining pending
  rows in insertion order to `enet.ecommerce.outbox.<aggregate_type>` (key =
  `aggregate_id`, headers = `message-name`/`event-id`), and stamping `published_at`
  only after the broker acked the whole batch — at-least-once, mark-after-ack.
- **Debezium CDC** — `bin/debezium-register` registers a MySQL binlog connector whose
  EventRouter SMT routes the same rows to the same topics, no PHP process at all
  (`config/debezium-outbox-connector.json`).

Run one flavor at a time (they'd double-publish the same rows); watch the result
with `make outbox-watch`.

**AVRO payload variant** — `--format avro` (on both `outbox:setup` and
`outbox:place`; switching formats needs `--fresh`) makes the *application* encode
the envelope to Confluent-framed AVRO via the same `MessageSerializer` as
`kafka:produce:sample`, stored in a binary column. Both relays then move opaque
bytes — the PHP relay unchanged, Debezium via
`config/debezium-outbox-connector-avro.json` (`make debezium-register-avro`:
no JSON expansion, `ByteArrayConverter` pass-through). Relayed records are
**byte-identical** to directly produced ones — same registered schemas, same
registry governance — so `kafka:consume enet.ecommerce.outbox.Order` decodes and
projects them like any AVRO topic. The trade-off vs the JSON format: the payload
is no longer readable in MySQL, and schemas must be registered before placing.
Connect-level Avro (deriving schemas from expanded JSON) is deliberately not
used: inferred schemas vary per record once nulls are dropped — the registry
deserves better. Full facilitator runbook: `Block-06-Demo-Runbook.md` in the
Consulting vault.

**Topic & group operations** — shell scripts in `bin/` (php-rdkafka has no admin API):
`kafka-setup` / `kafka-teardown` (provision/drop the full topic inventory),
`topic-create`, `topic-delete`, `topic-describe`, `group-describe`, `group-reset`,
`group-delete`, `partition-offsets`, plus Debezium CDC helpers `debezium-register`,
`debezium-status`, `debezium-delete`.

## Producer & serializer model

- **`Message`** (`src/App/Producer/Message.php`) is an abstract base. Each event is built
  through a static `create()` named constructor (`OrderCreated::create($orderId)`,
  `OrderAudited::create($orderId)`, …). The base supplies the envelope autonomously:
  `envelope()` = `{metadata: {event_id, timestamp}, ...payload}`. The partition key
  is a transport concern, kept out of the payload.
- **Wire name as a header.** The name lives in a `#[MessageName('order.created')]`
  class attribute, resolved once per class by `MessageNameResolver`, and stamped as
  the `message-name` Kafka **header** — so consumers route or skip without decoding.
- **Serializer.** `AvroSerializer` implements `MessageSerializer` (`src/Kafka/Serde/`):
  the Confluent wire format — magic byte + 4-byte schema id + Avro body, under
  RecordNameStrategy subjects by default (the Block 9 catalog route uses
  TopicNameStrategy (`<topic>-value`) for stock Kafka Connect interop).
  `SchemaRegistryClient` handles register/check/versions;
  it never touches the wire format.
- **Routing is data.** `config/producers.yaml` maps message-name →
  `{topic, subject, schema}` (`MessageRouting`/`Route`); `config/consumers.yaml`
  maps message-name → read-model DTO (`DtoRouting`).

## Layout

```
.
├── compose.yaml             # Kafka + Schema Registry + Kafka UI + PHP
├── Dockerfile               # PHP 8.4 + librdkafka + php-rdkafka
├── .env                     # KAFKA_BROKERS, SCHEMA_REGISTRY_URL (.env.local overrides, gitignored)
├── bin/console              # Symfony Console entry; plus admin shell scripts
├── config/
│   ├── services.yaml        # DI: PSR-4 autodiscovery, command tagging
│   ├── producers.yaml       # produce-side routing (name → type/topic/subject/schema)
│   └── consumers.yaml       # consume-side routing (name → DTO)
├── src/
│   ├── App/                 # the application: producer + consumer + CLI
│   │   ├── Console/         # one class per command (#[AsCommand])
│   │   ├── Producer/        # Message base + events, MessageName, routing
│   │   ├── Consumer/        # read-model DTOs + denormalizer
│   │   ├── Outbox/          # Block 6: transactional placer + relay's table gateway
│   │   └── Catalog/         # Block 9: ProjectionChange message, placer, repository, schema installer
│   ├── Kafka/               # the "plugin": Client, Serde, Config, Callback, Runtime
│   └── Framework/           # Kernel, DI extension, Db (DBAL connection + schema)
├── schemas/                 # AVRO: orders/ audit/ demo/ (Block 4 evolution)
│   └── catalog/             # Block 9: ProjectionChange.avsc (TopicNameStrategy subject)
├── tests/                   # PHPUnit suite
└── blocks/                  # facilitator notes (gitignored)
```

## Conventions

- **PSR-4 autodiscovery.** `config/services.yaml` autowires `Workshop\Kafka\` and
  `Workshop\App\Console\`; any `Command` is auto-tagged and registered. Adding a command
  means adding one class — no wiring edits.
- **Generic, parametrized commands over per-demo classes.** Add behavior with a flag,
  not a new class; reach for a new command only for a genuinely distinct operation.
- **Routing as data.** Topic/subject/schema and name→DTO maps live in YAML, not code.
- **Subjects use RecordNameStrategy by default** — the fully-qualified record name
  (`com.ecommerce.orders.order_created`), carrying no version marker; a breaking
  change mints a new subject. Each event type evolves on its own lineage. The
  Block 9 catalog route uses TopicNameStrategy (`<topic>-value`) for stock Kafka
  Connect interop.
- **Config in `.env`** (`KAFKA_BROKERS`, `SCHEMA_REGISTRY_URL`); per-user overrides
  in `.env.local`.
- Polish-language comments are fine in exercises; identifiers and shipped demos stay
  in English.

## Validation

All work must pass three gates before it is done:

```sh
docker compose run --rm php composer ecs       # coding standard
docker compose run --rm php composer phpstan   # static analysis
docker compose run --rm php composer test      # phpunit
```
