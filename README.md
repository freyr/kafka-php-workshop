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
bin/console kafka:produce:sample [-c N] [--message-name NAME] [--pool N] [--interval MS]
```

It picks a random message from the catalog (`order.created`, `order.updated`,
`order.cancelled`, `payment.processed`, `inventory.reserved`) — or one pinned with
`--message-name` — keys each by an order id drawn from a reusable `--pool`, and
AVRO-encodes it, stamping the wire name and event id as the `message-name` and
`event-id` Kafka headers. With `-c N` it sends N and stops; without it, it streams
until Ctrl+C (flushing on exit). Schemas are **not** auto-registered — run
`kafka:schema:register --all` first.

**Consume into the orders projection** — one parametrized consumer reads a topic,
AVRO-decodes each record, routes it by its `message-name` header to a read-model DTO,
and dispatches it to a generic projection handler. Idempotency and transactions are
bus middleware *outside* the handler (a simplified command-bus shape).

```sh
bin/console kafka:consume:setup                                    # provision orders + processed_events (idempotent)
bin/console kafka:consume <topic> [-g GROUP] [--from WHERE] [--commit MODE] \
    [--interval MS] [--auto-commit-interval MS] [--static-membership] [-m MAX] [-t TIMEOUT_MS]
```

`--from` sets where a run starts, independent of the committed offset: `beginning`
(replay the whole log), `committed` (resume — the default), or `end` (only new
records). `--commit` selects the delivery mode:

| `--commit` | behavior |
|---|---|
| `per-message` | commit after each handled message — at-least-once (default) |
| `auto` | librdkafka commits in the background every `--auto-commit-interval` ms |
| `idempotent` | dedup on `event_id` + the `orders` upsert in one DB transaction, commit after — effectively-once |
| `readonly` | a throwaway group that never commits and never reaches the handler; prints each record's name/id from the headers, no decode |

`order.created/updated/cancelled` share the `enet.ecommerce.orders` topic, keyed by
order id. `--commit idempotent` records each `event_id` in `processed_events` in the
same transaction as the `orders` upsert, so a redelivered event is a no-op. A named
`-g` group keeps committed offsets across runs; omit `-g` for a throwaway group from
earliest. A named group selects one of two profiles to demonstrate rebalancing:
`consumer.dynamic` by default (dynamic membership — every join/leave triggers a
cooperative rebalance), or `consumer.at-least-once` with `--static-membership` (adds
`group.instance.id`, so a restart rejoins without a rebalance). Leave static off for
short CLI runs, where a dead static member would stall reassignment until
`session.timeout.ms` elapses. Both profiles are visible in `kafka:config:show`.

**Schema evolution** (Block 4) — flow is **check → register → produce** (schemas are
**not** auto-registered on produce):

```sh
bin/console schema:check    <type> <schema-file>   # CI gate; non-zero exit on incompatible
bin/console schema:register <type> [schema-file]   # register a subject (registry assigns id, enforces compat)
bin/console schema:register --all                  # bootstrap every routed subject on a fresh stack
bin/console schema:versions <type>                 # registered version lineage
```

Evolution demo schemas: `schemas/orders/evolution/OrderCreated-v2-compatible.avsc`
(optional field + default → PASS) and `-v2-breaking.avsc` (required field → FAIL).

**Config & operations** (Block 8) — raw `\RdKafka`, the callbacks have no higher-level surface:

```sh
bin/console config:show [--producer] [--consumer]                 # recommended settings: value · default · why
bin/console config:stats [topic] [-g GROUP] [-r SECS] [--slow MS] # raw consumer: lag/RTT/queue from the stats callback
bin/console topic:list                                            # list topics (raw metadata API)
bin/console topic:describe <topic>                                # partition count
```

**Admin shell scripts** in `bin/` (php-rdkafka has no admin API): `kafka-setup`,
`kafka-teardown` (provision/drop the full topic inventory), `topic-create`,
`topic-delete`, `topic-describe`, `group-describe`, `group-reset`, `group-delete`,
`partition-offsets`.

## Producer & serializer model

- **`Message`** (`src/Produce/Message.php`) is an abstract base. Each event is built
  through a static `create()` named constructor (`OrderCreated::create($orderId)`,
  `PaymentProcessed::create($orderId)`, …). The base supplies the envelope autonomously:
  `envelope()` = `{metadata: {event_id, timestamp}, ...payload}`. The partition key
  is a transport concern, kept out of the payload.
- **Wire name as a header.** The name lives in a `#[MessageName('order.created')]`
  class attribute, resolved once per class by `MessageNameResolver`, and stamped as
  the `message-name` Kafka **header** — so consumers route or skip without decoding.
- **Serializer.** `AvroSerializer` implements `MessageSerializer` (`src/Kafka/Serde/`):
  the Confluent wire format — magic byte + 4-byte schema id + Avro body, under
  RecordNameStrategy subjects. `SchemaRegistryClient` handles register/check/versions;
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
│   ├── Console/             # one class per command (#[AsCommand])
│   ├── Produce/             # Message base + events, MessageName, routing
│   ├── Consume/             # read-model DTOs + denormalizer
│   ├── Kafka/               # Client, Serde, Config, Callback, Runtime, Admin
│   └── Framework/           # Kernel + DI extension
├── schemas/                 # AVRO: orders/ (+evolution/) payments/ inventory/ audit/
├── tests/                   # PHPUnit suite
└── blocks/                  # facilitator notes (gitignored)
```

## Conventions

- **PSR-4 autodiscovery.** `config/services.yaml` autowires `Workshop\Kafka\` and
  `Workshop\Console\`; any `Command` is auto-tagged and registered. Adding a command
  means adding one class — no wiring edits.
- **Generic, parametrized commands over per-demo classes.** Add behavior with a flag,
  not a new class; reach for a new command only for a genuinely distinct operation.
- **Routing as data.** Topic/subject/schema and name→DTO maps live in YAML, not code.
- **Subjects use RecordNameStrategy** — the fully-qualified record name
  (`com.ecommerce.orders.order_created`), carrying no version marker; a breaking
  change mints a new subject. Each event type evolves on its own lineage.
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
