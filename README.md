# PHP + Kafka Workshop

Working repo for a 2-day Kafka-for-PHP-teams workshop. Holds runnable demo
commands, AVRO schemas, slide assets, and a Docker stack with Kafka,
Confluent Schema Registry, Kafka UI, MySQL, Kafka Connect (Debezium CDC for
the Block 6 outbox), and a PHP 8.4 container with
`php-rdkafka` (extension) + [`enqueue/rdkafka`](https://github.com/php-enqueue/rdkafka)
(queue-interop wrapper used by all PHP commands) + `doctrine/dbal` (the
Block 5 idempotency demo), wired through `symfony/console` +
`symfony/dependency-injection` (with PSR-4 autodiscovery via `symfony/config`)
and configured through `symfony/dotenv`.

Per-block facilitator notes live locally in `blocks/` (gitignored — pulled
from the Consulting vault `PHP-Kafka-Research/`). This repo is the runnable
counterpart to that material.

## Layout

```
.
├── compose.yaml             # Compose stack: Kafka + SR + UI + MySQL + Connect + PHP
├── Dockerfile               # PHP 8.4 + librdkafka + php-rdkafka + pdo_mysql image
├── .env                     # Workshop defaults (loaded by bin/console)
├── bin/
│   ├── console              # Symfony Console entry — all PHP commands
│   ├── topic-create / topic-delete / topic-describe
│   ├── topic-map / topic-map-delete   # create/tear down the eCommerce topic map (Block 2)
│   ├── group-describe / group-reset / group-delete
│   ├── partition-offsets
│   └── debezium-register / debezium-status / debezium-delete   # Debezium outbox connector (Block 6)
├── config/
│   ├── services.php         # DI definitions; PSR-4 autodiscovery of services
│   └── debezium-outbox-connector.json   # Debezium MySQL + Outbox Event Router config (Block 6)
├── src/
│   ├── Kernel/              # KafkaContextFactory, AvroEventSerializer, SchemaRegistryClient,
│   │                        #   Database + IdempotencyStore + SideEffectStore (Block 5),
│   │                        #   OutboxStore (Block 6), enums
│   └── Console/             # One class per command, self-describing names
├── schemas/                 # AVRO schemas: common/ + orders/ payments/ inventory/
├── tests/                   # PHPUnit suite
└── blocks/                  # Per-block facilitator notes (gitignored)
```

## Running the stack

```sh
make create                                   # bring up kafka + schema registry + kafka-ui + mysql + connect (detached)
docker compose run --rm php composer install  # first-time: install PHP deps in the php container
```

Services exposed on the host:

| Service          | URL / port                  | Notes                                  |
|------------------|-----------------------------|----------------------------------------|
| Kafka broker     | `localhost:9092`            | KRaft mode, single broker              |
| Schema Registry  | `http://localhost:8081`     | Confluent SR, `AVRO` schemas           |
| Kafka UI         | `http://localhost:8080`     | Kafbat fork of the old provectus UI    |
| MySQL            | `localhost:3306`            | `workshop`/`workshop`, db `workshop` — Block 5/6 stores  |
| Kafka Connect    | `http://localhost:8083`     | Debezium (CDC outbox relay, Block 6)   |
| PHP console      | `bin/console list`          | Run via `docker compose run --rm php`  |

Topic, schema, and consumer-group state lives in the named volume `kafka-data`;
MySQL data in `mysql-data`. Remove them (`docker compose down -v` or
`make destroy`) to reset.

### Running commands

```sh
docker compose run --rm php bin/console list                                    # show all commands
docker compose run --rm php bin/console produce consumer-groups-events -c 5      # produce 5 events
docker compose run --rm php bin/console consume consumer-groups-events -g group-a
```

Two generic commands cover every demo — parametrize them rather than adding new
classes:

```sh
bin/console produce <topic> [-c N] [--key a,b,c | --key-cardinality N] [-p PARTITION] [--payload 'order-{n}']
bin/console consume <topic> [-g GROUP] [-m MAX] [-t TIMEOUT_MS] [--no-commit]
```

`produce` cycles messages through `--key` (stable key→partition hashing) or pins
them to `-p`; `consume` under a named `-g` group keeps its committed offsets
across runs, while omitting `-g` reads the whole topic from earliest under a
throwaway group (the old "inspect" behavior).

For Block 3, a second pair works with **enveloped AVRO** events serialized in
the Confluent wire format against Schema Registry (schemas in `schemas/`):

```sh
bin/console events:produce <order-created|payment-processed|inventory-reserved> \
    [--order-id ID] [--correlation-id ID] [--causation-id ID] [--status SUCCEEDED|FAILED]
bin/console events:consume <topic> [-g GROUP] [-m MAX] [-t TIMEOUT_MS]
```

`events:produce` builds the metadata+payload envelope, AVRO-encodes it,
auto-registers the subject (`<topic>-value`), and keys the message by
`aggregate_id`. `events:consume` decodes via the registry and prints the
envelope. `order-created` prints a ready-to-paste command to chain a caused
`payment-processed` (shared `correlation_id`, linked `causation_id`).

For Block 4, two commands inspect schema evolution against the registry:

```sh
bin/console schema:check <type> <schema-file>   # is a candidate .avsc compatible with the latest? (CI gate; non-zero exit on fail)
bin/console schema:versions <type>              # list the registered version lineage [1, 2, …]
```

`schema:check` is the pre-registration compatibility check (read-only). Demo
schemas live in `schemas/orders/evolution/` — `OrderCreated-v2-compatible.avsc`
(optional field + default → PASS) and `-v2-breaking.avsc` (required field, no
default → FAIL).

For Block 5, a delivery-guarantees demo shows at-least-once vs. idempotent
processing, backed by MySQL via `doctrine/dbal`:

```sh
bin/console delivery:consume [topic] [-g GROUP] [--idempotent] [--crash-after N] [-m MAX] [-t TIMEOUT_MS]
bin/console delivery:reset                       # truncate side_effects + processed_events
```

`delivery:consume` applies a side-effect row per order event; `--crash-after N`
applies N then exits **without** committing the Kafka offset (simulating a crash
after the DB commit but before the offset commit), so a recovery run redelivers.
Without `--idempotent` the recovery duplicates the side-effect; with it, the
`event_id` recorded in `processed_events` (same transaction as the side-effect)
makes the redelivery a no-op. The idempotency record and the side-effect commit
in one DBAL transaction; the Kafka offset is committed separately and last.

For Block 6, an outbox demo shows the dual-write problem and its fix, with two
relays — a PHP polling relay and Debezium CDC:

```sh
bin/console outbox:place [--order-id ID] [--naive] [--crash]   # place an order
bin/console outbox:relay [--once] [-b BATCH] [-p POLL_SECS]     # polling relay → AVRO on the orders topic
bin/console outbox:reset                                        # truncate orders + outbox
```

`outbox:place` writes the `orders` row and an `OrderCreated` event to the `outbox`
table in **one** DBAL transaction; `--crash` exits right after the commit (the
event is safe — a relay publishes it later). `--naive` instead commits the order
and then publishes to Kafka directly — with `--crash` that loses the event (the
dual-write trap). `outbox:relay` publishes unpublished rows as AVRO envelopes
(read them with `events:consume enet.ecommerce.orders`), flushing to the broker
before marking each row published, so it is at-least-once by design.

The same outbox table can be shipped by **Debezium CDC** instead of the polling
relay (needs the `connect` service, started by `make create`):

```sh
bin/debezium-register      # waits for Connect, creates topics, registers the connector
bin/debezium-status        # connector + task state (expect RUNNING)
bin/console outbox:place   # then consume the routed topic:
bin/console consume enet.ecommerce.outbox.Order
bin/debezium-delete        # remove the connector
```

The Outbox Event Router routes by `aggregate_type` to `enet.ecommerce.outbox.Order`,
keys by `aggregate_id`, and unwraps the `payload` column to JSON. Config lives in
`config/debezium-outbox-connector.json`. Debezium **3.x** is required for MySQL 8.4
and the `mysql` service runs row-based binlog.

For Block 7, a retry/DLT demo shows error classification, a bounded
in-process-retry → retry-topic chain → Dead Letter Topic, and DLT recovery
(the retry tiers and shared DLT are created by `bin/topic-map`):

```sh
bin/console retry:consume [topic] [--poison=id,…] [--flaky=id,…] [--naive] \
    [--in-process-retries N] [-g GROUP] [-m MAX] [-t TIMEOUT_MS] [--memory-limit MB]
bin/console dlt:inspect [topic] [-t TIMEOUT_MS]            # print DLT error metadata
bin/console dlt:replay  [topic] [--dry-run] [-t TIMEOUT_MS] # re-publish to the original topic
```

`retry:consume` classifies failures: `--poison` ids go straight to the DLT (0
retries), `--flaky` ids fail their in-process retries then route to
`enet.ecommerce.orders.retry.5s` and succeed when that tier re-delivers them
(point a second `retry:consume` at the retry topic to drain it). It acks only
after a message succeeds or is durably routed, so the partition never blocks;
`--naive` drops the routing and exits without acking to show a stuck partition.
Success applies the Block 5 side-effect idempotently, so `dlt:replay` is safe to
re-run — duplicates are skipped on `event_id`. `dlt:inspect` reads the shared
`enet.internal.dead-letters` topic and prints each message's origin, error, retry
count, and reason. Routing/metadata live in `src/Kernel/RetryRouter.php`.

Admin operations against the broker are short bash scripts in `bin/`:

```sh
bin/topic-create consumer-groups-events --partitions 1
bin/topic-map                                       # create the full eCommerce topic map
bin/group-reset offsets-group earliest offsets-events
bin/partition-offsets partitioning-events
```

## Conventions

- **PSR-4 autodiscovery for services and commands.** `config/services.php`
  declares `autowire` + `autoconfigure` defaults and loads
  `Workshop\Kernel\` and `Workshop\Console\` from their respective
  directories. Any class extending `Symfony\Component\Console\Command\Command`
  is auto-tagged `console.command` and registered with the Symfony
  Application. Adding a command means creating one class — no edits to
  `bin/console` or `services.php`.
- **PHP commands use `enqueue/rdkafka` via the `KafkaContextFactory`
  service.** Constructor-inject the factory and call `forProducer()` or
  `forConsumer(string $groupId)`. Direct use of raw `RdKafka\*` classes is
  reserved for the block-08 config deep-dive.
- **Generic, parametrized commands over per-demo classes.** Two commands —
  `ProduceCommand` (`produce`) and `ConsumeCommand` (`consume`) — back every
  block. Topic, key, partition, group, count, and timeout are arguments and
  options, not hardcoded per exercise, so the CLI mirrors real Kafka tools
  (`kcat`, `kafka-console-{producer,consumer}`). Add behavior with a flag, not
  a new class; reach for a new command only for a genuinely distinct operation.
- **The three workshop topics are cataloged in `Workshop\Kernel\Topics`.**
  Cases: `ConsumerGroups`, `Offsets`, `Partitioning` → `consumer-groups-events`,
  `offsets-events`, `partitioning-events`. The commands accept any topic as a
  plain string argument (real-life); the enum is the canonical name list the
  blocks and admin scripts use, not a constraint the CLI enforces.
- **Config lives in `.env`** (`KAFKA_BROKERS`, `SCHEMA_REGISTRY_URL`,
  `DATABASE_URL`), loaded by `symfony/dotenv`. Per-user overrides go to
  `.env.local` (gitignored).
- **Admin shell scripts in `bin/`** wrap `docker compose exec kafka` calls
  to the Kafka CLI — `enqueue/rdkafka` has no admin API, so these stay
  shell.
- One topic per event type; subject naming follows `TopicNameStrategy`
  unless noted in the block exercise.
- Polish-language comments are acceptable in workshop exercises (delivered
  in Polish); code identifiers and shipped demos stay in English.
