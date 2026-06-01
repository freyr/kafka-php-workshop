# PHP + Kafka Workshop

Working repo for a 2-day Kafka-for-PHP-teams workshop. Holds runnable demo
commands, AVRO schemas, slide assets, and a Docker stack with Kafka,
Confluent Schema Registry, Kafka UI, and a PHP 8.4 container with
`php-rdkafka` (extension) + [`enqueue/rdkafka`](https://github.com/php-enqueue/rdkafka)
(queue-interop wrapper used by all PHP commands), wired through
`symfony/console` + `symfony/dependency-injection` (with PSR-4
autodiscovery via `symfony/config`) and configured through `symfony/dotenv`.

Per-block facilitator notes live locally in `blocks/` (gitignored — pulled
from the Consulting vault `PHP-Kafka-Research/`). This repo is the runnable
counterpart to that material.

## Layout

```
.
├── compose.yaml             # Compose stack: Kafka + SR + UI + PHP
├── Dockerfile               # PHP 8.4 + librdkafka + php-rdkafka image
├── .env                     # Workshop defaults (loaded by bin/console)
├── bin/
│   ├── console              # Symfony Console entry — all PHP commands
│   ├── topic-create / topic-delete / topic-describe
│   ├── topic-map / topic-map-delete   # create/tear down the eCommerce topic map (Block 2)
│   ├── group-describe / group-reset / group-delete
│   └── partition-offsets
├── config/
│   └── services.php         # DI definitions; PSR-4 autodiscovery of services
├── src/
│   ├── Kernel/              # KafkaContextFactory, RawStringSerializer, Topics enum
│   └── Console/             # One class per command, self-describing names
├── schemas/                 # Shared AVRO schemas (subjects)
├── slides/                  # Exported deck assets
├── tests/                   # PHPUnit suite
└── blocks/                  # Per-block facilitator notes (gitignored)
```

## Running the stack

```sh
make create                                   # bring up kafka + schema registry + kafka-ui (detached)
docker compose run --rm php composer install  # first-time: install PHP deps in the php container
```

Services exposed on the host:

| Service          | URL / port                  | Notes                                  |
|------------------|-----------------------------|----------------------------------------|
| Kafka broker     | `localhost:9092`            | KRaft mode, single broker              |
| Schema Registry  | `http://localhost:8081`     | Confluent SR, `AVRO` schemas           |
| Kafka UI         | `http://localhost:8080`     | Kafbat fork of the old provectus UI    |
| PHP console      | `bin/console list`          | Run via `docker compose run --rm php`  |

Topic, schema, and consumer-group state lives in the named volume
`kafka-data`; remove it (`docker compose down -v` or `make destroy`) to reset.

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
- **Config lives in `.env`** (`KAFKA_BROKERS`, `SCHEMA_REGISTRY_URL`),
  loaded by `symfony/dotenv`. Per-user overrides go to `.env.local`
  (gitignored).
- **Admin shell scripts in `bin/`** wrap `docker compose exec kafka` calls
  to the Kafka CLI — `enqueue/rdkafka` has no admin API, so these stay
  shell.
- One topic per event type; subject naming follows `TopicNameStrategy`
  unless noted in the block exercise.
- Polish-language comments are acceptable in workshop exercises (delivered
  in Polish); code identifiers and shipped demos stay in English.
