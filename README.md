# PHP + Kafka Workshop

Working repo for a 2-day Kafka-for-PHP-teams workshop. Holds code examples,
hands-on exercises, AVRO schemas, slide assets, and a Docker stack with Kafka,
Confluent Schema Registry, Kafka UI, and a PHP 8.3 + `php-rdkafka` container.

Source material (research, agenda, per-block notes) lives in the Consulting
vault under `PHP-Kafka-Research/`. This repo is the runnable counterpart.

## Layout

```
.
├── docker/                          # Compose stack: Kafka + SR + UI + PHP
│   ├── compose.yaml
│   └── php/                         # PHP 8.3 + librdkafka + php-rdkafka image
├── schemas/                         # Shared AVRO schemas (subjects)
├── slides/                          # Exported deck assets
├── block-01-mental-model/           # Day 1 — Kafka mental model for PHP teams
├── block-02-topic-architecture/     # Day 1 — Topic design and partitioning
├── block-03-event-structure/        # Day 1 — Event design, envelopes, AVRO
├── block-04-schema-registry/        # Day 1 — Schema Registry + compatibility
├── block-05-delivery-guarantees/    # Day 2 — Acks, offsets, idempotency
├── block-06-outbox/                 # Day 2 — Outbox + Debezium, choreography
├── block-07-retry-dlt/              # Day 2 — Retry topics, DLT, error handling
├── block-08-php-config/             # Day 2 — Producer/consumer config, ops
└── block-09-blueprint/              # Day 2 — Target architecture session
```

Each `block-XX-*/` folder contains `examples/` (runnable demos shown during the
block) and `exercises/` (student tasks with starter code), plus its own
`README.md` linking back to the vault research document.

## Running the stack

```sh
docker compose up -d
```

`compose.yaml` lives at the repo root; image build contexts (currently only
the PHP CLI) live under `docker/`.

Services exposed on the host:

| Service          | URL / port               | Notes                                  |
|------------------|--------------------------|----------------------------------------|
| Kafka broker     | `localhost:9092`         | KRaft mode, single broker              |
| Schema Registry  | `http://localhost:8081`  | Confluent SR, `AVRO` schemas           |
| Kafka UI         | `http://localhost:8080`  | Kafbat fork of the old provectus UI    |
| PHP CLI          | `docker compose run php` | PHP 8.3 with `librdkafka` + `rdkafka`  |

Topic, schema, and consumer-group state is persisted in the named volume
`kafka-data`; remove it (`docker compose down -v`) to reset the stack.

## Conventions

- One topic per event type; subject naming follows `TopicNameStrategy` unless
  noted in the block exercise.
- Producer/consumer configs default to the values agreed in
  `block-08-php-config/` — examples in earlier blocks may simplify them for
  clarity but should call out any deviation.
- Polish-language comments are acceptable in `exercises/` (workshop is
  delivered in Polish); code identifiers and `examples/` stay in English.
