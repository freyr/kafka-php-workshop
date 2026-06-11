.DEFAULT_GOAL := help

##@ Help
help: ## show this help
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage: make \033[36m<target>\033[0m\n"} \
		/^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2 } \
		/^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) }' $(MAKEFILE_LIST)

##@ Stack lifecycle
create: ## start the stack (waits for healthchecks) and provision everything: vendor, topics, AVRO schemas, DB tables
	docker compose up -d --wait
	$(MAKE) setup
destroy: ## stop containers and drop all volumes (wipes topics, schemas, and the database)
	docker compose down -v
recreate: destroy create ## full rebuild from scratch: destroy + create (create provisions everything)

##@ Provisioning
setup: ## provision everything, idempotently: composer vendor, every workshop topic, AVRO schemas, consumer + outbox tables
	docker compose run --rm php composer install
	bin/kafka-setup
	docker compose run --rm php php bin/console kafka:schema:register --all
	docker compose run --rm php bin/console kafka:consume:setup
	docker compose run --rm php bin/console outbox:setup

teardown: ## delete every workshop topic created by setup (idempotent; removes the Debezium connector first)
	-@bin/debezium-delete
	bin/kafka-teardown

##@ Consumer (orders projection)
TOPIC      ?= enet.ecommerce.orders
GROUP      ?= demo
FROM       ?= committed
PROFILE    ?= modern
IDEMPOTENT ?=
consume-setup: ## provision the consumer store (orders + processed_events tables)
	docker compose run --rm php bin/console kafka:consume:setup
consume: ## consume into the projection; override TOPIC/GROUP/FROM/PROFILE, set IDEMPOTENT=1 (e.g. make consume PROFILE=default FROM=beginning IDEMPOTENT=1)
	docker compose run --rm php bin/console kafka:consume $(TOPIC) --group $(GROUP) --from $(FROM) --profile $(PROFILE) $(if $(IDEMPOTENT),--idempotent,)

##@ PHP container
bash: ## interactive bash shell in an ephemeral php container
	docker compose run --rm php bash

test: ## run phpunit inside the php container (composer test)
	docker compose run --rm php composer test

##@ Integration tests
integration: integration-reset integration-test ## full cycle: reset kafka + db state, then run the integration suite

integration-reset: ## wipe state: recreate every topic, drop + recreate the projection tables, re-register schemas
	docker compose up -d kafka schema-registry mysql
	@active=$$(docker compose exec -T kafka kafka-consumer-groups --bootstrap-server kafka:29092 --list --state Stable 2>/dev/null | tail -n +2); \
	if [ -n "$$active" ]; then \
		echo "✗ live consumers are attached to the broker — stop them first, they would race the suite and project into the shared database:"; \
		echo "$$active"; \
		exit 1; \
	fi
	-@bin/debezium-delete
	bin/kafka-teardown
	bin/kafka-setup
	docker compose run --rm php php bin/console kafka:consume:setup --fresh
	docker compose run --rm php php bin/console kafka:schema:register --all

integration-test: ## run the integration testsuite (assumes the stack is up and state is reset)
	docker compose run --rm -e KAFKA_INTEGRATION=1 php composer test:integration

##@ Code style
ecs: ## report easy-coding-standard violations
	docker compose run --rm php composer ecs

ecs-fix: ## auto-fix easy-coding-standard violations
	docker compose run --rm php composer ecs-fix

##@ Static analysis
phpstan: ## run phpstan static analysis (level max)
	docker compose run --rm php composer phpstan

##@ Kafka inspection
topics: ## list all topics on the broker
	docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 --list

groups: ## list all consumer groups
	docker compose exec kafka kafka-consumer-groups --bootstrap-server kafka:29092 --list

##@ Outbox (Block 6 transactional outbox)
outbox-setup: ## provision the outbox table (FRESH=1 to drop + recreate; FORMAT=avro for the AVRO payload variant)
	docker compose run --rm php bin/console outbox:setup $(if $(FRESH),--fresh,) $(if $(FORMAT),--format $(FORMAT),)
outbox-place: ## place business writes: order + outbox row in one tx (COUNT=n NAME=order.created FAIL=1 FORMAT=avro)
	docker compose run --rm php bin/console outbox:place $(if $(COUNT),--count $(COUNT),) $(if $(NAME),--message-name $(NAME),) $(if $(FAIL),--fail,) $(if $(FORMAT),--format $(FORMAT),)
outbox-relay: ## run the PHP polling relay until Ctrl+C (ONCE=1 to drain the backlog and exit)
	docker compose run --rm php bin/console outbox:relay $(if $(ONCE),--once,)
outbox-watch: ## tail the relayed topic with keys + headers (both relay flavors land here)
	docker compose exec kafka kafka-console-consumer --bootstrap-server kafka:29092 \
		--topic enet.ecommerce.outbox.Order --from-beginning \
		--property print.headers=true --property print.key=true

##@ Enqueue (production-style clients over enqueue/rdkafka)
enqueue-produce: ## simulate php-fpm requests, one broker-acked message each (COUNT=n NAME=order.created)
	docker compose run --rm php bin/console enqueue:produce $(if $(COUNT),--count $(COUNT),) $(if $(NAME),--message-name $(NAME),)
enqueue-relay: ## run the enqueue outbox relay until Ctrl+C (ONCE=1 to drain the backlog and exit)
	docker compose run --rm php bin/console enqueue:outbox:relay $(if $(ONCE),--once,)
enqueue-consume: ## consume through the message bus with dedup always on (TOPIC=... GROUP=... MAX=n)
	docker compose run --rm php bin/console enqueue:consume $(if $(TOPIC),$(TOPIC),) $(if $(GROUP),--group $(GROUP),) $(if $(MAX),--max $(MAX),)

##@ Error handling (Block 7 — error.demo topic family, DLQ + retry)
errors-setup: ## provision the Block 7 demo: ensure topics, re-provision the outbox for AVRO (the demo lane ships real wire bytes), ensure the runtime_flags table
	bin/kafka-setup
	docker compose run --rm php bin/console outbox:setup --fresh --format avro
	docker compose run --rm php bin/console kafka:consume:setup
errors-produce: ## place error.demo events through the outbox with failures scattered in (COUNT=20 POISON=2 unframed + HEADERLESS=2 convention-less), then relay them to the main topic
	docker compose run --rm php bin/console outbox:place --message-name error.demo --format avro --count $(or $(COUNT),20) --poison $(or $(POISON),2) --headerless $(or $(HEADERLESS),2)
	docker compose run --rm php bin/console outbox:relay --once
errors-consume-main: ## the main lane: 3 short retries, then off-load; poison/permanent → DLQ; breaker fails fast. Ctrl+C to stop
	docker compose run --rm php bin/console kafka:consume enet.ecommerce.outbox.ErrorDemo --errors main --profile modern --idempotent --group errors-main -v
errors-consume-slow: ## the slow lane: drain the .retry topic with patient unbounded retries; breaker pauses. Ctrl+C to stop
	docker compose run --rm php bin/console kafka:consume enet.ecommerce.outbox.ErrorDemo.retry --errors slow --profile modern --idempotent --group errors-slow -v
failure-on: ## flip the transient-failure switch ON — the running consumer starts throwing from its handler
	docker compose run --rm php bin/console kafka:failure-mode on
failure-off: ## flip the transient-failure switch OFF — retries start succeeding again
	docker compose run --rm php bin/console kafka:failure-mode off
dlq-inspect: ## triage the DLQ: print every dead-lettered message's diagnostic headers (read-only)
	docker compose run --rm php bin/console kafka:dlq:inspect
dlq-replay: ## repair + re-publish DLQ messages to their original topic (DRY=1 preview; FIX_FRAME=1 re-frame raw AVRO; FIX_NAME=error.demo restore the header; ID=<event-id> select one); replay is dedup-safe
	docker compose run --rm php bin/console kafka:dlq:replay $(if $(DRY),--dry-run,) $(if $(FIX_FRAME),--fix-frame,) $(if $(FIX_NAME),--fix-message-name $(FIX_NAME),) $(if $(ID),--id $(ID),)

##@ Debezium (Block 6 CDC outbox)
debezium-register: ## register the connector for the JSON-payload outbox (EventRouter expands the envelope)
	bin/debezium-register
debezium-register-avro: ## register the connector for the AVRO-payload outbox (ByteArrayConverter pass-through; replaces the JSON one)
	bin/debezium-register config/debezium-outbox-connector-avro.json
debezium-status: ## show the connector + task state
	bin/debezium-status
debezium-delete: ## delete the connector
	bin/debezium-delete