.DEFAULT_GOAL := help

##@ Help
help: ## show this help
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage: make \033[36m<target>\033[0m\n"} \
		/^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2 } \
		/^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) }' $(MAKEFILE_LIST)

##@ Stack lifecycle
create: ## start kafka, schema registry, kafka-ui (detached); php is profile-gated
	docker compose up -d
destroy: ## stop containers and drop the kafka-data volume (wipes topics)
	docker compose down -v
recreate: destroy create setup ## destroy + create + provision topics

##@ Topic provisioning
setup: ## create every workshop topic across all blocks (idempotent)
	bin/kafka-setup
	docker compose run --rm php php bin/console kafka:schema:register --all

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
outbox-setup: ## provision the outbox table (FRESH=1 to drop + recreate it empty)
	docker compose run --rm php bin/console outbox:setup $(if $(FRESH),--fresh,)
outbox-place: ## place business writes: order + outbox row in one tx (COUNT=n NAME=order.created FAIL=1)
	docker compose run --rm php bin/console outbox:place $(if $(COUNT),--count $(COUNT),) $(if $(NAME),--message-name $(NAME),) $(if $(FAIL),--fail,)
outbox-relay: ## run the PHP polling relay until Ctrl+C (ONCE=1 to drain the backlog and exit)
	docker compose run --rm php bin/console outbox:relay $(if $(ONCE),--once,)
outbox-watch: ## tail the relayed topic with keys + headers (both relay flavors land here)
	docker compose exec kafka kafka-console-consumer --bootstrap-server kafka:29092 \
		--topic enet.ecommerce.outbox.Order --from-beginning \
		--property print.headers=true --property print.key=true

##@ Debezium (Block 6 CDC outbox)
debezium-register: ## register the Debezium MySQL outbox connector
	bin/debezium-register
debezium-status: ## show the connector + task state
	bin/debezium-status
debezium-delete: ## delete the connector
	bin/debezium-delete