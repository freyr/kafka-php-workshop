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

teardown: ## delete every workshop topic created by setup (idempotent)
	bin/kafka-teardown

##@ Consumer (orders projection)
TOPIC  ?= enet.ecommerce.orders
GROUP  ?= demo
FROM   ?= committed
COMMIT ?= per-message
consume-setup: ## provision the consumer store (orders + processed_events tables)
	docker compose run --rm php bin/console kafka:consume:setup
consume: ## consume into the projection; override TOPIC/GROUP/FROM/COMMIT (e.g. make consume COMMIT=idempotent FROM=beginning)
	docker compose run --rm php bin/console kafka:consume $(TOPIC) -g $(GROUP) --from $(FROM) --commit $(COMMIT)

##@ PHP container
bash: ## interactive bash shell in an ephemeral php container
	docker compose run --rm php bash

test: ## run phpunit inside the php container (composer test)
	docker compose run --rm php composer test

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

##@ Debezium (Block 6 CDC outbox)
debezium-register: ## register the Debezium MySQL outbox connector
	bin/debezium-register
debezium-status: ## show the connector + task state
	bin/debezium-status
debezium-delete: ## delete the connector
	bin/debezium-delete