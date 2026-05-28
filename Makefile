SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c
MAKEFLAGS += --no-print-directory
.DEFAULT_GOAL := help

DC         := docker compose
KAFKA_EXEC := $(DC) exec kafka
PHP_EXEC   := $(DC) exec php
PHP_RUN    := $(DC) run --rm php
BROKER     := kafka:29092

##@ Help

.PHONY: help
help: ## show this help
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage: make \033[36m<target>\033[0m\n"} \
		/^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2 } \
		/^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) }' $(MAKEFILE_LIST)

##@ Stack lifecycle

.PHONY: up
up: ## start kafka, schema registry, kafka-ui, and the php container (detached)
	$(DC) up -d

.PHONY: down
down: ## stop containers; named volumes are kept
	$(DC) down

.PHONY: restart
restart: ## down + up
	$(MAKE) down
	$(MAKE) up

.PHONY: nuke
nuke: ## down + drop the kafka-data volume (FULL RESET, deletes topics/offsets/schemas)
	$(DC) down -v

.PHONY: ps
ps: ## list stack containers and their status
	$(DC) ps

.PHONY: logs
logs: ## tail all stack logs (Ctrl-C to detach)
	$(DC) logs -f --tail=100

.PHONY: logs-kafka
logs-kafka: ## tail just the broker logs
	$(DC) logs -f --tail=100 kafka

##@ First-time setup

.PHONY: bootstrap
bootstrap: ## up + composer install (run once after cloning)
	$(MAKE) up
	@echo "waiting for php container to be running..."
	@until [ "$$($(DC) ps --status running --services | grep -c '^php$$')" = "1" ]; do sleep 1; done
	$(MAKE) composer-install

##@ PHP container

.PHONY: php
php: ## interactive bash shell inside the running php container
	$(PHP_EXEC) bash

.PHONY: c
c: ## run a workshop console command — usage: make c CMD="consumer-groups:produce"
	@test -n "$(CMD)" || { echo "CMD is required, e.g. make c CMD=\"consumer-groups:produce\"" >&2; exit 2; }
	$(PHP_RUN) bin/console $(CMD)

.PHONY: console
console: ## list workshop console commands
	$(PHP_RUN) bin/console list

.PHONY: composer-install
composer-install: ## install composer dependencies inside the container
	$(PHP_RUN) composer install

.PHONY: composer-update
composer-update: ## update composer dependencies inside the container
	$(PHP_RUN) composer update

.PHONY: test
test: ## run phpunit inside the container (composer test)
	$(PHP_RUN) composer test

##@ Kafka inspection

.PHONY: topics
topics: ## list all topics on the broker
	$(KAFKA_EXEC) kafka-topics --bootstrap-server $(BROKER) --list

.PHONY: groups
groups: ## list all consumer groups
	$(KAFKA_EXEC) kafka-consumer-groups --bootstrap-server $(BROKER) --list

.PHONY: describe-topic
describe-topic: ## describe a topic — usage: make describe-topic TOPIC=consumer-groups-events
	@test -n "$(TOPIC)" || { echo "TOPIC is required, e.g. make describe-topic TOPIC=consumer-groups-events" >&2; exit 2; }
	$(KAFKA_EXEC) kafka-topics --bootstrap-server $(BROKER) --describe --topic $(TOPIC)

.PHONY: describe-group
describe-group: ## describe a consumer group — usage: make describe-group GROUP=offsets-group
	@test -n "$(GROUP)" || { echo "GROUP is required, e.g. make describe-group GROUP=offsets-group" >&2; exit 2; }
	$(KAFKA_EXEC) kafka-consumer-groups --bootstrap-server $(BROKER) --describe --group $(GROUP)

##@ URLs (quick reference)

.PHONY: urls
urls: ## print URLs of the supporting UIs and broker
	@echo "Kafka UI        : http://localhost:8080"
	@echo "Schema Registry : http://localhost:8081"
	@echo "Broker (host)   : localhost:9092"
