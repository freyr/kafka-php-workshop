#!/usr/bin/env bash
set -euo pipefail

TOPIC="demo02-events"
BROKER="kafka:29092"
K="docker exec kpw-kafka"

$K kafka-consumer-groups --bootstrap-server "$BROKER" --delete --group demo02-group 2>/dev/null || true
$K kafka-topics --bootstrap-server "$BROKER" --delete --topic "$TOPIC" --if-exists
$K kafka-topics --bootstrap-server "$BROKER" --create --topic "$TOPIC" \
  --partitions 1 --replication-factor 1

seq 1 10 | sed 's/^/payment-completed-/' \
  | docker exec -i kpw-kafka kafka-console-producer \
      --bootstrap-server "$BROKER" --topic "$TOPIC"

echo "topic $TOPIC ready with 10 messages"
