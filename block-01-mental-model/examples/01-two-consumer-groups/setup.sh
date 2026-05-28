#!/usr/bin/env bash
set -euo pipefail

TOPIC="demo01-events"
BROKER="kafka:29092"
K="docker exec kpw-kafka"

$K kafka-topics --bootstrap-server "$BROKER" --delete --topic "$TOPIC" --if-exists
$K kafka-topics --bootstrap-server "$BROKER" --create --topic "$TOPIC" \
  --partitions 1 --replication-factor 1

seq 1 5 | sed 's/^/order-placed-/' \
  | docker exec -i kpw-kafka kafka-console-producer \
      --bootstrap-server "$BROKER" --topic "$TOPIC"

echo "topic $TOPIC ready with 5 messages"
