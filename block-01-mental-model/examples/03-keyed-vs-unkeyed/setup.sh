#!/usr/bin/env bash
set -euo pipefail

TOPIC="demo03-events"
BROKER="kafka:29092"
PARTITIONS="${PARTITIONS:-4}"
K="docker exec kpw-kafka"

$K kafka-topics --bootstrap-server "$BROKER" --delete --topic "$TOPIC" --if-exists
$K kafka-topics --bootstrap-server "$BROKER" --create --topic "$TOPIC" \
  --partitions "$PARTITIONS" --replication-factor 1

echo "topic $TOPIC ready with $PARTITIONS partitions"
