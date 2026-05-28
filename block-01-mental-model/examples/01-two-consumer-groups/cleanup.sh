#!/usr/bin/env bash
set -euo pipefail

TOPIC="demo01-events"
BROKER="kafka:29092"
K="docker exec kpw-kafka"

for g in demo01-group-a demo01-group-b; do
  $K kafka-consumer-groups --bootstrap-server "$BROKER" --delete --group "$g" 2>/dev/null || true
done
$K kafka-topics --bootstrap-server "$BROKER" --delete --topic "$TOPIC" --if-exists
echo "demo 1 cleaned up"
