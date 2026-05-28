#!/usr/bin/env bash
set -euo pipefail

TOPIC="demo02-events"
BROKER="kafka:29092"
K="docker exec kpw-kafka"

$K kafka-consumer-groups --bootstrap-server "$BROKER" --delete --group demo02-group 2>/dev/null || true
$K kafka-topics --bootstrap-server "$BROKER" --delete --topic "$TOPIC" --if-exists
echo "demo 2 cleaned up"
