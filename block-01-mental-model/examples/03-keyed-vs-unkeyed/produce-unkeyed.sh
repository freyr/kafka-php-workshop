#!/usr/bin/env bash
set -euo pipefail

TOPIC="demo03-events"
BROKER="kafka:29092"
N="${1:-20}"

seq 1 "$N" | sed 's/^/unkeyed-event-/' \
  | docker exec -i kpw-kafka kafka-console-producer \
      --bootstrap-server "$BROKER" --topic "$TOPIC"

echo "produced $N unkeyed messages"
