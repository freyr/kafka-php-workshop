#!/usr/bin/env bash
set -euo pipefail

TOPIC="demo02-events"
BROKER="kafka:29092"
GROUP_ID="demo02-group"

docker exec -i kpw-kafka kafka-console-consumer \
  --bootstrap-server "$BROKER" \
  --topic "$TOPIC" \
  --group "$GROUP_ID" \
  --timeout-ms 5000 \
  --property print.offset=true \
  --property print.partition=true
