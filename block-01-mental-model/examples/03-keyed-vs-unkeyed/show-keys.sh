#!/usr/bin/env bash
set -euo pipefail

TOPIC="demo03-events"
BROKER="kafka:29092"

docker exec -i kpw-kafka kafka-console-consumer \
  --bootstrap-server "$BROKER" \
  --topic "$TOPIC" \
  --from-beginning \
  --timeout-ms 5000 \
  --property print.key=true \
  --property print.partition=true \
  --property print.offset=true \
  --property key.separator=" | "
