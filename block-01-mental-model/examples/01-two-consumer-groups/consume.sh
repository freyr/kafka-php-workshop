#!/usr/bin/env bash
set -euo pipefail

GROUP="${1:-}"
if [[ -z "$GROUP" ]]; then
  echo "usage: $0 <group-suffix>   e.g. group-a" >&2
  exit 2
fi

TOPIC="demo01-events"
BROKER="kafka:29092"
GROUP_ID="demo01-${GROUP}"

docker exec -i kpw-kafka kafka-console-consumer \
  --bootstrap-server "$BROKER" \
  --topic "$TOPIC" \
  --group "$GROUP_ID" \
  --from-beginning \
  --max-messages 5 \
  --timeout-ms 5000 \
  --property print.offset=true \
  --property print.partition=true
