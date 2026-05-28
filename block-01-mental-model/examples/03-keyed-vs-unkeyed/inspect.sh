#!/usr/bin/env bash
set -euo pipefail

TOPIC="demo03-events"
BROKER="kafka:29092"

echo "end offsets per partition (topic:partition:offset):"
docker exec kpw-kafka kafka-run-class kafka.tools.GetOffsetShell \
  --bootstrap-server "$BROKER" \
  --topic "$TOPIC" \
  --time -1 \
  | sort -t: -k2 -n
