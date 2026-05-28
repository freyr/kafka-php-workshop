#!/usr/bin/env bash
set -euo pipefail

BROKER="kafka:29092"
docker exec kpw-kafka kafka-consumer-groups \
  --bootstrap-server "$BROKER" \
  --group demo02-group \
  --describe
