#!/usr/bin/env bash
set -euo pipefail

GROUP="${1:-}"
if [[ -z "$GROUP" ]]; then
  echo "usage: $0 <group-suffix>   e.g. group-a" >&2
  exit 2
fi

BROKER="kafka:29092"
GROUP_ID="demo01-${GROUP}"

docker exec kpw-kafka kafka-consumer-groups \
  --bootstrap-server "$BROKER" \
  --group "$GROUP_ID" \
  --describe
