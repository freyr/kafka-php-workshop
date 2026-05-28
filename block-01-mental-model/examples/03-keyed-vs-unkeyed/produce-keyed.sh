#!/usr/bin/env bash
set -euo pipefail

TOPIC="demo03-events"
BROKER="kafka:29092"
N="${1:-20}"
KEYS=("${@:2}")
if [[ ${#KEYS[@]} -eq 0 ]]; then
  KEYS=(alice bob carol dave)
fi

awk -v n="$N" -v keys="${KEYS[*]}" '
  BEGIN {
    nk = split(keys, k, " ")
    for (i = 1; i <= n; i++) printf "%s|event-%d\n", k[((i-1) % nk) + 1], i
  }' \
  | docker exec -i kpw-kafka kafka-console-producer \
      --bootstrap-server "$BROKER" --topic "$TOPIC" \
      --property "parse.key=true" \
      --property "key.separator=|"

echo "produced $N keyed messages across keys: ${KEYS[*]}"
