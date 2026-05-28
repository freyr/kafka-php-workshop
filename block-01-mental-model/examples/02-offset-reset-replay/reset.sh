#!/usr/bin/env bash
set -euo pipefail

POS="${1:-}"
if [[ -z "$POS" ]]; then
  echo "usage: $0 <earliest|latest|N>" >&2
  exit 2
fi

TOPIC="demo02-events"
BROKER="kafka:29092"
GROUP_ID="demo02-group"

case "$POS" in
  earliest) FLAG=(--to-earliest) ;;
  latest)   FLAG=(--to-latest) ;;
  *)
    if [[ "$POS" =~ ^[0-9]+$ ]]; then
      FLAG=(--to-offset "$POS")
    else
      echo "invalid position: $POS (use earliest, latest, or a number)" >&2
      exit 2
    fi
    ;;
esac

docker exec kpw-kafka kafka-consumer-groups \
  --bootstrap-server "$BROKER" \
  --group "$GROUP_ID" \
  --reset-offsets \
  "${FLAG[@]}" \
  --topic "$TOPIC" \
  --execute
