#!/usr/bin/env bash
set -euo pipefail

TOPIC="demo03-events"
BROKER="kafka:29092"

docker exec kpw-kafka kafka-topics --bootstrap-server "$BROKER" --delete --topic "$TOPIC" --if-exists
echo "demo 3 cleaned up"
