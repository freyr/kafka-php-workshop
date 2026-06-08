# Single source of truth for the workshop's Kafka topic inventory.
#
# Every topic any block needs is declared here, once. bin/kafka-setup creates
# this list; bin/kafka-teardown deletes it — both source this file, so the two
# can never drift. Broker auto-create is off (compose.yaml), so a topic absent
# from this list will not spring into existence.
#
# Each entry is "<topic>:<partitions>"; the trailing comment notes the owning
# block and the partition key. Replication factor is always 1 (single broker).
WORKSHOP_TOPICS=(
  # Block 1 — mental model: consumer groups, offsets, partitioning. Free-form
  # JSON via the produce/consume commands. Multiple partitions where the demo
  # needs visible parallelism (group fan-out, key→partition routing).
  "consumer-groups-events:3"           # key=null      — fan one stream across a group
  "offsets-events:1"                   # key=null      — single partition, simple offset math
  "partitioning-events:3"              # key=<varies>  — watch keys land on stable partitions

  # Block 2 — eCommerce topic map. enet.ecommerce.* naming; partition counts
  # sized per stream. Reused by Blocks 3-5 (AVRO events) and Block 8 (ops).
  "enet.ecommerce.orders:6"            # key=orderId   — order lifecycle, ordered per order
  "enet.ecommerce.payments:6"          # key=orderId   — correlate payments with their order
  "enet.ecommerce.inventory:12"        # key=productId — sale-spike headroom
  "enet.ecommerce.users:6"             # key=userId    — low volume
  "enet.ecommerce.notifications:6"     # key=null      — round-robin, fire-and-forget
  "enet.internal.dead-letters:3"       # key=preserved — shared DLT (Blocks 5-7), near-zero volume

  # Block 6 — outbox / CDC. Debezium routes outbox rows to the .outbox.<Aggregate>
  # topic; schema-history is the connector's internal DDL log. bin/debezium-register
  # also ensures these so it stays runnable on its own, but setting them up here
  # means a single bin/kafka-setup provisions the whole workshop.
  "enet.ecommerce.outbox.Order:6"      # key=aggregateId — CDC output for the Order aggregate
  "schema-history.outbox:1"            # key=null      — Debezium schema-history log

  # Block 7 — retry-topic chain for enet.ecommerce.orders. 1 partition each: only
  # failed messages land here, so throughput is tiny. Demo-short delays (5s, 30s);
  # production uses minutes (1m / 5m / 30m). DLT is the shared one from Block 2.
  "enet.ecommerce.orders.retry.5s:1"   # key=preserved — first retry tier
  "enet.ecommerce.orders.retry.30s:1"  # key=preserved — second retry tier
)
