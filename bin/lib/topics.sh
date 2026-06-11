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
  "enet.ecommerce.audit:1"             # key=orderId   — single-partition audit log; consumer-group offsets demo (demo:offsets:*)

  # Block 4 — schema-evolution playground. A throwaway flat event (demo.order.evolved)
  # you evolve in place; single partition so produced/consumed records stay in offset
  # order and are trivial to read back.
  "enet.demo.orders:1"                 # key=orderId   — schema-evolution exercise; isolated from the real orders topic

  # Block 6 — outbox / CDC. Debezium routes outbox rows to the .outbox.<Aggregate>
  # topic; schema-history is the connector's internal DDL log. bin/debezium-register
  # also ensures these so it stays runnable on its own, but setting them up here
  # means a single bin/kafka-setup provisions the whole workshop.
  "enet.ecommerce.outbox.Order:6"      # key=aggregateId — CDC output for the Order aggregate
  "schema-history.outbox:1"            # key=null      — Debezium schema-history log

  # Block 7 — error handling. The error.demo event's DEDICATED topic family: the
  # outbox relay lands ErrorDemo aggregates on the main topic; kafka:consume
  # --errors routes failures to .retry (transient parking, drained by the slow
  # lane) and .dlq (poison/permanent — manual triage via kafka:dlq:inspect).
  # 3 main partitions make never-block-a-partition visible per partition; the
  # retry/dlq tiers are low-throughput, 1 partition each.
  "enet.ecommerce.outbox.ErrorDemo:3"       # key=demo id — Block 7 main lane
  "enet.ecommerce.outbox.ErrorDemo.retry:1" # key=demo id — transient parking (slow lane)
  "enet.ecommerce.outbox.ErrorDemo.dlq:1"   # key=demo id — dead letters, manual triage only
)
