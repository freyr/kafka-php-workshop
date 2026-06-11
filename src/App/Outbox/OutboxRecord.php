<?php

declare(strict_types=1);

namespace Workshop\App\Outbox;

/**
 * One pending outbox row as the relay sees it: everything needed to build the
 * Kafka record (destination from aggregate_type, key from aggregate_id, headers
 * from event_type/id, value from payload) plus the position the relay marks
 * published after the broker acks. The payload stays the raw bytes the business
 * transaction wrote — the relay forwards bytes, it never re-serializes.
 */
final readonly class OutboxRecord
{
    public function __construct(
        public int $position,
        public string $id,
        public string $aggregateType,
        public string $aggregateId,
        public string $eventType,
        public string $payload,
    ) {
    }
}
