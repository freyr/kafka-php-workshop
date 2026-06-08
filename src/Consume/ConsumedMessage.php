<?php

declare(strict_types=1);

namespace Workshop\Consume;

/**
 * One consumed record after interpretation: the typed read-model DTO plus the
 * envelope identity the middleware needs. This is the message the ConsumerBus
 * dispatches — middleware reads the metadata (event_id for dedup), the handler
 * reads only the dto, so idempotency stays a concern outside the handler.
 */
final readonly class ConsumedMessage
{
    public function __construct(
        public string $eventId,
        public string $name,
        public object $dto,
        public int $partition,
        public int $offset,
    ) {
    }
}
