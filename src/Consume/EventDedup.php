<?php

declare(strict_types=1);

namespace Workshop\Consume;

use Doctrine\DBAL\Connection;

/**
 * The dedup ledger behind the idempotent commit strategy. seen()/record() are run
 * inside the bus's transaction by IdempotencyMiddleware, so the check-then-insert is
 * atomic with the projection write: a redelivered event finds its id already present
 * and the handler is skipped. The processed_events primary key on event_id is the
 * ultimate guard — even a race that slips past seen() fails the insert.
 */
final readonly class EventDedup
{
    public function __construct(
        private Connection $connection,
    ) {
    }

    public function seen(string $eventId): bool
    {
        return false !== $this->connection->fetchOne(
            'SELECT 1 FROM processed_events WHERE event_id = ?',
            [$eventId],
        );
    }

    public function record(ConsumedMessage $message): void
    {
        $this->connection->insert('processed_events', [
            'event_id' => $message->eventId,
            'message_name' => $message->name,
            'partition_id' => $message->partition,
            'offset_value' => $message->offset,
        ]);
    }
}
