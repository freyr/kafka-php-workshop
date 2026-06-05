<?php

declare(strict_types=1);

namespace Workshop\Kernel;

use Doctrine\DBAL\Connection;

/**
 * The "processed events" table from Block 5, backed by MySQL via Doctrine DBAL.
 * Records which event_ids have already been applied so an idempotent consumer can
 * skip a redelivered message instead of repeating its side-effect.
 *
 * Every method operates on a Connection passed in by the caller, so the
 * idempotency record is written in the *same transaction* as the business
 * side-effect ({@see SideEffectStore}). That atomicity — and the fact that the DB
 * commit is independent of the Kafka offset commit — is exactly what lets a
 * recovery run recognise a duplicate after a crash-before-commit.
 */
final readonly class IdempotencyStore
{
    /**
     * Record the event as processed. Returns true if this is the first time
     * (newly inserted), false if it was already recorded (a duplicate). Uses
     * INSERT IGNORE so the check-and-record is a single atomic statement with no
     * read-then-write race.
     */
    public function recordIfNew(Connection $tx, string $eventId, string $eventType): bool
    {
        $affected = $tx->executeStatement(
            'INSERT IGNORE INTO processed_events (event_id, event_type, processed_at)
             VALUES (?, ?, NOW())',
            [$eventId, $eventType],
        );

        return 1 === (int) $affected;
    }

    public function truncate(Connection $conn): void
    {
        $conn->executeStatement('TRUNCATE TABLE processed_events');
    }

    public function count(Connection $conn): int
    {
        return (int) $conn->fetchOne('SELECT COUNT(*) FROM processed_events');
    }
}
