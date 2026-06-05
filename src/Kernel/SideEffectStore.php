<?php

declare(strict_types=1);

namespace Workshop\Kernel;

use Doctrine\DBAL\Connection;

/**
 * The observable business side-effect for the Block 5 demo, backed by MySQL via
 * Doctrine DBAL — stand-in for "charge the card", "reserve the stock", "create
 * the order row". Each applied effect is one row in side_effects, so the demo can
 * show by row count whether a redelivered message produced a duplicate.
 *
 * {@see apply()} runs on a caller-supplied Connection so it commits in the same
 * transaction as the idempotency record ({@see IdempotencyStore}).
 */
final readonly class SideEffectStore
{
    public function apply(Connection $tx, string $orderId, string $eventId): void
    {
        $tx->executeStatement(
            'INSERT INTO side_effects (order_id, event_id, applied_at)
             VALUES (?, ?, NOW())',
            [$orderId, $eventId],
        );
    }

    public function truncate(Connection $conn): void
    {
        $conn->executeStatement('TRUNCATE TABLE side_effects');
    }

    public function count(Connection $conn): int
    {
        return (int) $conn->fetchOne('SELECT COUNT(*) FROM side_effects');
    }
}
