<?php

declare(strict_types=1);

namespace Workshop\App\Outbox;

use Doctrine\DBAL\ArrayParameterType;
use Doctrine\DBAL\Connection;

/**
 * The outbox table gateway. add() is the append the business transaction makes
 * (always called inside OutboxPlacer's transaction); fetchUnpublished() and
 * markPublished() are the two halves of the relay's poll-publish-mark cycle.
 * Rows are never deleted here — published rows keep their published_at stamp so
 * the room can read the full timeline in MySQL after a demo.
 */
final readonly class OutboxRepository
{
    public function __construct(
        private Connection $connection,
    ) {
    }

    public function add(string $id, string $aggregateType, string $aggregateId, string $eventType, string $payload): void
    {
        $this->connection->executeStatement(
            <<<'SQL'
                INSERT INTO outbox (id, aggregate_type, aggregate_id, event_type, payload)
                VALUES (:id, :aggregate_type, :aggregate_id, :event_type, :payload)
                SQL,
            [
                'id' => $id,
                'aggregate_type' => $aggregateType,
                'aggregate_id' => $aggregateId,
                'event_type' => $eventType,
                'payload' => $payload,
            ],
        );
    }

    /**
     * The next slice of pending rows in insertion order. Single-relay by design:
     * the workshop runs one `outbox:relay` at a time, so no row locking — two
     * concurrent relays would publish the same rows twice (which is exactly the
     * at-least-once conversation, but not one to have by accident).
     *
     * @return list<OutboxRecord>
     */
    public function fetchUnpublished(int $limit): array
    {
        $rows = $this->connection->fetchAllAssociative(sprintf(
            'SELECT position, id, aggregate_type, aggregate_id, event_type, payload
             FROM outbox WHERE published_at IS NULL ORDER BY position LIMIT %d',
            max(1, $limit),
        ));

        return array_map(
            static fn (array $row): OutboxRecord => new OutboxRecord(
                is_numeric($row['position'] ?? null) ? (int) $row['position'] : 0,
                is_string($row['id'] ?? null) ? $row['id'] : '',
                is_string($row['aggregate_type'] ?? null) ? $row['aggregate_type'] : '',
                is_string($row['aggregate_id'] ?? null) ? $row['aggregate_id'] : '',
                is_string($row['event_type'] ?? null) ? $row['event_type'] : '',
                is_string($row['payload'] ?? null) ? $row['payload'] : '',
            ),
            $rows,
        );
    }

    /**
     * Stamp a delivered batch. Called only after the producer's flush confirmed
     * every record in it — the relay's side of the at-least-once contract (a crash
     * between flush and this UPDATE re-publishes the batch on restart; consumers
     * dedup on event id, Block 5).
     *
     * @param non-empty-list<int> $positions
     */
    public function markPublished(array $positions): int
    {
        $affected = $this->connection->executeStatement(
            'UPDATE outbox SET published_at = NOW(3) WHERE position IN (?)',
            [$positions],
            [ArrayParameterType::INTEGER],
        );

        return (int) $affected;
    }

    public function countUnpublished(): int
    {
        $count = $this->connection->fetchOne('SELECT COUNT(*) FROM outbox WHERE published_at IS NULL');

        return is_numeric($count) ? (int) $count : 0;
    }
}
