<?php

declare(strict_types=1);

namespace Workshop\Kernel;

use Doctrine\DBAL\Connection;

/**
 * The transactional-outbox table from Block 6, backed by MySQL via Doctrine DBAL.
 * An event row is written here in the *same transaction* as the business data
 * ({@see add()} runs on a caller-supplied Connection) — that single atomic write
 * is what eliminates the dual-write problem.
 *
 * A relay then ships rows to Kafka: either the polling relay (outbox:relay, which
 * uses {@see fetchUnpublished()}/{@see markPublished()}) or Debezium CDC, which
 * reads the binlog and ignores the published_at bookkeeping entirely.
 */
final readonly class OutboxStore
{
    /**
     * Persist an event into the outbox. The id is the envelope event_id, so it is
     * both the dedup key downstream (Block 5) and the Debezium event id. Payload
     * is the JSON-encoded enveloped record the relay re-serializes to AVRO.
     */
    public function add(
        Connection $tx,
        string $id,
        string $aggregateType,
        string $aggregateId,
        string $eventType,
        string $payloadJson,
    ): void {
        $tx->executeStatement(
            'INSERT INTO outbox (id, aggregate_type, aggregate_id, event_type, payload, created_at)
             VALUES (?, ?, ?, ?, ?, NOW(3))',
            [$id, $aggregateType, $aggregateId, $eventType, $payloadJson],
        );
    }

    /**
     * Oldest-first batch of not-yet-published rows. ORDER BY created_at keeps
     * per-aggregate order under a single relay instance; production multi-instance
     * relays add FOR UPDATE SKIP LOCKED (see the block notes).
     *
     * @return list<array{id: string, aggregate_id: string, event_type: string, payload: string}>
     */
    public function fetchUnpublished(Connection $conn, int $limit): array
    {
        /** @var list<array{id: string, aggregate_id: string, event_type: string, payload: string}> $rows */
        $rows = $conn->fetchAllAssociative(
            'SELECT id, aggregate_id, event_type, payload
             FROM outbox
             WHERE published_at IS NULL
             ORDER BY created_at ASC
             LIMIT ' . $limit,
        );

        return $rows;
    }

    /**
     * Mark rows published. Called only after Kafka has confirmed delivery, so a
     * crash between the Kafka flush and this update re-publishes on the next run —
     * the relay is at-least-once, which is exactly why consumers still dedup.
     *
     * @param list<string> $ids
     */
    public function markPublished(Connection $tx, array $ids): void
    {
        if ([] === $ids) {
            return;
        }

        $placeholders = implode(',', array_fill(0, count($ids), '?'));
        $tx->executeStatement(
            "UPDATE outbox SET published_at = NOW(3) WHERE id IN ($placeholders)",
            $ids,
        );
    }

    public function unpublishedCount(Connection $conn): int
    {
        return (int) $conn->fetchOne('SELECT COUNT(*) FROM outbox WHERE published_at IS NULL');
    }

    public function count(Connection $conn): int
    {
        return (int) $conn->fetchOne('SELECT COUNT(*) FROM outbox');
    }

    public function truncate(Connection $conn): void
    {
        $conn->executeStatement('TRUNCATE TABLE outbox');
    }
}
