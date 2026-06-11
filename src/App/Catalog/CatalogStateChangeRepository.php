<?php

declare(strict_types=1);

namespace Workshop\App\Catalog;

use Doctrine\DBAL\Connection;

/**
 * The ProductCatalog context's state-change gateway — the produce half of the
 * Block 9 projection demo. Append-only by design: Debezium tails the binlog for
 * these INSERTs, so there is no relay, no published_at, no poll cursor (contrast
 * with OutboxRepository, which serves both relay flavors). Rows stay in place
 * after publication; the table doubles as the demo's audit log.
 */
final readonly class CatalogStateChangeRepository
{
    /**
     * The EventRouter's route.by.field value. One aggregate per context here and
     * the connector pins the destination topic statically — the column exists
     * because the EventRouter contract requires it, not to fan out.
     */
    private const string AGGREGATE_TYPE = 'ProductCatalog';

    public function __construct(
        private Connection $connection,
    ) {
    }

    public function add(string $id, string $sku, string $eventType, string $payloadBytes): void
    {
        $this->connection->executeStatement(
            <<<'SQL'
                INSERT INTO product_catalog_state_change (id, aggregate_type, aggregate_id, event_type, payload)
                VALUES (:id, :aggregate_type, :aggregate_id, :event_type, :payload)
                SQL,
            [
                'id' => $id,
                'aggregate_type' => self::AGGREGATE_TYPE,
                'aggregate_id' => $sku,
                'event_type' => $eventType,
                'payload' => $payloadBytes,
            ],
        );
    }
}
