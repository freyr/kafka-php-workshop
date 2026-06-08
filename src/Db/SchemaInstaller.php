<?php

declare(strict_types=1);

namespace Workshop\Db;

use Doctrine\DBAL\Connection;

/**
 * Provisions the consumer's two tables idempotently (CREATE TABLE IF NOT EXISTS), so
 * `kafka:consume:setup` can be re-run safely. The consumer itself never issues DDL —
 * provisioning is a deliberate, separate step the room runs once, the same way
 * topics and schemas are provisioned out of band.
 *
 *  - orders: the read-model projection the handler upserts from each order event.
 *  - processed_events: the dedup ledger keyed by the event's UUIDv7 event_id; the
 *    primary key is the hard idempotency guard behind the --commit=idempotent flow.
 */
final readonly class SchemaInstaller
{
    public function __construct(
        private Connection $connection,
    ) {
    }

    /**
     * @return list<string> the tables ensured, for the command to report
     */
    public function install(): array
    {
        $this->connection->executeStatement(<<<'SQL'
            CREATE TABLE IF NOT EXISTS orders (
              order_id         VARCHAR(64)  NOT NULL PRIMARY KEY,
              customer_name    VARCHAR(255) NULL,
              total_cents      BIGINT       NULL,
              status           VARCHAR(32)  NULL,
              previous_status  VARCHAR(32)  NULL,
              cancelled_reason VARCHAR(255) NULL,
              updated_at       TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            ) ENGINE=InnoDB
            SQL);

        $this->connection->executeStatement(<<<'SQL'
            CREATE TABLE IF NOT EXISTS processed_events (
              event_id     VARCHAR(36) NOT NULL PRIMARY KEY,
              message_name VARCHAR(64) NULL,
              partition_id INT         NULL,
              offset_value BIGINT      NULL,
              processed_at TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB
            SQL);

        return ['orders', 'processed_events'];
    }
}
