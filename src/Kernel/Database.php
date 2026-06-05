<?php

declare(strict_types=1);

namespace Workshop\Kernel;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Exception as DbalException;
use Doctrine\DBAL\Tools\DsnParser;

/**
 * Lazy Doctrine DBAL connection for the Block 5 and Block 6 demos. Owns the
 * connection, the one-time schema bootstrap, and a transaction helper so a group
 * of writes commit atomically in a single transaction — the production pattern
 * both blocks teach (Block 5: idempotency record + side-effect; Block 6: business
 * row + outbox row).
 */
final class Database
{
    private ?Connection $connection = null;

    public function __construct(
        private readonly string $url,
    ) {
    }

    public function connection(): Connection
    {
        if (null === $this->connection) {
            $params = (new DsnParser([
                'mysql' => 'pdo_mysql',
            ]))->parse($this->url);
            $this->connection = DriverManager::getConnection($params);
        }

        return $this->connection;
    }

    /**
     * Run $fn inside a transaction, committing on success and rolling back on any
     * throwable. The closure receives the same Connection, so every store call
     * inside it shares one transaction.
     *
     * @template T
     *
     * @param callable(Connection): T $fn
     *
     * @return T
     */
    public function transactional(callable $fn): mixed
    {
        return $this->connection()->transactional($fn);
    }

    /**
     * Create the Block 5 demo tables if they do not exist. Idempotent — safe to
     * call on every command run.
     */
    public function ensureSchema(): void
    {
        $conn = $this->reachableConnection();

        $conn->executeStatement(
            'CREATE TABLE IF NOT EXISTS processed_events (
                event_id     VARCHAR(64)  NOT NULL PRIMARY KEY,
                event_type   VARCHAR(128) NOT NULL,
                processed_at DATETIME     NOT NULL
            ) ENGINE=InnoDB',
        );

        $conn->executeStatement(
            'CREATE TABLE IF NOT EXISTS side_effects (
                id         BIGINT AUTO_INCREMENT PRIMARY KEY,
                order_id   VARCHAR(128) NOT NULL,
                event_id   VARCHAR(64)  NOT NULL,
                applied_at DATETIME     NOT NULL
            ) ENGINE=InnoDB',
        );
    }

    /**
     * Create the Block 6 outbox demo tables if they do not exist. The `orders`
     * table is the business write; `outbox` is the event written in the *same*
     * transaction. Column names are snake_case to match the explicit field
     * mapping in config/debezium-outbox-connector.json. Idempotent.
     */
    public function ensureOutboxSchema(): void
    {
        $conn = $this->reachableConnection();

        $conn->executeStatement(
            'CREATE TABLE IF NOT EXISTS orders (
                order_id    VARCHAR(128) NOT NULL PRIMARY KEY,
                customer_id VARCHAR(128) NOT NULL,
                total_cents BIGINT       NOT NULL,
                created_at  DATETIME(3)  NOT NULL
            ) ENGINE=InnoDB',
        );

        $conn->executeStatement(
            'CREATE TABLE IF NOT EXISTS outbox (
                id             VARCHAR(64)  NOT NULL PRIMARY KEY,
                aggregate_type VARCHAR(64)  NOT NULL,
                aggregate_id   VARCHAR(128) NOT NULL,
                event_type     VARCHAR(128) NOT NULL,
                payload        JSON         NOT NULL,
                created_at     DATETIME(3)  NOT NULL,
                published_at   DATETIME(3)  NULL,
                KEY idx_outbox_unpublished (published_at, created_at)
            ) ENGINE=InnoDB',
        );
    }

    private function reachableConnection(): Connection
    {
        try {
            return $this->connection();
        } catch (DbalException $e) {
            throw new \RuntimeException('MySQL is unreachable. Is the stack up? Try `make create`.', previous: $e);
        }
    }
}
