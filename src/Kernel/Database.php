<?php

declare(strict_types=1);

namespace Workshop\Kernel;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Exception as DbalException;
use Doctrine\DBAL\Tools\DsnParser;

/**
 * Lazy Doctrine DBAL connection for the Block 5 idempotency demo. Owns the
 * connection, the one-time schema bootstrap, and a transaction helper so the
 * idempotency record and the business side-effect commit atomically in a single
 * transaction — the production pattern the block teaches.
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
     * Create the demo tables if they do not exist. Idempotent — safe to call on
     * every command run.
     */
    public function ensureSchema(): void
    {
        try {
            $conn = $this->connection();
        } catch (DbalException $e) {
            throw new \RuntimeException('MySQL is unreachable. Is the stack up? Try `make create`.', previous: $e);
        }

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
}
