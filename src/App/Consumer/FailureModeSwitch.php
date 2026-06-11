<?php

declare(strict_types=1);

namespace Workshop\App\Consumer;

use Doctrine\DBAL\Connection;

/**
 * The Block 7 transient-failure toggle: one row in the runtime_flags table,
 * flipped by `kafka:failure-mode` and consulted by ErrorDemoHandler on every
 * message. MySQL is the channel on purpose — it is the only shared store every
 * `docker compose run` container already reaches, so the toggle crosses process
 * boundaries with zero new infrastructure. The per-message SELECT is deliberate
 * (no caching): the demo control must take effect deterministically on the very
 * next message.
 */
final readonly class FailureModeSwitch
{
    private const string FLAG = 'transient-failure';

    public function __construct(
        private Connection $connection,
    ) {
    }

    public function enabled(): bool
    {
        $value = $this->connection->fetchOne(
            'SELECT enabled FROM runtime_flags WHERE name = ?',
            [self::FLAG],
        );

        return is_numeric($value) && 1 === (int) $value;
    }

    public function enable(): void
    {
        $this->set(true);
    }

    public function disable(): void
    {
        $this->set(false);
    }

    private function set(bool $enabled): void
    {
        $this->connection->executeStatement(
            <<<'SQL'
                INSERT INTO runtime_flags (name, enabled)
                VALUES (:name, :enabled) AS incoming
                ON DUPLICATE KEY UPDATE enabled = incoming.enabled
                SQL,
            [
                'name' => self::FLAG,
                'enabled' => $enabled ? 1 : 0,
            ],
        );
    }
}
