<?php

declare(strict_types=1);

namespace Workshop\Tests\Integration;

use Symfony\Component\Console\Command\Command;

/**
 * kafka:consume:setup against the live mysql: the ensure-only default is
 * idempotent, and --fresh resets the store to empty (the integration reset path).
 */
final class ProjectionSetupCommandIntegrationTest extends IntegrationTestCase
{
    public function testSetupIsIdempotent(): void
    {
        foreach ([1, 2] as $run) {
            $tester = $this->runCommand('kafka:consume:setup', []);

            self::assertSame(Command::SUCCESS, $tester->getStatusCode(), sprintf('run %d: %s', $run, $tester->getDisplay()));
            self::assertStringContainsString('consumer store ready', $tester->getDisplay());
        }

        // Both tables answer queries after the double run.
        self::assertSame(0, (int) $this->db()->fetchOne('SELECT COUNT(*) FROM orders'));
        self::assertSame(0, (int) $this->db()->fetchOne('SELECT COUNT(*) FROM processed_events'));
    }

    public function testFreshResetsTheStore(): void
    {
        $this->db()->executeStatement("INSERT INTO orders (order_id, status) VALUES ('ord-fresh-test', 'open')");

        $tester = $this->runCommand('kafka:consume:setup', [
            '--fresh' => true,
        ]);

        self::assertSame(Command::SUCCESS, $tester->getStatusCode(), $tester->getDisplay());
        self::assertStringContainsString('orders dropped', $tester->getDisplay());
        self::assertStringContainsString('processed_events dropped', $tester->getDisplay());
        self::assertSame(0, (int) $this->db()->fetchOne('SELECT COUNT(*) FROM orders'), '--fresh must leave an empty store');
    }
}
