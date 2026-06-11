<?php

declare(strict_types=1);

namespace Workshop\Tests\Integration;

use Symfony\Component\Console\Command\Command;

/**
 * catalog:setup against the live mysql: the ensure-only default is idempotent,
 * and --fresh resets both demo tables to empty.
 *
 * Note: IntegrationTestCase::setUp() provisions and truncates only the orders /
 * processed_events tables — the catalog tables are not touched between tests.
 * testSetupIsIdempotent therefore opens with a --fresh run to own its state
 * (otherwise rows left by earlier manual runs would break the COUNT(*) = 0 assertions).
 */
final class CatalogSetupCommandIntegrationTest extends IntegrationTestCase
{
    public function testSetupIsIdempotent(): void
    {
        // Reset first so the test owns its preconditions — catalog tables are not
        // truncated by IntegrationTestCase::setUp() and may carry rows from prior runs.
        $this->runCommand('catalog:setup', [
            '--fresh' => true,
        ]);

        foreach ([1, 2] as $run) {
            $tester = $this->runCommand('catalog:setup', []);

            self::assertSame(Command::SUCCESS, $tester->getStatusCode(), sprintf('run %d: %s', $run, $tester->getDisplay()));
            self::assertStringContainsString('catalog demo store ready', $tester->getDisplay());
        }

        // Both tables answer queries after the double run.
        self::assertSame(0, (int) $this->db()->fetchOne('SELECT COUNT(*) FROM product_catalog_state_change'));
        self::assertSame(0, (int) $this->db()->fetchOne('SELECT COUNT(*) FROM products_projection'));
    }

    public function testFreshResetsBothTables(): void
    {
        $setupTester = $this->runCommand('catalog:setup', []);
        self::assertSame(Command::SUCCESS, $setupTester->getStatusCode(), 'precondition: ' . $setupTester->getDisplay());
        $this->db()->executeStatement("INSERT INTO products_projection (sku, name, price, margin) VALUES ('SKU-IT-FRESH', 'it', 1, 1)");

        $tester = $this->runCommand('catalog:setup', [
            '--fresh' => true,
        ]);

        self::assertSame(Command::SUCCESS, $tester->getStatusCode(), $tester->getDisplay());
        self::assertStringContainsString('product_catalog_state_change dropped', $tester->getDisplay());
        self::assertStringContainsString('products_projection dropped', $tester->getDisplay());
        self::assertSame(0, (int) $this->db()->fetchOne('SELECT COUNT(*) FROM products_projection'), '--fresh must leave an empty store');
        self::assertSame(0, (int) $this->db()->fetchOne('SELECT COUNT(*) FROM product_catalog_state_change'), '--fresh must leave an empty source table');
    }
}
