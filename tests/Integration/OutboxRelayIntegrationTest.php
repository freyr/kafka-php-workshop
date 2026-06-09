<?php

declare(strict_types=1);

namespace Workshop\Tests\Integration;

use Symfony\Component\Console\Command\Command;
use Workshop\Framework\Db\OutboxSchemaInstaller;

/**
 * The Block 6 transactional-outbox flow against the live stack: outbox:place
 * commits the order mutation and the outbox row atomically (or rolls both back),
 * and outbox:relay drains pending rows to enet.ecommerce.outbox.Order — observed
 * via watermark deltas, like every topic assertion in this suite — marking them
 * published only afterwards.
 */
final class OutboxRelayIntegrationTest extends IntegrationTestCase
{
    private const string TOPIC = 'enet.ecommerce.outbox.Order';

    protected function setUp(): void
    {
        parent::setUp();

        // Drop-and-recreate (not ensure-and-truncate): another test in the suite
        // may have provisioned the outbox in the AVRO payload format, and ensure
        // would silently keep that column type.
        $installer = new OutboxSchemaInstaller($this->db());
        $installer->drop();
        $installer->install();
    }

    public function testPlaceCommitsTheOrderAndItsOutboxRowTogether(): void
    {
        $tester = $this->runCommand('outbox:place', [
            '--count' => '3',
            '--interval' => '0',
            '--message-name' => 'order.created',
            '--pool' => '1',
        ]);

        self::assertSame(Command::SUCCESS, $tester->getStatusCode(), $tester->getDisplay());

        // Three outbox rows, none published — Kafka was never contacted.
        self::assertSame(3, $this->rowCount('outbox'));
        self::assertSame(3, $this->rowCount('outbox', 'published_at IS NULL'));
        // One order row: a pool of 1 concentrates all writes on a single aggregate.
        self::assertSame(1, $this->rowCount('orders'));

        // The stored payload is the wire envelope; its metadata.event_id is the
        // row's id — the dedup key both relay flavors forward.
        $row = $this->db()->fetchAssociative('SELECT id, payload FROM outbox ORDER BY position LIMIT 1');
        self::assertIsArray($row);
        self::assertIsString($row['payload']);
        $payload = json_decode($row['payload'], true);
        self::assertIsArray($payload);
        self::assertIsArray($payload['metadata']);
        self::assertSame($row['id'], $payload['metadata']['event_id']);
    }

    public function testFailRollsBackBothWritesLeavingNoTrace(): void
    {
        $tester = $this->runCommand('outbox:place', [
            '--fail' => true,
        ]);

        self::assertSame(Command::SUCCESS, $tester->getStatusCode(), $tester->getDisplay());
        self::assertStringContainsString('rolled back', $tester->getDisplay());

        self::assertSame(0, $this->rowCount('outbox'));
        self::assertSame(0, $this->rowCount('orders'));
    }

    public function testRelayPublishesThePendingBacklogThenMarksIt(): void
    {
        $placer = $this->runCommand('outbox:place', [
            '--count' => '5',
            '--interval' => '0',
            '--pool' => '2',
        ]);
        self::assertSame(Command::SUCCESS, $placer->getStatusCode(), $placer->getDisplay());

        $before = $this->probe()->totalEnd(self::TOPIC);

        $relay = $this->runCommand('outbox:relay', [
            '--once' => true,
            '--interval' => '0',
        ]);

        self::assertSame(Command::SUCCESS, $relay->getStatusCode(), $relay->getDisplay());
        self::assertStringContainsString('relayed 5 event(s)', $relay->getDisplay());
        self::assertStringContainsString('pending now: 0', $relay->getDisplay());

        // Every row published exactly once (watermark delta), and marked as such.
        self::assertSame($before + 5, $this->probe()->totalEnd(self::TOPIC));
        self::assertSame(0, $this->rowCount('outbox', 'published_at IS NULL'));
        self::assertSame(5, $this->rowCount('outbox', 'published_at IS NOT NULL'));

        // A second drain finds nothing — published rows are never re-relayed.
        $again = $this->runCommand('outbox:relay', [
            '--once' => true,
            '--interval' => '0',
        ]);
        self::assertSame(Command::SUCCESS, $again->getStatusCode(), $again->getDisplay());
        self::assertStringContainsString('relayed 0 event(s)', $again->getDisplay());
        self::assertSame($before + 5, $this->probe()->totalEnd(self::TOPIC));
    }

    private function rowCount(string $table, string $where = '1 = 1'): int
    {
        $count = $this->db()->fetchOne(sprintf('SELECT COUNT(*) FROM %s WHERE %s', $table, $where));

        return is_numeric($count) ? (int) $count : -1;
    }
}
