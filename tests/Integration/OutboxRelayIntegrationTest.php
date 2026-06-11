<?php

declare(strict_types=1);

namespace Workshop\Tests\Integration;

use Symfony\Component\Console\Command\Command;
use Workshop\Framework\Db\OutboxSchemaInstaller;

/**
 * The Block 6 transactional-outbox flow against the live stack: outbox:place
 * commits the order mutation and the outbox row atomically (or rolls both back),
 * storing Confluent-framed AVRO bytes (encoded against the registered subjects,
 * like kafka:produce:sample), and outbox:relay forwards them untouched to
 * enet.ecommerce.outbox.Order — observed via watermark deltas, like every topic
 * assertion in this suite — marking rows published only afterwards.
 */
final class OutboxRelayIntegrationTest extends IntegrationTestCase
{
    private const string TOPIC = 'enet.ecommerce.outbox.Order';

    protected function setUp(): void
    {
        parent::setUp();

        // Drop-and-recreate (not ensure-and-truncate): another test in the suite
        // may have left a stale table behind, and ensure would silently keep it.
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

        // The stored payload IS the wire payload — Confluent framing (0x00 magic
        // byte + 4-byte schema id + AVRO body), no relay re-serializes it.
        $payload = $this->db()->fetchOne('SELECT payload FROM outbox ORDER BY position LIMIT 1');
        self::assertIsString($payload);
        self::assertGreaterThan(5, strlen($payload));
        self::assertSame("\x00", $payload[0]);
        self::assertFalse(json_validate($payload), 'an AVRO payload must not be JSON text');
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

    public function testSetupRefusesAStaleNonBlobPayloadColumn(): void
    {
        // A table left behind by a pre-AVRO provisioning (JSON payload column):
        // setup must fail loudly and demand --fresh instead of lying about the
        // column under existing rows.
        $this->db()->executeStatement('DROP TABLE outbox');
        $this->db()->executeStatement(<<<'SQL'
            CREATE TABLE outbox (
              position BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
              payload  JSON NOT NULL,
              PRIMARY KEY (position)
            ) ENGINE=InnoDB
            SQL);

        $tester = $this->runCommand('outbox:setup');

        self::assertSame(Command::FAILURE, $tester->getStatusCode(), $tester->getDisplay());
        self::assertStringContainsString('--fresh', $tester->getDisplay());
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
