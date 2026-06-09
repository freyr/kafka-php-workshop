<?php

declare(strict_types=1);

namespace Workshop\Tests\Integration;

use Symfony\Component\Console\Command\Command;
use Workshop\App\Outbox\PayloadFormat;
use Workshop\Framework\Db\OutboxSchemaInstaller;

/**
 * The AVRO flavor of the Block 6 outbox: outbox:place --format avro stores
 * Confluent-framed bytes (encoded against the registered subjects, like
 * kafka:produce:sample) in a binary payload column, and the PHP relay forwards
 * them untouched — so the topic carries records the normal AVRO consume path can
 * decode. Asserted via the stored magic byte and watermark deltas.
 */
final class OutboxAvroRelayIntegrationTest extends IntegrationTestCase
{
    private const string TOPIC = 'enet.ecommerce.outbox.Order';

    protected function setUp(): void
    {
        parent::setUp();

        // Drop-and-recreate in the AVRO format — ensure would keep whatever
        // payload column type a previous test left behind.
        $installer = new OutboxSchemaInstaller($this->db());
        $installer->drop();
        $installer->install(PayloadFormat::Avro);
    }

    public function testSetupRefusesASilentFormatSwitch(): void
    {
        // The table exists in AVRO format; asking for json without --fresh must
        // fail loudly instead of lying about the payload column.
        $tester = $this->runCommand('outbox:setup', [
            '--format' => 'json',
        ]);

        self::assertSame(Command::FAILURE, $tester->getStatusCode(), $tester->getDisplay());
        self::assertStringContainsString('--fresh', $tester->getDisplay());
    }

    public function testPlaceStoresConfluentFramedBytes(): void
    {
        $tester = $this->runCommand('outbox:place', [
            '--count' => '2',
            '--interval' => '0',
            '--format' => 'avro',
            '--message-name' => 'order.created',
            '--pool' => '1',
        ]);

        self::assertSame(Command::SUCCESS, $tester->getStatusCode(), $tester->getDisplay());

        // Confluent wire format: 0x00 magic byte + 4-byte schema id + AVRO body.
        // The stored payload IS the wire payload — no relay re-serializes it.
        $payload = $this->db()->fetchOne('SELECT payload FROM outbox ORDER BY position LIMIT 1');
        self::assertIsString($payload);
        self::assertGreaterThan(5, strlen($payload));
        self::assertSame("\x00", $payload[0]);
        self::assertFalse(json_validate($payload), 'an AVRO payload must not be JSON text');
    }

    public function testRelayForwardsTheBytesAndMarksThePublishedRows(): void
    {
        $placer = $this->runCommand('outbox:place', [
            '--count' => '3',
            '--interval' => '0',
            '--format' => 'avro',
        ]);
        self::assertSame(Command::SUCCESS, $placer->getStatusCode(), $placer->getDisplay());

        $before = $this->probe()->totalEnd(self::TOPIC);

        $relay = $this->runCommand('outbox:relay', [
            '--once' => true,
            '--interval' => '0',
        ]);

        self::assertSame(Command::SUCCESS, $relay->getStatusCode(), $relay->getDisplay());
        self::assertStringContainsString('relayed 3 event(s)', $relay->getDisplay());
        self::assertSame($before + 3, $this->probe()->totalEnd(self::TOPIC));

        $pending = $this->db()->fetchOne('SELECT COUNT(*) FROM outbox WHERE published_at IS NULL');
        self::assertSame(0, is_numeric($pending) ? (int) $pending : -1);
    }
}
