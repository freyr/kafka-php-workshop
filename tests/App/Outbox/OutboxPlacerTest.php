<?php

declare(strict_types=1);

namespace Workshop\Tests\App\Outbox;

use Doctrine\DBAL\Connection;
use PHPUnit\Framework\TestCase;
use Workshop\App\Outbox\OrderStateWriter;
use Workshop\App\Outbox\OutboxPlacer;
use Workshop\App\Outbox\OutboxRepository;
use Workshop\App\Outbox\PayloadFormat;
use Workshop\App\Outbox\SimulatedCrash;
use Workshop\App\Producer\Message;
use Workshop\App\Producer\MessageNameResolver;
use Workshop\App\Producer\OrderCreated;
use Workshop\Kafka\Serde\MessageSerializer;

/**
 * The placer's collaborators are real (they share the one mocked Connection), so
 * these tests assert the pattern's core claim at the SQL boundary: both writes
 * happen inside the transactional() callback, and the simulated crash escapes it
 * (the actual rollback is the database's job — covered by the integration suite).
 */
final class OutboxPlacerTest extends TestCase
{
    public function testWritesTheOrderAndTheOutboxRowInOneTransaction(): void
    {
        $statements = [];

        $connection = $this->createMock(Connection::class);
        $connection->expects(self::once())
            ->method('transactional')
            ->willReturnCallback(static fn (\Closure $func): mixed => $func());
        $connection->method('executeStatement')
            ->willReturnCallback(function (string $sql, array $params = []) use (&$statements): int {
                $statements[] = [$sql, $params];

                return 1;
            });

        $message = OrderCreated::create('ord-1');

        $this->placer($connection)->place($message);

        self::assertCount(2, $statements);
        self::assertStringContainsString('INSERT INTO orders', $statements[0][0]);
        self::assertStringContainsString('INSERT INTO outbox', $statements[1][0]);

        $outbox = $statements[1][1];
        self::assertSame($message->eventId(), $outbox['id']);
        self::assertSame('Order', $outbox['aggregate_type']);
        self::assertSame('ord-1', $outbox['aggregate_id']);
        self::assertSame('order.created', $outbox['event_type']);

        // The stored payload is the wire envelope, JSON-encoded.
        self::assertIsString($outbox['payload']);
        $payload = json_decode($outbox['payload'], true);
        self::assertIsArray($payload);
        self::assertSame($message->envelope(), $payload);
    }

    public function testAvroFormatStoresTheSerializerBytes(): void
    {
        $statements = [];

        $connection = $this->createMock(Connection::class);
        $connection->expects(self::once())
            ->method('transactional')
            ->willReturnCallback(static fn (\Closure $func): mixed => $func());
        $connection->method('executeStatement')
            ->willReturnCallback(function (string $sql, array $params = []) use (&$statements): int {
                $statements[] = [$sql, $params];

                return 1;
            });

        $message = OrderCreated::create('ord-1');

        $this->placer($connection)->place($message, false, PayloadFormat::Avro);

        // The stored payload is exactly what the MessageSerializer produced —
        // Confluent-framed wire bytes, not a JSON re-encoding of the envelope.
        $outbox = $statements[1][1];
        self::assertSame("\x00FRAMED-AVRO", $outbox['payload']);
    }

    public function testCrashBeforeCommitEscapesTheTransaction(): void
    {
        $connection = $this->createMock(Connection::class);
        $connection->expects(self::once())
            ->method('transactional')
            ->willReturnCallback(static fn (\Closure $func): mixed => $func());
        $connection->method('executeStatement')->willReturn(1);

        $this->expectException(SimulatedCrash::class);

        $this->placer($connection)->place(OrderCreated::create('ord-1'), true);
    }

    private function placer(Connection $connection): OutboxPlacer
    {
        $serializer = new class implements MessageSerializer {
            public function encode(Message $payload): string
            {
                return "\x00FRAMED-AVRO";
            }

            public function decode(string $bytes, ?\AvroSchema $readerSchema = null): mixed
            {
                return null;
            }
        };

        return new OutboxPlacer(
            $connection,
            new OutboxRepository($connection),
            new OrderStateWriter($connection),
            new MessageNameResolver(),
            $serializer,
        );
    }
}
