<?php

declare(strict_types=1);

namespace Workshop\Tests\App\Outbox;

use Doctrine\DBAL\Connection;
use PHPUnit\Framework\TestCase;
use Workshop\App\Outbox\OrderStateWriter;
use Workshop\App\Outbox\OutboxPlacer;
use Workshop\App\Outbox\OutboxRepository;
use Workshop\App\Outbox\SimulatedCrash;
use Workshop\App\Outbox\Tamper;
use Workshop\App\Producer\ErrorDemo;
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

        // The stored payload is exactly what the MessageSerializer produced —
        // Confluent-framed wire bytes, ready for a relay to forward untouched.
        self::assertSame("\x00FRAMED-AVRO", $outbox['payload']);
    }

    public function testErrorDemoPlacesOnlyTheOutboxRowUnderItsOwnAggregate(): void
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

        $message = ErrorDemo::create('err-1', 4);

        $this->placer($connection)->place($message);

        // No business state to change — the error demo borrows the outbox only
        // as its at-least-once producing vehicle.
        self::assertCount(1, $statements);
        self::assertStringContainsString('INSERT INTO outbox', $statements[0][0]);
        self::assertSame('ErrorDemo', $statements[0][1]['aggregate_type']);
        self::assertSame('err-1', $statements[0][1]['aggregate_id']);
        self::assertSame('error.demo', $statements[0][1]['event_type']);
    }

    public function testUnframedTamperStripsTheConfluentFrameNotTheBody(): void
    {
        $statements = [];

        $connection = $this->createStub(Connection::class);
        $connection->method('transactional')
            ->willReturnCallback(static fn (\Closure $func): mixed => $func());
        $connection->method('executeStatement')
            ->willReturnCallback(function (string $sql, array $params = []) use (&$statements): int {
                $statements[] = [$sql, $params];

                return 1;
            });

        $this->placer($connection)->place(ErrorDemo::create('err-1', 1), false, Tamper::Unframed);

        $payload = $statements[0][1]['payload'];
        self::assertIsString($payload);
        self::assertSame('ED-AVRO', $payload, 'the whole 5-byte Confluent frame is gone — raw AVRO, as the wrong serializer would ship it; the consumer\'s frame check fails deterministically');
        self::assertSame('error.demo', $statements[0][1]['event_type'], 'the headers convention is intact — only the framing is broken');
    }

    public function testHeaderlessTamperKeepsThePayloadButDropsTheEventType(): void
    {
        $statements = [];

        $connection = $this->createStub(Connection::class);
        $connection->method('transactional')
            ->willReturnCallback(static fn (\Closure $func): mixed => $func());
        $connection->method('executeStatement')
            ->willReturnCallback(function (string $sql, array $params = []) use (&$statements): int {
                $statements[] = [$sql, $params];

                return 1;
            });

        $this->placer($connection)->place(ErrorDemo::create('err-1', 1), false, Tamper::Headerless);

        self::assertSame("\x00FRAMED-AVRO", $statements[0][1]['payload'], 'the payload stays perfectly valid framed AVRO');
        self::assertSame('', $statements[0][1]['event_type'], 'no event type → the relay ships the record without the message-name header — the convention contract is broken');
        self::assertSame('ErrorDemo', $statements[0][1]['aggregate_type'], 'routing to the dedicated topic family still works — that comes from the aggregate, not the header');
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
