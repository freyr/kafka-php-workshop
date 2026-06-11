<?php

declare(strict_types=1);

namespace Workshop\Tests\App\Console;

use Doctrine\DBAL\Connection;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Tester\CommandTester;
use Workshop\App\Console\OutboxPlaceCommand;
use Workshop\App\Outbox\OrderStateWriter;
use Workshop\App\Outbox\OutboxPlacer;
use Workshop\App\Outbox\OutboxRepository;
use Workshop\App\Producer\Message;
use Workshop\App\Producer\MessageCatalog;
use Workshop\App\Producer\MessageNameResolver;
use Workshop\Kafka\Serde\MessageSerializer;

/**
 * The placer is real and runs against one mocked Connection whose transactional()
 * executes the callback, so the command's happy and rollback paths exercise the
 * full placer flow with no database.
 */
final class OutboxPlaceCommandTest extends TestCase
{
    public function testNonStateChangingMessageNameIsRejected(): void
    {
        $tester = $this->tester(self::createStub(Connection::class));

        // order.audited is a real catalog message, but it changes no order state,
        // so the outbox simulation refuses it.
        $tester->execute([
            '--message-name' => 'order.audited',
        ]);

        self::assertSame(Command::INVALID, $tester->getStatusCode());
        self::assertStringContainsString('Unknown message name', $tester->getDisplay());
        self::assertStringContainsString('order.created', $tester->getDisplay());
    }

    public function testCountMustBePositive(): void
    {
        $tester = $this->tester(self::createStub(Connection::class));

        $tester->execute([
            '--count' => '0',
        ]);

        self::assertSame(Command::INVALID, $tester->getStatusCode());
        self::assertStringContainsString('--count must be >= 1', $tester->getDisplay());
    }

    public function testPoolMustBePositive(): void
    {
        $tester = $this->tester(self::createStub(Connection::class));

        $tester->execute([
            '--pool' => '0',
        ]);

        self::assertSame(Command::INVALID, $tester->getStatusCode());
        self::assertStringContainsString('--pool must be >= 1', $tester->getDisplay());
    }

    public function testPlacementStoresTheFramedBytes(): void
    {
        $statements = [];

        $connection = self::createStub(Connection::class);
        $connection->method('transactional')
            ->willReturnCallback(static fn (\Closure $func): mixed => $func());
        $connection->method('executeStatement')
            ->willReturnCallback(function (string $sql, array $params = []) use (&$statements): int {
                $statements[] = [$sql, $params];

                return 1;
            });

        $tester = $this->tester($connection);

        $tester->execute([
            '--message-name' => 'order.created',
            '--interval' => '0',
        ]);

        self::assertSame(Command::SUCCESS, $tester->getStatusCode(), $tester->getDisplay());
        // The outbox INSERT carries the serializer's wire bytes — the same
        // Confluent-framed AVRO a direct produce would put on the topic.
        self::assertSame("\x00FRAMED-AVRO", $statements[1][1]['payload']);
    }

    public function testEachPlacementRunsItsOwnTransaction(): void
    {
        $connection = $this->connection();
        $connection->expects(self::exactly(2))
            ->method('transactional')
            ->willReturnCallback(static fn (\Closure $func): mixed => $func());

        $tester = $this->tester($connection);

        $tester->execute([
            '--count' => '2',
            '--interval' => '0',
            '--message-name' => 'order.created',
        ]);

        self::assertSame(Command::SUCCESS, $tester->getStatusCode(), $tester->getDisplay());
        self::assertStringContainsString('placed order.created', $tester->getDisplay());
        self::assertStringContainsString('placed 2 event(s)', $tester->getDisplay());
        self::assertStringContainsString('Kafka was never contacted', $tester->getDisplay());
    }

    public function testFailNarratesTheRollbackInsteadOfFailing(): void
    {
        $connection = $this->connection();
        $connection->expects(self::once())
            ->method('transactional')
            ->willReturnCallback(static fn (\Closure $func): mixed => $func());

        $tester = $this->tester($connection);

        $tester->execute([
            '--fail' => true,
        ]);

        self::assertSame(Command::SUCCESS, $tester->getStatusCode(), $tester->getDisplay());
        self::assertStringContainsString('rolled back', $tester->getDisplay());
        self::assertStringContainsString('1 rolled back, 0 placed', $tester->getDisplay());
    }

    /**
     * @return Connection&\PHPUnit\Framework\MockObject\MockObject
     */
    private function connection(): Connection
    {
        $connection = $this->createMock(Connection::class);
        $connection->method('executeStatement')->willReturn(1);

        return $connection;
    }

    private function tester(Connection $connection): CommandTester
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

        $placer = new OutboxPlacer(
            $connection,
            new OutboxRepository($connection),
            new OrderStateWriter($connection),
            new MessageNameResolver(),
            $serializer,
        );

        return new CommandTester(new OutboxPlaceCommand($placer, new MessageCatalog()));
    }
}
