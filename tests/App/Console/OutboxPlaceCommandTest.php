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
use Workshop\App\Producer\MessageCatalog;
use Workshop\App\Producer\MessageNameResolver;

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
        $placer = new OutboxPlacer(
            $connection,
            new OutboxRepository($connection),
            new OrderStateWriter($connection),
            new MessageNameResolver(),
        );

        return new CommandTester(new OutboxPlaceCommand($placer, new MessageCatalog()));
    }
}
