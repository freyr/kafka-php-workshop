<?php

declare(strict_types=1);

namespace Workshop\Tests\Enqueue\Console;

use Doctrine\DBAL\Connection;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Tester\CommandTester;
use Workshop\App\Outbox\OutboxRepository;
use Workshop\Enqueue\Console\EnqueueOutboxRelayCommand;
use Workshop\Enqueue\EnqueueContextFactory;

/**
 * The validation branch returns Command::INVALID before any client or database is
 * touched; the empty-backlog drain builds a real (never-flushed) enqueue producer
 * and walks the whole loop against a stubbed Connection — no broker, no MySQL.
 */
final class EnqueueOutboxRelayCommandTest extends TestCase
{
    public function testBatchMustBePositive(): void
    {
        $tester = $this->tester(self::createStub(Connection::class));

        $tester->execute([
            '--batch' => '0',
        ]);

        self::assertSame(Command::INVALID, $tester->getStatusCode());
        self::assertStringContainsString('--batch must be >= 1', $tester->getDisplay());
    }

    public function testOnceExitsCleanlyOnAnEmptyBacklog(): void
    {
        $connection = self::createStub(Connection::class);
        $connection->method('fetchOne')->willReturn('0');
        $connection->method('fetchAllAssociative')->willReturn([]);

        $tester = $this->tester($connection);

        $tester->execute([
            '--once' => true,
        ]);

        self::assertSame(Command::SUCCESS, $tester->getStatusCode(), $tester->getDisplay());
        self::assertStringContainsString('enqueue relay started — 0 event(s) pending', $tester->getDisplay());
        self::assertStringContainsString('relayed 0 event(s)', $tester->getDisplay());
    }

    private function tester(Connection $connection): CommandTester
    {
        return new CommandTester(new EnqueueOutboxRelayCommand(
            new EnqueueContextFactory('broker.test:29092'),
            new OutboxRepository($connection),
        ));
    }
}
