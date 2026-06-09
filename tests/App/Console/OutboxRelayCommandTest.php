<?php

declare(strict_types=1);

namespace Workshop\Tests\App\Console;

use Doctrine\DBAL\Connection;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Tester\CommandTester;
use Workshop\App\Console\OutboxRelayCommand;
use Workshop\App\Outbox\OutboxRepository;
use Workshop\App\Producer\MessageNameResolver;
use Workshop\App\Producer\MessageRouting;
use Workshop\Kafka\Client\ProducerFactory;
use Workshop\Kafka\Config\BrokerProbe;
use Workshop\Kafka\Config\ConfBuilder;
use Workshop\Kafka\Config\KafkaProfiles;

/**
 * The validation branches return Command::INVALID before any client or database
 * is touched; the empty-backlog drain builds a real (never-flushed) producer and
 * walks the whole loop against a mocked Connection — no broker, no MySQL.
 */
final class OutboxRelayCommandTest extends TestCase
{
    public function testUnknownProfileIsRejected(): void
    {
        $tester = $this->tester(self::createStub(Connection::class));

        $tester->execute([
            '--profile' => 'turbo',
        ]);

        self::assertSame(Command::INVALID, $tester->getStatusCode());
        self::assertStringContainsString('Unknown profile: turbo', $tester->getDisplay());
        self::assertStringContainsString('idempotent | simple', $tester->getDisplay());
    }

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
        self::assertStringContainsString('relay started — 0 event(s) pending', $tester->getDisplay());
        self::assertStringContainsString('relayed 0 event(s)', $tester->getDisplay());
    }

    private function tester(Connection $connection): CommandTester
    {
        $noop = new class implements BrokerProbe {
            public function assertReachable(string $brokers): void
            {
            }
        };

        $factory = new ProducerFactory(
            new ConfBuilder('broker.test:29092', $noop),
            new KafkaProfiles(),
            new MessageRouting([]),
            new MessageNameResolver(),
        );

        return new CommandTester(new OutboxRelayCommand(new OutboxRepository($connection), $factory));
    }
}
