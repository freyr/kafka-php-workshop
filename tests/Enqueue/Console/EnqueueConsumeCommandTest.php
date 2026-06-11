<?php

declare(strict_types=1);

namespace Workshop\Tests\Enqueue\Console;

use Doctrine\DBAL\Connection;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Tester\CommandTester;
use Symfony\Component\DependencyInjection\ServiceLocator;
use Workshop\App\Consumer\ConsoleWriter;
use Workshop\App\Consumer\DtoRouting;
use Workshop\App\Consumer\EventDedup;
use Workshop\App\Consumer\IdempotencyMiddleware;
use Workshop\App\Consumer\MessageBus;
use Workshop\App\Consumer\MessageDenormalizer;
use Workshop\App\Consumer\MessageInterpreter;
use Workshop\App\Consumer\TransactionMiddleware;
use Workshop\App\Producer\Message;
use Workshop\Enqueue\Console\EnqueueConsumeCommand;
use Workshop\Enqueue\EnqueueContextFactory;
use Workshop\Kafka\Serde\MessageSerializer;

/**
 * The validation branch returns Command::INVALID before any enqueue consumer is
 * built, so this runs without a broker or database — the collaborators are real
 * (or harmless doubles) but never reached.
 */
final class EnqueueConsumeCommandTest extends TestCase
{
    public function testMaxMustNotBeNegative(): void
    {
        $tester = $this->tester();

        $tester->execute([
            '--max' => '-1',
        ]);

        self::assertSame(Command::INVALID, $tester->getStatusCode());
        self::assertStringContainsString('--max must be >= 0', $tester->getDisplay());
    }

    private function tester(): CommandTester
    {
        $serializer = new class implements MessageSerializer {
            public function encode(Message $payload): string
            {
                return '';
            }

            public function decode(string $bytes, ?\AvroSchema $readerSchema = null): mixed
            {
                return null;
            }
        };

        // A stub, not a mock: validation returns INVALID before the DB is touched,
        // so the connection is never used and configures no expectations.
        $connection = self::createStub(Connection::class);

        $bus = new MessageBus(
            new ServiceLocator([]),
            new TransactionMiddleware($connection),
            new IdempotencyMiddleware(new EventDedup($connection)),
        );

        return new CommandTester(new EnqueueConsumeCommand(
            new EnqueueContextFactory('broker.test:29092'),
            new MessageInterpreter(new DtoRouting([]), $serializer, new MessageDenormalizer()),
            $bus,
            new ConsoleWriter(),
        ));
    }
}
