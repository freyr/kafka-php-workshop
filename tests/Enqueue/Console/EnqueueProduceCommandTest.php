<?php

declare(strict_types=1);

namespace Workshop\Tests\Enqueue\Console;

use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Tester\CommandTester;
use Workshop\App\Producer\Message;
use Workshop\App\Producer\MessageCatalog;
use Workshop\App\Producer\MessageRouting;
use Workshop\Enqueue\Console\EnqueueProduceCommand;
use Workshop\Enqueue\EnqueueContextFactory;
use Workshop\Kafka\Serde\MessageSerializer;

/**
 * The validation branches return Command::INVALID before any enqueue context is
 * built, so these run without a broker — same contract as ProduceCommandTest.
 */
final class EnqueueProduceCommandTest extends TestCase
{
    public function testUnknownMessageNameIsRejected(): void
    {
        $tester = $this->tester();

        $tester->execute([
            '--message-name' => 'order.exploded',
        ]);

        self::assertSame(Command::INVALID, $tester->getStatusCode());
        self::assertStringContainsString('Unknown message name: order.exploded', $tester->getDisplay());
        self::assertStringContainsString('order.created', $tester->getDisplay());
    }

    public function testCountMustBePositive(): void
    {
        $tester = $this->tester();

        $tester->execute([
            '--count' => '0',
        ]);

        self::assertSame(Command::INVALID, $tester->getStatusCode());
        self::assertStringContainsString('--count must be >= 1', $tester->getDisplay());
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

        return new CommandTester(new EnqueueProduceCommand(
            new EnqueueContextFactory('broker.test:29092'),
            new MessageCatalog(),
            new MessageRouting([]),
            $serializer,
        ));
    }
}
