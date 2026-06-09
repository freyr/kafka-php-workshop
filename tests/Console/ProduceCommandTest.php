<?php

declare(strict_types=1);

namespace Workshop\Tests\Console;

use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Tester\CommandTester;
use Workshop\Console\ProduceCommand;
use Workshop\Kafka\Client\ProducerFactory;
use Workshop\Kafka\Config\BrokerProbe;
use Workshop\Kafka\Config\ConfBuilder;
use Workshop\Kafka\Config\KafkaProfiles;
use Workshop\Kafka\Serde\MessageSerializer;
use Workshop\Produce\Message;
use Workshop\Produce\MessageCatalog;
use Workshop\Produce\MessageNameResolver;
use Workshop\Produce\MessageRouting;

/**
 * The input-validation branches return Command::INVALID before any client is
 * built, so these run without a broker — the factory and serializer are real but
 * never reached.
 */
final class ProduceCommandTest extends TestCase
{
    public function testUnknownMessageNameIsRejected(): void
    {
        $tester = $this->tester();

        $tester->execute([
            '--message-name' => 'not.a.message',
        ]);

        self::assertSame(Command::INVALID, $tester->getStatusCode());
        self::assertStringContainsString('Unknown message name', $tester->getDisplay());
        self::assertStringContainsString('order.created', $tester->getDisplay());
    }

    public function testCountMustBePositive(): void
    {
        $tester = $this->tester();

        $tester->execute([
            '--count' => '0',
        ]);

        self::assertSame(Command::INVALID, $tester->getStatusCode());
        self::assertStringContainsString('must be >= 1', $tester->getDisplay());
    }

    public function testPoolMustBePositive(): void
    {
        $tester = $this->tester();

        $tester->execute([
            '--pool' => '0',
        ]);

        self::assertSame(Command::INVALID, $tester->getStatusCode());
        self::assertStringContainsString('--pool must be >= 1', $tester->getDisplay());
    }

    public function testUnknownProfileIsRejected(): void
    {
        $tester = $this->tester();

        $tester->execute([
            '--profile' => 'turbo',
        ]);

        self::assertSame(Command::INVALID, $tester->getStatusCode());
        self::assertStringContainsString('Unknown profile: turbo', $tester->getDisplay());
        self::assertStringContainsString('idempotent | simple', $tester->getDisplay());
    }

    private function tester(): CommandTester
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

        $serializer = new class implements MessageSerializer {
            public function encode(Message $payload): string
            {
                return '';
            }

            public function decode(string $bytes): mixed
            {
                return null;
            }
        };

        return new CommandTester(new ProduceCommand($factory, $serializer, new MessageCatalog()));
    }
}
