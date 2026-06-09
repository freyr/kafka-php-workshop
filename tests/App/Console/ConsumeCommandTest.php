<?php

declare(strict_types=1);

namespace Workshop\Tests\App\Console;

use Doctrine\DBAL\Connection;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Tester\CommandTester;
use Symfony\Component\DependencyInjection\ServiceLocator;
use Workshop\App\Console\ConsumeCommand;
use Workshop\App\Consumer\ConsoleWriter;
use Workshop\App\Consumer\DtoRouting;
use Workshop\App\Consumer\EventDedup;
use Workshop\App\Consumer\IdempotencyMiddleware;
use Workshop\App\Consumer\LatestSchemaResolver;
use Workshop\App\Consumer\MessageBus;
use Workshop\App\Consumer\MessageDenormalizer;
use Workshop\App\Consumer\MessageInterpreter;
use Workshop\App\Consumer\TransactionMiddleware;
use Workshop\App\Producer\Message;
use Workshop\App\Producer\MessageRouting;
use Workshop\Kafka\Client\ConsumerFactory;
use Workshop\Kafka\Config\BrokerProbe;
use Workshop\Kafka\Config\ConfBuilder;
use Workshop\Kafka\Config\KafkaProfiles;
use Workshop\Kafka\Serde\MessageSerializer;
use Workshop\Kafka\Serde\SchemaRegistryClient;

/**
 * The option-validation branches return Command::INVALID before any consumer is
 * built, so these run without a broker or database — the collaborators are real (or
 * harmless doubles) but never reached.
 */
final class ConsumeCommandTest extends TestCase
{
    public function testUnknownOffsetResetIsRejected(): void
    {
        $tester = $this->tester();

        $tester->execute([
            'topic' => 'orders',
            '--from' => 'middle',
        ]);

        self::assertSame(Command::INVALID, $tester->getStatusCode());
        self::assertStringContainsString('Unknown offset reset', $tester->getDisplay());
    }

    public function testUnknownConsumerProfileIsRejected(): void
    {
        $tester = $this->tester();

        $tester->execute([
            'topic' => 'orders',
            '--profile' => 'sometimes',
        ]);

        self::assertSame(Command::INVALID, $tester->getStatusCode());
        self::assertStringContainsString('Unknown consumer profile', $tester->getDisplay());
    }

    private function tester(): CommandTester
    {
        $noop = new class implements BrokerProbe {
            public function assertReachable(string $brokers): void
            {
            }
        };

        $consumers = new ConsumerFactory(
            new ConfBuilder('broker.test:29092', $noop),
            new KafkaProfiles(),
        );

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
        $connection = $this->createStub(Connection::class);

        // The bus's handler locator is empty: validation returns INVALID before any
        // record is dispatched, so no handler is ever resolved.
        $bus = new MessageBus(
            new ServiceLocator([]),
            new TransactionMiddleware($connection),
            new IdempotencyMiddleware(new EventDedup($connection)),
        );

        $command = new ConsumeCommand(
            $consumers,
            new MessageInterpreter(new DtoRouting([]), $serializer, new MessageDenormalizer()),
            $bus,
            new ConsoleWriter(),
            new LatestSchemaResolver(new MessageRouting([]), new SchemaRegistryClient(new \GuzzleHttp\Client())),
        );

        return new CommandTester($command);
    }
}
