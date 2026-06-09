<?php

declare(strict_types=1);

namespace Workshop\Tests\Console;

use Doctrine\DBAL\Connection;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Tester\CommandTester;
use Workshop\Console\ConsumeCommand;
use Workshop\Consume\DtoRouting;
use Workshop\Consume\EventDedup;
use Workshop\Consume\IdempotencyMiddleware;
use Workshop\Consume\MessageDenormalizer;
use Workshop\Consume\MessageInterpreter;
use Workshop\Consume\ProjectionHandler;
use Workshop\Consume\TransactionMiddleware;
use Workshop\Kafka\Client\ConsumerFactory;
use Workshop\Kafka\Config\BrokerProbe;
use Workshop\Kafka\Config\ConfBuilder;
use Workshop\Kafka\Config\KafkaProfiles;
use Workshop\Kafka\Serde\MessageSerializer;
use Workshop\Produce\Message;

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

            public function decode(string $bytes): mixed
            {
                return null;
            }
        };

        // A stub, not a mock: validation returns INVALID before the DB is touched,
        // so the connection is never used and configures no expectations.
        $connection = $this->createStub(Connection::class);

        $command = new ConsumeCommand(
            $consumers,
            new MessageInterpreter(new DtoRouting([]), $serializer, new MessageDenormalizer()),
            new ProjectionHandler($connection),
            new TransactionMiddleware($connection),
            new IdempotencyMiddleware(new EventDedup($connection)),
        );

        return new CommandTester($command);
    }
}
