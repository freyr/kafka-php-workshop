<?php

declare(strict_types=1);

namespace Workshop\Tests\Console;

use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Tester\CommandTester;
use Workshop\Console\EventProduceCommand;
use Workshop\Kafka\Client\ProducerFactory;
use Workshop\Kafka\Config\BrokerProbe;
use Workshop\Kafka\Config\ConfBuilder;
use Workshop\Kafka\Config\KafkaTuning;
use Workshop\Kafka\Config\ProfileRegistry;
use Workshop\Kafka\Serde\AvroEnvelopeSerializer;
use Workshop\Kafka\Serde\AvroEventSerializer;
use Workshop\Produce\MessageRouting;

/**
 * The unknown-type branch returns Command::INVALID before any client or registry
 * is touched, so it runs without a broker or Schema Registry.
 */
final class EventProduceCommandTest extends TestCase
{
    public function testUnknownTypeIsRejected(): void
    {
        $noop = new class implements BrokerProbe {
            public function assertReachable(string $brokers): void
            {
            }
        };
        $factory = new ProducerFactory(
            new ConfBuilder('broker.test:29092', $noop),
            new ProfileRegistry(new KafkaTuning()),
        );
        $routing = new MessageRouting([]);
        $avro = new AvroEnvelopeSerializer(new AvroEventSerializer('http://registry.test'));

        $tester = new CommandTester(new EventProduceCommand($factory, $routing, $avro));
        $tester->execute([
            'type' => 'no-such-type',
        ]);

        self::assertSame(Command::INVALID, $tester->getStatusCode());
        self::assertStringContainsString('Unknown event type', $tester->getDisplay());
    }
}
