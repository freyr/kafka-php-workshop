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
use Workshop\Kafka\Config\KafkaTuning;
use Workshop\Kafka\Config\ProfileRegistry;
use Workshop\Kafka\Serde\JsonSerializer;
use Workshop\Produce\MessageNameResolver;
use Workshop\Produce\MessageRouting;

/**
 * The input-validation branches return Command::INVALID before any client is
 * built, so these run without a broker — the factory is real but never reached.
 */
final class ProduceCommandTest extends TestCase
{
    public function testKeyAndKeyCardinalityAreMutuallyExclusive(): void
    {
        $tester = $this->tester();

        $tester->execute([
            '--key' => 'a,b',
            '--key-cardinality' => '4',
        ]);

        self::assertSame(Command::INVALID, $tester->getStatusCode());
        self::assertStringContainsString('mutually exclusive', $tester->getDisplay());
    }

    public function testKeyCardinalityMustBePositive(): void
    {
        $tester = $this->tester();

        $tester->execute([
            '--key-cardinality' => '0',
        ]);

        self::assertSame(Command::INVALID, $tester->getStatusCode());
        self::assertStringContainsString('must be >= 1', $tester->getDisplay());
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
            new ProfileRegistry(new KafkaTuning()),
            new MessageRouting([]),
            new MessageNameResolver(),
        );

        return new CommandTester(new ProduceCommand($factory, new JsonSerializer(new MessageNameResolver())));
    }
}
