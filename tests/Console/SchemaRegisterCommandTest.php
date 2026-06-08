<?php

declare(strict_types=1);

namespace Workshop\Tests\Console;

use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Tester\CommandTester;
use Workshop\Console\SchemaRegisterCommand;
use Workshop\Kafka\Serde\SchemaRegistryClient;
use Workshop\Produce\MessageRouting;

/**
 * Both guard branches return before any registry call, so they run without a
 * Schema Registry — the client is never constructed.
 */
final class SchemaRegisterCommandTest extends TestCase
{
    public function testUnknownTypeIsRejected(): void
    {
        $tester = new CommandTester($this->command(new MessageRouting([])));
        $tester->execute([
            'type' => 'no-such-type',
        ]);

        self::assertSame(Command::INVALID, $tester->getStatusCode());
        self::assertStringContainsString('Unknown event type', $tester->getDisplay());
    }

    public function testUnreadableSchemaFileIsRejected(): void
    {
        $routing = new MessageRouting([
            'order-created' => [
                'topic' => 'enet.ecommerce.orders',
                'subject' => 'com.ecommerce.orders.order_created',
                'schema' => '/tmp/canonical-not-used-here.avsc',
            ],
        ]);

        $tester = new CommandTester($this->command($routing));
        $tester->execute([
            'type' => 'order-created',
            'schema-file' => '/no/such/path.avsc',
        ]);

        self::assertSame(Command::INVALID, $tester->getStatusCode());
        self::assertStringContainsString('Cannot read schema file', $tester->getDisplay());
    }

    private function command(MessageRouting $routing): SchemaRegisterCommand
    {
        $registry = (new \ReflectionClass(SchemaRegistryClient::class))->newInstanceWithoutConstructor();

        return new SchemaRegisterCommand($registry, $routing);
    }
}
