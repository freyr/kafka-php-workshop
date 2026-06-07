<?php

declare(strict_types=1);

namespace Workshop\Tests\Kafka\Config;

use PHPUnit\Framework\TestCase;
use Workshop\Kafka\Config\BrokerProbe;
use Workshop\Kafka\Config\ConfBuilder;
use Workshop\Kafka\Config\KafkaTuning;
use Workshop\Kafka\Config\ProfileRegistry;
use Workshop\Kafka\Config\TcpBrokerProbe;
use Workshop\Kernel\BrokerUnreachableException;

final class ConfBuilderTest extends TestCase
{
    private const BROKERS = 'broker.test:29092';

    private ProfileRegistry $profiles;

    protected function setUp(): void
    {
        $this->profiles = new ProfileRegistry(new KafkaTuning());
    }

    public function testBuildSetsBrokerListAndClientId(): void
    {
        $conf = $this->builder()->build($this->profiles->get('producer.simple'))->dump();

        self::assertSame(self::BROKERS, $conf['metadata.broker.list']);
        self::assertStringStartsWith('workshop.producer.', $conf['client.id']);
    }

    public function testIdempotentProfileSettingsLandOnTheConf(): void
    {
        $conf = $this->builder()->build($this->profiles->get('producer.idempotent'))->dump();

        self::assertSame('true', $conf['enable.idempotence']);
        // compression.type is an alias; librdkafka's dump() uses the canonical name.
        self::assertSame('lz4', $conf['compression.codec']);
        // acks=all is set too, but it is a topic-scoped librdkafka property and
        // does not surface in the global Conf::dump(); ProfileRegistryTest covers it.
    }

    public function testRuntimeOverridesWinOverProfileAndDefaults(): void
    {
        $conf = $this->builder()->build(
            $this->profiles->get('consumer.at-least-once'),
            [
                'group.id' => 'orders-worker',
                'client.id' => 'custom-id',
            ],
        )->dump();

        self::assertSame('orders-worker', $conf['group.id']);
        self::assertSame('custom-id', $conf['client.id']);
    }

    public function testGethostnamePlaceholderIsResolved(): void
    {
        // consumer.at-least-once carries group.instance.id = gethostname().
        $conf = $this->builder()->build($this->profiles->get('consumer.at-least-once'))->dump();

        self::assertArrayHasKey('group.instance.id', $conf);
        self::assertNotSame('', $conf['group.instance.id']);
        self::assertStringNotContainsString('gethostname', $conf['group.instance.id']);
    }

    public function testEphemeralProfileLeavesGroupInstanceIdUnset(): void
    {
        $conf = $this->builder()->build($this->profiles->get('consumer.ephemeral'))->dump();

        // librdkafka reports an unset group.instance.id as empty string.
        self::assertSame('', $conf['group.instance.id'] ?? '');
        // cooperative-sticky is a global property, so it is observable in dump().
        self::assertSame('cooperative-sticky', $conf['partition.assignment.strategy']);
    }

    public function testProbeFailurePropagates(): void
    {
        $throwing = new class implements BrokerProbe {
            public function assertReachable(string $brokers): void
            {
                throw new BrokerUnreachableException($brokers, 'connection refused');
            }
        };

        $this->expectException(BrokerUnreachableException::class);

        (new ConfBuilder(self::BROKERS, $throwing))->build($this->profiles->get('producer.simple'));
    }

    public function testTcpProbeThrowsOnAnUnreachablePort(): void
    {
        $this->expectException(BrokerUnreachableException::class);

        // Nothing listens on 127.0.0.1:1 — connection refused fast.
        (new TcpBrokerProbe(0.5))->assertReachable('127.0.0.1:1');
    }

    private function builder(): ConfBuilder
    {
        $noop = new class implements BrokerProbe {
            public function assertReachable(string $brokers): void
            {
            }
        };

        return new ConfBuilder(self::BROKERS, $noop);
    }
}
