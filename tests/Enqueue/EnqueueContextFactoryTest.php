<?php

declare(strict_types=1);

namespace Workshop\Tests\Enqueue;

use PHPUnit\Framework\TestCase;
use Workshop\Enqueue\EnqueueContextFactory;
use Workshop\Enqueue\RawBodySerializer;
use Workshop\Kafka\Callback\DeliveryTally;

/**
 * The settings methods are pure, so every librdkafka value each role applies is
 * asserted here without touching a broker — the enqueue counterpart of
 * KafkaProfilesTest. Context construction is lazy (no connection until a
 * producer/consumer is actually used), so building the contexts is safe too.
 */
final class EnqueueContextFactoryTest extends TestCase
{
    private EnqueueContextFactory $factory;

    protected function setUp(): void
    {
        $this->factory = new EnqueueContextFactory('broker.test:29092');
    }

    public function testFpmProducerIsDurableButNotIdempotent(): void
    {
        $settings = $this->factory->fpmProducerSettings();

        self::assertSame('broker.test:29092', $settings['metadata.broker.list']);
        self::assertSame('all', $settings['acks']);
        self::assertSame('1', $settings['max.in.flight.requests.per.connection']);
        // One message per request: ordering is already serialized by max.in.flight=1,
        // so the idempotence handshake would only add latency to every fpm worker.
        self::assertArrayNotHasKey('enable.idempotence', $settings);
    }

    public function testFpmProducerFailsInRequestTimeNotBatchTime(): void
    {
        $settings = $this->factory->fpmProducerSettings();

        self::assertSame('0', $settings['linger.ms']);
        self::assertSame('10000', $settings['message.timeout.ms']);
    }

    public function testRelayProducerCarriesTheFullReliabilityStack(): void
    {
        $settings = $this->factory->relayProducerSettings();

        self::assertSame('true', $settings['enable.idempotence']);
        self::assertSame('all', $settings['acks']);
        self::assertSame('lz4', $settings['compression.type']);
        self::assertSame('50', $settings['linger.ms']);
    }

    public function testConsumerCommitsExplicitlyAndStartsFromEarliest(): void
    {
        $settings = $this->factory->consumerSettings('demo-group');

        self::assertSame('demo-group', $settings['group.id']);
        self::assertSame('false', $settings['enable.auto.commit']);
        self::assertSame('earliest', $settings['auto.offset.reset']);
    }

    public function testEveryContextSpeaksRawBytesNotEnqueueJson(): void
    {
        self::assertInstanceOf(RawBodySerializer::class, $this->factory->fpmProducer()->getSerializer());
        self::assertInstanceOf(RawBodySerializer::class, $this->factory->relayProducer(new DeliveryTally())->getSerializer());
        self::assertInstanceOf(RawBodySerializer::class, $this->factory->consumer('demo-group')->getSerializer());
    }
}
