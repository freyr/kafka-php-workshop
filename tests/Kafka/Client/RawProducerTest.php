<?php

declare(strict_types=1);

namespace Workshop\Tests\Kafka\Client;

use PHPUnit\Framework\TestCase;
use Workshop\Kafka\Callback\DeliveryTally;
use Workshop\Kafka\Client\RawProducer;

/**
 * A real \RdKafka\Producer (final, unmockable) pointed at an unroutable broker
 * with a 200ms message timeout: produce() enqueues locally, and flush() then
 * serves the timed-out delivery report — which is exactly the mark-after-ack
 * seam the relay depends on, observable without any broker.
 */
final class RawProducerTest extends TestCase
{
    public function testFlushSurfacesDeliveryFailuresInTheTally(): void
    {
        $conf = new \RdKafka\Conf();
        $conf->set('metadata.broker.list', '127.0.0.1:9092');
        $conf->set('message.timeout.ms', '200');
        $conf->set('log_level', '0'); // keep the unroutable-broker noise out of test output

        $tally = new DeliveryTally();
        $tally->attachTo($conf);

        $producer = new RawProducer(new \RdKafka\Producer($conf), $tally, 5000);

        $producer->produce('outbox-test-topic', 'ord-1', '{"a":1}', [
            'message-name' => 'order.created',
            'event-id' => 'evt-1',
        ]);
        $producer->flush();

        // The message timed out (no broker), so the queue drained into a failure
        // report — the relay would now refuse to mark the row published.
        self::assertSame(1, $producer->failedDeliveries());

        $producer->resetDeliveryTally();
        self::assertSame(0, $producer->failedDeliveries());
    }
}
