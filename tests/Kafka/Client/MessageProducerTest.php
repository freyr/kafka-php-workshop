<?php

declare(strict_types=1);

namespace Workshop\Tests\Kafka\Client;

use PHPUnit\Framework\TestCase;
use Workshop\Kafka\Client\MessageProducer;
use Workshop\Kafka\Serde\MessageSerializer;

final class MessageProducerTest extends TestCase
{
    public function testEachSendEncodesThePayloadThroughTheSerializer(): void
    {
        $spy = new class implements MessageSerializer {
            /**
             * @var list<mixed>
             */
            public array $encoded = [];

            public function encode(mixed $payload): string
            {
                $this->encoded[] = $payload;

                return 'ENC:' . (is_scalar($payload) ? $payload : '');
            }

            public function decode(string $bytes): mixed
            {
                return $bytes;
            }
        };

        $producer = new MessageProducer($this->localProducer(), $spy);

        $producer->keyed('demo-topic', 'alice', 'order-1');
        $producer->unkeyed('demo-topic', 'order-2');
        $producer->toPartition('demo-topic', 0, 'order-3');

        // Every send routes the payload through the serializer before it is queued.
        self::assertSame(['order-1', 'order-2', 'order-3'], $spy->encoded);
    }

    /**
     * A real \RdKafka\Producer (the class is final and cannot be mocked), pointed
     * at an unroutable broker with a short message timeout so producev only enqueues
     * locally and the destructor cannot block. No flush() is called — broker
     * delivery is verified end-to-end in the demo pass.
     */
    private function localProducer(): \RdKafka\Producer
    {
        $conf = new \RdKafka\Conf();
        $conf->set('metadata.broker.list', '127.0.0.1:9092');
        $conf->set('message.timeout.ms', '200');
        $conf->set('log_level', '0'); // keep the unroutable-broker noise out of test output

        return new \RdKafka\Producer($conf);
    }
}
