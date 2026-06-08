<?php

declare(strict_types=1);

namespace Workshop\Tests\Kafka\Client;

use PHPUnit\Framework\TestCase;
use Workshop\Kafka\Client\MessageProducer;
use Workshop\Kafka\Serde\MessageSerializer;
use Workshop\Produce\Message;
use Workshop\Produce\MessageNameResolver;
use Workshop\Produce\MessageRouting;
use Workshop\Produce\OrderCreated;
use Workshop\Produce\TextMessage;

final class MessageProducerTest extends TestCase
{
    public function testEachSendEncodesTheMessageThroughTheSerializer(): void
    {
        $spy = new class implements MessageSerializer {
            /**
             * @var list<Message>
             */
            public array $encoded = [];

            public function encode(Message $payload): string
            {
                $this->encoded[] = $payload;

                return 'ENC';
            }

            public function decode(string $bytes): mixed
            {
                return $bytes;
            }
        };

        $routing = new MessageRouting([
            'text.plain' => [
                'type' => 'json',
                'topic' => 'demo-topic',
            ],
        ]);
        $producer = new MessageProducer($this->localProducer(), $spy, $routing, new MessageNameResolver());

        $keyed = TextMessage::create(1, 'alice', 'order-1');
        $unkeyed = TextMessage::create(2, null, 'order-2');

        $producer->produce($keyed);
        $producer->produce($unkeyed, true);

        // Every send routes the message through the serializer before it is queued.
        self::assertSame([$keyed, $unkeyed], $spy->encoded);
    }

    public function testProduceThrowsForAnUnroutedMessage(): void
    {
        $spy = new class implements MessageSerializer {
            public function encode(Message $payload): string
            {
                return 'ENC';
            }

            public function decode(string $bytes): mixed
            {
                return $bytes;
            }
        };

        // The route is resolved before any send, so an unrouted message fails fast.
        $producer = new MessageProducer($this->localProducer(), $spy, new MessageRouting([]), new MessageNameResolver());

        $this->expectException(\InvalidArgumentException::class);

        $producer->produce(OrderCreated::create('ord-1'));
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
