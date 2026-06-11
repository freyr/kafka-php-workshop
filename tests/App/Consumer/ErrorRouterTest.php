<?php

declare(strict_types=1);

namespace Workshop\Tests\App\Consumer;

use PHPUnit\Framework\TestCase;
use Workshop\App\Consumer\ErrorRouter;
use Workshop\Kafka\Client\BytesProducer;
use Workshop\Kafka\Runtime\TransientException;

final class ErrorRouterTest extends TestCase
{
    public function testOffloadFromTheMainTopicGoesToItsRetryTopicWithPinnedOrigin(): void
    {
        $producer = $this->producer();
        $router = new ErrorRouter($producer);

        $destination = $router->offloadToRetry(
            $this->message(partition: 2, offset: 41, headers: [
                'message-name' => 'error.demo',
                'event-id' => 'e1',
            ]),
            new TransientException('dependency down'),
            'enet.ecommerce.outbox.ErrorDemo',
        );

        self::assertSame('enet.ecommerce.outbox.ErrorDemo.retry', $destination);
        $sent = $producer->sent[0];
        self::assertSame($destination, $sent['topic']);
        self::assertSame('key-1', $sent['key'], 'the original key travels — replays stay ordered per entity');
        self::assertSame('original-bytes', $sent['payload'], 'the original payload travels untouched');
        self::assertSame('error.demo', $sent['headers']['message-name']);
        self::assertSame('e1', $sent['headers']['event-id']);
        self::assertSame('enet.ecommerce.outbox.ErrorDemo', $sent['headers']['x-original-topic']);
        self::assertSame('2', $sent['headers']['x-original-partition']);
        self::assertSame('41', $sent['headers']['x-original-offset']);
        self::assertSame('1', $sent['headers']['x-retry-count']);
        self::assertSame(TransientException::class, $sent['headers']['x-error-class']);
        self::assertSame('dependency down', $sent['headers']['x-error-message']);
        self::assertArrayHasKey('x-failed-at', $sent['headers']);
        self::assertArrayNotHasKey('x-dead-letter-reason', $sent['headers']);
    }

    public function testASecondHopKeepsTheFirstOriginAndIncrementsTheCount(): void
    {
        $producer = $this->producer();
        $router = new ErrorRouter($producer);

        $router->offloadToRetry(
            $this->message(partition: 0, offset: 7, headers: [
                'x-original-topic' => 'enet.ecommerce.outbox.ErrorDemo',
                'x-original-partition' => '2',
                'x-original-offset' => '41',
                'x-retry-count' => '1',
            ]),
            new TransientException('still down'),
            'enet.ecommerce.outbox.ErrorDemo.retry',
        );

        $sent = $producer->sent[0];
        self::assertSame('enet.ecommerce.outbox.ErrorDemo.retry', $sent['topic'], 'the retry topic derives from the pinned origin, not the source');
        self::assertSame('enet.ecommerce.outbox.ErrorDemo', $sent['headers']['x-original-topic'], 'the first origin is pinned, never overwritten');
        self::assertSame('2', $sent['headers']['x-original-partition']);
        self::assertSame('41', $sent['headers']['x-original-offset']);
        self::assertSame('2', $sent['headers']['x-retry-count']);
    }

    public function testDeadLetterCarriesTheReason(): void
    {
        $producer = $this->producer();
        $router = new ErrorRouter($producer);

        $destination = $router->deadLetter(
            $this->message(partition: 1, offset: 3, headers: []),
            new \RuntimeException('cannot decode'),
            ErrorRouter::REASON_POISON,
            'enet.ecommerce.outbox.ErrorDemo',
        );

        self::assertSame('enet.ecommerce.outbox.ErrorDemo.dlq', $destination);
        self::assertSame('poison_message', $producer->sent[0]['headers']['x-dead-letter-reason']);
    }

    public function testPoisonOnTheRetryTopicDeadLettersToTheOriginalTopicsDlq(): void
    {
        $producer = $this->producer();
        $router = new ErrorRouter($producer);

        $destination = $router->deadLetter(
            $this->message(partition: 0, offset: 9, headers: [
                'x-original-topic' => 'enet.ecommerce.outbox.ErrorDemo',
            ]),
            new \RuntimeException('boom'),
            ErrorRouter::REASON_PERMANENT,
            'enet.ecommerce.outbox.ErrorDemo.retry',
        );

        self::assertSame('enet.ecommerce.outbox.ErrorDemo.dlq', $destination, 'never <topic>.retry.dlq');
    }

    public function testARetryTopicRecordWithoutPinnedHeadersStillAvoidsRetryDlq(): void
    {
        $producer = $this->producer();
        $router = new ErrorRouter($producer);

        $destination = $router->deadLetter(
            $this->message(partition: 0, offset: 9, headers: []),
            new \RuntimeException('boom'),
            ErrorRouter::REASON_POISON,
            'enet.ecommerce.outbox.ErrorDemo.retry',
        );

        self::assertSame('enet.ecommerce.outbox.ErrorDemo.dlq', $destination, 'the .retry suffix is stripped as a fallback');
    }

    public function testStaleDiagnosticsNeverTravelToTheNextHop(): void
    {
        $producer = $this->producer();
        $router = new ErrorRouter($producer);

        $router->offloadToRetry(
            $this->message(partition: 0, offset: 7, headers: [
                'x-original-topic' => 'enet.ecommerce.outbox.ErrorDemo',
                'x-error-class' => 'Stale\\Exception',
                'x-error-message' => 'old failure',
                'x-dead-letter-reason' => 'stale_reason',
            ]),
            new TransientException('fresh failure'),
            'enet.ecommerce.outbox.ErrorDemo.retry',
        );

        $headers = $producer->sent[0]['headers'];
        self::assertSame(TransientException::class, $headers['x-error-class']);
        self::assertSame('fresh failure', $headers['x-error-message']);
        self::assertArrayNotHasKey('x-dead-letter-reason', $headers, 'a retry hop is not dead-lettered');
    }

    public function testLongErrorMessagesAreTruncated(): void
    {
        $producer = $this->producer();
        $router = new ErrorRouter($producer);

        $router->deadLetter(
            $this->message(partition: 0, offset: 0, headers: []),
            new \RuntimeException(str_repeat('x', 2000)),
            ErrorRouter::REASON_PERMANENT,
            'topic',
        );

        self::assertSame(500, mb_strlen($producer->sent[0]['headers']['x-error-message']));
    }

    public function testAFailedDeliveryThrowsSoTheOffsetStaysUncommitted(): void
    {
        $producer = $this->producer(failedDeliveries: 1);
        $router = new ErrorRouter($producer);

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessageMatches('/not acked/');

        $router->deadLetter($this->message(partition: 0, offset: 0, headers: []), new \RuntimeException('x'), ErrorRouter::REASON_POISON, 'topic');
    }

    /**
     * @param array<string, string> $headers
     */
    private function message(int $partition, int $offset, array $headers): \RdKafka\Message
    {
        $message = new \RdKafka\Message();
        $message->err = RD_KAFKA_RESP_ERR_NO_ERROR;
        $message->payload = 'original-bytes';
        $message->key = 'key-1';
        $message->headers = $headers;
        $message->partition = $partition;
        $message->offset = $offset;

        return $message;
    }

    /**
     * @return BytesProducer&object{sent: list<array{topic: string, key: ?string, payload: string, headers: array<string, string>}>}
     */
    private function producer(int $failedDeliveries = 0): BytesProducer
    {
        return new class($failedDeliveries) implements BytesProducer {
            /**
             * @var list<array{topic: string, key: ?string, payload: string, headers: array<string, string>}>
             */
            public array $sent = [];

            public function __construct(
                private readonly int $failedDeliveries,
            ) {
            }

            public function produce(string $topic, ?string $key, string $payload, array $headers = []): void
            {
                $this->sent[] = [
                    'topic' => $topic,
                    'key' => $key,
                    'payload' => $payload,
                    'headers' => $headers,
                ];
            }

            public function flush(): void
            {
            }

            public function failedDeliveries(): int
            {
                return $this->failedDeliveries;
            }

            public function resetDeliveryTally(): void
            {
            }
        };
    }
}
