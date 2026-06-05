<?php

declare(strict_types=1);

namespace Workshop\Kernel;

use Enqueue\RdKafka\RdKafkaMessage;

/**
 * Block 7: routes a failed message to the next retry tier or to the Dead Letter
 * Topic, carrying full error metadata in Kafka headers. The original payload
 * bytes and key are preserved untouched, so replay is a straight re-publish.
 *
 * The retry chain is intentionally short with demo-friendly delays (5s, 30s);
 * production uses minutes (1m / 5m / 30m). Each tier is its own topic so a slow
 * retry never blocks the live partition — the message moves off the hot path and
 * the original consumer acks and moves on.
 *
 * Every publish opens and closes its own producer context, which flushes and
 * waits for the broker ack. That is deliberate: the routed message is durable on
 * the broker *before* the caller commits the original offset, so a crash in the
 * gap cannot lose it (the producer-side mirror of Block 5's commit-last rule).
 */
final readonly class RetryRouter
{
    public const string DLT_TOPIC = 'enet.internal.dead-letters';

    /**
     * Ordered retry tiers. Index N is the destination for a message that has
     * already taken N hops (x-retry-count = N). Past the end of the chain, the
     * message is exhausted and goes to the DLT.
     *
     * @var list<array{suffix: string, delay: int}>
     */
    private const array CHAIN = [
        [
            'suffix' => '.retry.5s',
            'delay' => 5,
        ],
        [
            'suffix' => '.retry.30s',
            'delay' => 30,
        ],
    ];

    public function __construct(
        private KafkaContextFactory $kafka,
    ) {
    }

    /**
     * Route a retryable failure to the next retry tier, or to the DLT when the
     * chain is exhausted. $currentRetry is the message's current x-retry-count
     * (0 on the original topic). Returns the destination topic name.
     */
    public function retry(RdKafkaMessage $original, \Throwable $error, int $currentRetry): string
    {
        if (! isset(self::CHAIN[$currentRetry])) {
            return $this->deadLetter($original, $error, 'retries_exhausted', $currentRetry);
        }

        $tier = self::CHAIN[$currentRetry];
        $target = $this->originalTopic($original) . $tier['suffix'];

        $headers = $this->headers($original, $error, $currentRetry + 1);
        $headers['x-next-retry-after'] = (string) (time() + $tier['delay']);

        $this->publish($target, $original->getBody(), $original->getKey(), $headers);

        return $target;
    }

    /**
     * Dead-letter a message with full diagnostic metadata. $reason is one of
     * poison_message / deserialization_error / retries_exhausted. Returns the
     * DLT topic name.
     */
    public function deadLetter(RdKafkaMessage $original, \Throwable $error, string $reason, int $retryCount): string
    {
        $headers = $this->headers($original, $error, $retryCount);
        $headers['x-error-stack-trace'] = mb_substr($error->getTraceAsString(), 0, 4096);
        $headers['x-dead-letter-reason'] = $reason;
        $headers['x-dead-letter-timestamp'] = (string) time();

        $this->publish(self::DLT_TOPIC, $original->getBody(), $original->getKey(), $headers);

        return self::DLT_TOPIC;
    }

    /**
     * Build the carried-forward error metadata. The x-original-* fields are
     * pinned on the *first* failure (read from the live Kafka message) and then
     * carried verbatim through every subsequent hop, so they always name the true
     * source topic/partition/offset — that is what the replay tool routes by.
     *
     * @return array<string, string>
     */
    private function headers(RdKafkaMessage $original, \Throwable $error, int $retryCount): array
    {
        $existing = $original->getHeaders();
        $kafka = $original->getKafkaMessage();

        return [
            'x-original-topic' => $this->originalTopic($original),
            'x-original-partition' => (string) ($existing['x-original-partition'] ?? $kafka?->partition ?? ''),
            'x-original-offset' => (string) ($existing['x-original-offset'] ?? $kafka?->offset ?? ''),
            'x-original-key' => (string) ($existing['x-original-key'] ?? $original->getKey() ?? ''),
            'x-error-class' => $error::class,
            'x-error-message' => mb_substr($error->getMessage(), 0, 1024),
            'x-retry-count' => (string) $retryCount,
            'x-first-failure-ts' => (string) ($existing['x-first-failure-ts'] ?? time()),
            'x-consumer-host' => gethostname() ?: 'unknown',
        ];
    }

    private function originalTopic(RdKafkaMessage $original): string
    {
        $existing = $original->getHeaders();
        $pinned = (string) ($existing['x-original-topic'] ?? '');
        if ('' !== $pinned) {
            return $pinned;
        }

        return $original->getKafkaMessage()?->topic_name ?? '';
    }

    /**
     * @param array<string, string> $headers
     */
    private function publish(string $topic, string $body, ?string $key, array $headers): void
    {
        $context = $this->kafka->forProducer();
        $message = $context->createMessage($body, [], $headers);
        $message->setKey($key);
        $context->createProducer()->send($context->createTopic($topic), $message);
        $context->close();
    }
}
