<?php

declare(strict_types=1);

namespace Workshop\App\Consumer;

use Workshop\Kafka\Client\BytesProducer;

/**
 * Routes a failed message off the partition: to <topic>.retry (transient, budget
 * exhausted or breaker failing fast) or <topic>.dlq (poison, permanent, or — on
 * the slow lane — never, since its budget is unbounded). The original payload
 * bytes, key, and headers travel UNTOUCHED — that is what makes replay safe and
 * ordered — plus diagnostic x-* headers that make the destination self-describing:
 *
 *   x-original-topic/-partition/-offset  first origin, pinned on the first hop
 *   x-error-class / x-error-message      what failed (message truncated)
 *   x-retry-count                        hops through the retry topic
 *   x-dead-letter-reason                 poison_message | handler_permanent | retries_exhausted
 *   x-failed-at                          ISO-8601 UTC of the routing decision
 *
 * Every off-load is flushed and delivery-checked BEFORE the caller lets the run
 * loop commit the source offset: a failed flush throws, the command dies, the
 * offset stays uncommitted, and the message redelivers — duplicated at worst
 * (event-id dedup absorbs it), never lost.
 */
final readonly class ErrorRouter
{
    public const string REASON_POISON = 'poison_message';
    public const string REASON_PERMANENT = 'handler_permanent';
    public const string REASON_EXHAUSTED = 'retries_exhausted';

    private const int ERROR_MESSAGE_MAX = 500;

    public function __construct(
        private BytesProducer $producer,
    ) {
    }

    /**
     * @return string the destination topic, for the narration
     */
    public function offloadToRetry(\RdKafka\Message $message, \Throwable $error, string $sourceTopic): string
    {
        $destination = $this->originalTopic($message, $sourceTopic) . '.retry';

        $headers = $this->diagnosticHeaders($message, $error, $sourceTopic);
        $headers['x-retry-count'] = (string) ($this->retryCount($message) + 1);

        $this->send($destination, $message, $headers);

        return $destination;
    }

    /**
     * @return string the destination topic, for the narration
     */
    public function deadLetter(\RdKafka\Message $message, \Throwable $error, string $reason, string $sourceTopic): string
    {
        // Poison found ON the retry topic dead-letters to the ORIGINAL topic's
        // DLQ (from x-original-topic), never to <topic>.retry.dlq.
        $destination = $this->originalTopic($message, $sourceTopic) . '.dlq';

        $headers = $this->diagnosticHeaders($message, $error, $sourceTopic);
        $headers['x-dead-letter-reason'] = $reason;

        $this->send($destination, $message, $headers);

        return $destination;
    }

    public function retryCount(\RdKafka\Message $message): int
    {
        $count = $this->header($message, 'x-retry-count');

        return is_numeric($count) ? (int) $count : 0;
    }

    /**
     * The first origin: the pinned x-original-topic when the message has already
     * hopped, the source topic otherwise. The suffix strip is a belt-and-braces
     * fallback for a retry-topic record that lost its headers — its DLQ must
     * still be the main topic's, never a derived <topic>.retry.dlq.
     */
    private function originalTopic(\RdKafka\Message $message, string $sourceTopic): string
    {
        $pinned = $this->header($message, 'x-original-topic');
        if ('' !== $pinned) {
            return $pinned;
        }

        return str_ends_with($sourceTopic, '.retry')
            ? substr($sourceTopic, 0, -\strlen('.retry'))
            : $sourceTopic;
    }

    /**
     * @return array<string, string>
     */
    private function diagnosticHeaders(\RdKafka\Message $message, \Throwable $error, string $sourceTopic): array
    {
        $headers = $this->originalHeaders($message);

        // Stale per-hop diagnostics never travel; each routing decision writes
        // its own. The pinned x-original-* and x-retry-count survive the unset
        // by being re-set below / by the caller.
        unset($headers['x-error-class'], $headers['x-error-message'], $headers['x-dead-letter-reason'], $headers['x-failed-at']);

        // Pin the first origin once; later hops keep the original coordinates.
        if (! isset($headers['x-original-topic'])) {
            $headers['x-original-topic'] = $sourceTopic;
            $headers['x-original-partition'] = (string) $message->partition;
            $headers['x-original-offset'] = (string) $message->offset;
        }

        $headers['x-error-class'] = $error::class;
        $headers['x-error-message'] = mb_substr($error->getMessage(), 0, self::ERROR_MESSAGE_MAX);
        $headers['x-failed-at'] = gmdate('Y-m-d\TH:i:s\Z');

        return $headers;
    }

    /**
     * Produce, flush, and verify delivery. The off-load must be durable BEFORE
     * the source offset commits; a failure here propagates so the command exits
     * without committing.
     *
     * @param array<string, string> $headers
     */
    private function send(string $destination, \RdKafka\Message $message, array $headers): void
    {
        $this->producer->resetDeliveryTally();
        $this->producer->produce(
            $destination,
            $message->key,
            (string) $message->payload,
            $headers,
        );
        $this->producer->flush();

        if ($this->producer->failedDeliveries() > 0) {
            throw new \RuntimeException(sprintf('Off-load to %s was not acked — leaving the source offset uncommitted so the message redelivers.', $destination));
        }
    }

    /**
     * @return array<string, string>
     */
    private function originalHeaders(\RdKafka\Message $message): array
    {
        return $message->headers;
    }

    private function header(\RdKafka\Message $message, string $key): string
    {
        $value = $message->headers[$key] ?? null;

        return is_string($value) ? $value : '';
    }
}
