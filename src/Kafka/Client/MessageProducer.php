<?php

declare(strict_types=1);

namespace Workshop\Kafka\Client;

use RdKafka\Producer;
use RdKafka\ProducerTopic;
use Workshop\Kafka\Serde\MessageSerializer;

/**
 * A thin, intent-revealing wrapper over \RdKafka\Producer. The three send methods
 * name the modeling decision instead of leaving it to raw producev flags:
 *
 *  - keyed()       same key → same partition (crc32(key) % n), so an aggregate's
 *                  events stay ordered. The key is the unit of ordering.
 *  - unkeyed()     no key → librdkafka scatters records (consistent_random) for
 *                  throughput; no ordering guarantee.
 *  - toPartition() pin a specific partition, overriding key-based routing.
 *
 * The payload is encoded through the injected MessageSerializer, so the same
 * producer speaks JSON (Block 1-2) or AVRO envelopes (Block 3) unchanged.
 * Call close() to flush — librdkafka sends asynchronously, so undelivered messages
 * are lost if the process exits without flushing.
 */
final class MessageProducer
{
    /**
     * @var array<string, ProducerTopic>
     */
    private array $topics = [];

    public function __construct(
        private readonly Producer $producer,
        private readonly MessageSerializer $serializer,
        private readonly int $flushTimeoutMs = 10000,
    ) {
    }

    public function keyed(string $topic, string $aggregateId, mixed $payload): void
    {
        $this->send($topic, RD_KAFKA_PARTITION_UA, $payload, $aggregateId);
    }

    public function unkeyed(string $topic, mixed $payload): void
    {
        $this->send($topic, RD_KAFKA_PARTITION_UA, $payload, null);
    }

    public function toPartition(string $topic, int $partition, mixed $payload, ?string $key = null): void
    {
        $this->send($topic, $partition, $payload, $key);
    }

    /**
     * Block until the producer queue drains, then fail loudly if anything is still
     * undelivered — the at-least-once guarantee on the produce side.
     */
    public function close(): void
    {
        $result = $this->producer->flush($this->flushTimeoutMs);

        if (RD_KAFKA_RESP_ERR_NO_ERROR !== $result) {
            throw new \RuntimeException(sprintf('Producer flush did not complete within %dms — messages may be undelivered.', $this->flushTimeoutMs));
        }
    }

    private function send(string $topic, int $partition, mixed $payload, ?string $key): void
    {
        $this->topicFor($topic)->producev($partition, 0, $this->serializer->encode($payload), $key);
        // Serve delivery-report and error callbacks without blocking.
        $this->producer->poll(0);
    }

    private function topicFor(string $name): ProducerTopic
    {
        return $this->topics[$name] ??= $this->producer->newTopic($name);
    }
}
