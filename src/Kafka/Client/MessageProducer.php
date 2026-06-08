<?php

declare(strict_types=1);

namespace Workshop\Kafka\Client;

use RdKafka\Producer;
use RdKafka\ProducerTopic;
use Workshop\Kafka\Serde\MessageSerializer;
use Workshop\Produce\Message;
use Workshop\Produce\MessageNameResolver;
use Workshop\Produce\MessageRouting;
use Workshop\Produce\Produced;
use Workshop\Produce\Route;

/**
 * A typed wrapper over \RdKafka\Producer. The single public send, produce(), takes
 * a Message and routes it to its own topic — resolved from the message's
 * #[MessageName] via the routing table — so callers never name a topic. By default
 * it keys on the message's partition key (crc32(key) % n → same key, same
 * partition, so an aggregate's events stay ordered); pass $unkeyed to let
 * librdkafka scatter records (consistent_random) for throughput with no ordering
 * guarantee.
 *
 * Every send runs the Message through the injected MessageSerializer, so the same
 * producer speaks JSON (Block 1-2) or AVRO envelopes (Block 3) — the serializer,
 * not the producer, knows the wire shape. Call close() to flush — librdkafka sends
 * asynchronously, so undelivered messages are lost if the process exits without
 * flushing.
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
        private readonly MessageRouting $routing,
        private readonly MessageNameResolver $names,
        private readonly int $flushTimeoutMs = 10000,
    ) {
    }

    /**
     * Produce one message to its routed topic. By default it is keyed by its
     * partition key for per-aggregate ordering; pass $unkeyed to scatter it across
     * partitions instead. The route is resolved up front, so an unrouted message
     * fails before any broker or registry contact.
     */
    public function produce(Message $message, bool $unkeyed = false): Produced
    {
        $route = $this->extractRouteFor($message);

        if ($unkeyed) {
            $this->unkeyed($route->topic, $message);
        } else {
            $this->keyed($route->topic, $message->partitionKey(), $message);
        }

        return new Produced($this->names->nameOf($message), $route);
    }

    private function keyed(string $topic, string $aggregateId, Message $message): void
    {
        $this->send($topic, RD_KAFKA_PARTITION_UA, $message, $aggregateId);
    }

    private function unkeyed(string $topic, Message $message): void
    {
        $this->send($topic, RD_KAFKA_PARTITION_UA, $message, null);
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

    private function send(string $topic, int $partition, Message $message, ?string $key): void
    {
        // The wire name and event id ride as headers, not in the body: a consumer can
        // route or skip a record by reading message-name, and dedup on event-id, both
        // without decoding the payload (event-id falls back to metadata.event_id).
        $headers = [
            'message-name' => $this->names->nameOf($message),
            'event-id' => $message->eventId(),
        ];

        $this->topicFor($topic)->producev($partition, 0, $this->serializer->encode($message), $key, $headers);
        // Serve delivery-report and error callbacks without blocking.
        $this->producer->poll(0);
    }

    private function topicFor(string $name): ProducerTopic
    {
        return $this->topics[$name] ??= $this->producer->newTopic($name);
    }

    private function extractRouteFor(Message $message): Route
    {
        return $this->routing->for($this->names->nameOf($message));
    }
}
