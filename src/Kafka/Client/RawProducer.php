<?php

declare(strict_types=1);

namespace Workshop\Kafka\Client;

use RdKafka\Producer;
use RdKafka\ProducerTopic;
use Workshop\Kafka\Callback\DeliveryTally;

/**
 * The outbox relay's producer: sends pre-serialized bytes to an explicit topic.
 * Where MessageProducer routes a typed Message through the routing table and a
 * serializer, the relay already holds both — the destination comes from the
 * row's aggregate_type and the payload is the JSON the business transaction
 * wrote — so this wrapper only carries bytes. It shares its DeliveryTally with
 * the relay loop: flush() drains the queue, then failedDeliveries() says whether
 * every record in the batch was actually acked, the precondition for marking
 * outbox rows published.
 */
final class RawProducer
{
    /**
     * @var array<string, ProducerTopic>
     */
    private array $topics = [];

    public function __construct(
        private readonly Producer $producer,
        private readonly DeliveryTally $deliveries,
        private readonly int $flushTimeoutMs = 10000,
    ) {
    }

    /**
     * @param array<string, string> $headers
     */
    public function produce(string $topic, ?string $key, string $payload, array $headers = []): void
    {
        $this->topicFor($topic)->producev(RD_KAFKA_PARTITION_UA, 0, $payload, $key, $headers);
        // Serve delivery-report and error callbacks without blocking.
        $this->producer->poll(0);
    }

    /**
     * Block until every queued record is either acked or failed — both outcomes
     * fire the delivery tally, so after a clean flush the tally is the batch's
     * truth. A flush that cannot drain (broker gone) fails loudly instead.
     */
    public function flush(): void
    {
        $result = $this->producer->flush($this->flushTimeoutMs);

        if (RD_KAFKA_RESP_ERR_NO_ERROR !== $result) {
            throw new \RuntimeException(sprintf('Producer flush did not complete within %dms — messages may be undelivered.', $this->flushTimeoutMs));
        }
    }

    public function failedDeliveries(): int
    {
        return $this->deliveries->failed();
    }

    public function resetDeliveryTally(): void
    {
        $this->deliveries->reset();
    }

    private function topicFor(string $name): ProducerTopic
    {
        return $this->topics[$name] ??= $this->producer->newTopic($name);
    }
}
