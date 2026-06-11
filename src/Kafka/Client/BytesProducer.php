<?php

declare(strict_types=1);

namespace Workshop\Kafka\Client;

/**
 * The carry-bytes producing seam RawProducer implements: pre-serialized payload,
 * explicit topic, delivery verified through the tally. Consumers of the seam
 * (the outbox relay, the Block 7 ErrorRouter) need exactly this surface and
 * nothing of librdkafka, which is what makes their routing logic unit-testable.
 */
interface BytesProducer
{
    /**
     * @param array<string, string> $headers
     */
    public function produce(string $topic, ?string $key, string $payload, array $headers = []): void;

    /**
     * Block until every queued record is acked or failed; throw when the queue
     * cannot drain.
     */
    public function flush(): void;

    public function failedDeliveries(): int;

    public function resetDeliveryTally(): void;
}
