<?php

declare(strict_types=1);

namespace Workshop\Produce;

use Symfony\Component\Uid\Uuid;

/**
 * Base for every producible message. A concrete message is built through its
 * static create() named constructor, which hands the base two things: the Kafka
 * partition key and the business payload as a plain array. The base supplies the
 * envelope autonomously — a UUIDv7 event_id and a UTC epoch-millis timestamp are
 * generated at construction. The wire name is NOT part of the envelope; it is
 * resolved once per class from the concrete class's #[MessageName] attribute by
 * MessageNameResolver and stamped onto the record as the `message-name` Kafka
 * header by the producer, so consumers can route or skip without decoding.
 */
abstract class Message
{
    private readonly string $eventId;
    private readonly int $timestamp;

    /**
     * @param string               $partitionKey the Kafka message key — drives
     *                                           partitioning and ordering;
     *                                           intentionally kept out of the
     *                                           payload, a transport concern
     * @param array<string, mixed> $payload      the business payload (lower_snake
     *                                           wire field names)
     */
    protected function __construct(
        private readonly string $partitionKey,
        public readonly array $payload,
    ) {
        if (array_key_exists('metadata', $payload)) {
            throw new \LogicException(sprintf('%s payload must not contain a "metadata" key — it is reserved by the envelope.', static::class));
        }

        $this->eventId = Uuid::v7()->toRfc4122();
        $this->timestamp = self::nowMillis();
    }

    public function partitionKey(): string
    {
        return $this->partitionKey;
    }

    /**
     * The full enveloped record that goes on the wire: a minimal metadata record
     * plus the flattened business payload. The wire name is NOT carried in the
     * payload — it travels as the `message-name` Kafka header (stamped by the
     * producer) so a consumer can route or skip a record without decoding the body.
     *
     * @return array<string, mixed>
     */
    final public function envelope(): array
    {
        return [
            'metadata' => [
                'event_id' => $this->eventId,
                'timestamp' => $this->timestamp,
            ],
            ...$this->payload,
        ];
    }

    protected static function nowMillis(): int
    {
        return (int) floor(microtime(true) * 1000);
    }

    protected static function generateId(string $prefix): string
    {
        return $prefix . '-' . substr(Uuid::v4()->toRfc4122(), 0, 8);
    }

    /**
     * @return array{amount_cents: int, currency: string}
     */
    protected static function money(int $cents): array
    {
        return [
            'amount_cents' => $cents,
            'currency' => 'PLN',
        ];
    }
}
