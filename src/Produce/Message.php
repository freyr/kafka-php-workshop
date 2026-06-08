<?php

declare(strict_types=1);

namespace Workshop\Produce;

use Symfony\Component\Uid\Uuid;

/**
 * Base for every producible message. It supplies the envelope autonomously:
 * a UUIDv7 event_id and a UTC epoch-millis timestamp are generated at
 * construction. The wire name is NOT read here — it is resolved once per class
 * from the concrete class's #[MessageName] attribute by MessageNameResolver at
 * the serialization stage, and passed into envelope(). Concrete messages only
 * describe their business payload (toPayload) and their Kafka partition key
 * (partitionKey).
 */
abstract class Message implements SerializableMessage
{
    private readonly string $eventId;
    private readonly int $timestamp;

    public function __construct()
    {
        $this->eventId = Uuid::v7()->toRfc4122();
        $this->timestamp = self::nowMillis();
    }

    /**
     * The Kafka message key. Drives partitioning and ordering; intentionally kept
     * out of the payload — it is a transport concern, not business data.
     */
    abstract public function partitionKey(): string;

    /**
     * The full enveloped record that goes on the wire: a minimal metadata record
     * plus the flattened business payload. The wire name is supplied by the
     * caller (resolved once via MessageNameResolver), not re-derived here.
     *
     * @return array<string, mixed>
     */
    final public function envelope(string $name): array
    {
        $payload = $this->toPayload();
        if (array_key_exists('metadata', $payload)) {
            throw new \LogicException(sprintf('%s::toPayload() must not return a "metadata" key — it is reserved by the envelope.', static::class));
        }

        return [
            'metadata' => [
                'event_id' => $this->eventId,
                'timestamp' => $this->timestamp,
                'name' => $name,
            ],
            ...$payload,
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
