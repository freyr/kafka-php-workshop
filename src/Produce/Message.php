<?php

declare(strict_types=1);

namespace Workshop\Produce;

use Symfony\Component\Uid\Uuid;

/**
 * Base for every producible message. It supplies the envelope autonomously:
 * a UUIDv7 event_id and a UTC epoch-millis timestamp are generated at
 * construction, and the name is read from the concrete class's #[MessageName]
 * attribute. Concrete messages only describe their business payload (toPayload)
 * and their Kafka partition key (partitionKey).
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
     * The wire name, read from the concrete class's #[MessageName] attribute.
     */
    final public function name(): string
    {
        $attributes = (new \ReflectionClass($this))->getAttributes(MessageName::class);
        if ([] === $attributes) {
            throw new \LogicException(sprintf('%s is missing the #[MessageName] attribute.', static::class));
        }

        return $attributes[0]->newInstance()->value;
    }

    /**
     * The full enveloped record that goes on the wire: a minimal metadata record
     * plus the flattened business payload.
     *
     * @return array<string, mixed>
     */
    final public function envelope(): array
    {
        $payload = $this->toPayload();
        if (array_key_exists('metadata', $payload)) {
            throw new \LogicException(sprintf('%s::toPayload() must not return a "metadata" key — it is reserved by the envelope.', static::class));
        }

        return [
            'metadata' => [
                'event_id' => $this->eventId,
                'timestamp' => $this->timestamp,
                'name' => $this->name(),
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
