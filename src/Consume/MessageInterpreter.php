<?php

declare(strict_types=1);

namespace Workshop\Consume;

use Workshop\Kafka\Serde\MessageSerializer;

/**
 * Turns a raw consumed record into a typed ConsumedMessage, or null for anything
 * this consumer does not handle. The pipeline is the consume-side mirror of the
 * producer's envelope:
 *
 *   message-name header → route to a DTO class (skip unhandled types WITHOUT
 *   decoding) → AVRO-decode the payload to an array → strip the reserved metadata →
 *   denormalize the business fields into the read-model DTO.
 *
 * The idempotency key (event_id) is read from the `event-id` header — available
 * before/without a decode — and falls back to the envelope's metadata.event_id for
 * records produced before the header existed. A record with no resolvable identity,
 * non-AVRO bytes, or a payload that does not fit its DTO is treated as unhandled
 * (null), never a crash: a shared-topic consumer must tolerate bytes it cannot use.
 */
final readonly class MessageInterpreter
{
    public function __construct(
        private DtoRouting $routing,
        private MessageSerializer $serializer,
        private MessageDenormalizer $denormalizer,
    ) {
    }

    public function interpret(\RdKafka\Message $message): ?ConsumedMessage
    {
        $name = $this->header($message, 'message-name');
        $dtoClass = '' === $name ? null : $this->routing->for($name);
        if (null === $dtoClass) {
            return null; // a type this consumer does not handle — skip, no decode
        }

        try {
            $decoded = $this->serializer->decode((string) $message->payload);
        } catch (\Throwable) {
            return null; // genuine decode failure (poison) — tolerate, do not crash
        }
        if (! is_array($decoded)) {
            return null; // not Confluent-framed AVRO (decode returned null)
        }
        /** @var array<string, mixed> $event */
        $event = $decoded;

        $eventId = $this->resolveEventId($message, $event);
        if ('' === $eventId) {
            return null; // no identity → cannot dedup safely, treat as unhandled
        }

        $payload = $event;
        unset($payload['metadata']);

        try {
            $dto = $this->denormalizer->denormalize($payload, $dtoClass);
        } catch (\Throwable) {
            return null; // routed, but the payload does not fit the DTO (schema drift)
        }

        return new ConsumedMessage($eventId, $name, $dto, $message->partition, $message->offset);
    }

    /**
     * Prefer the out-of-band `event-id` header; fall back to the decoded envelope's
     * metadata.event_id so pre-header records still dedup.
     *
     * @param array<string, mixed> $event
     */
    private function resolveEventId(\RdKafka\Message $message, array $event): string
    {
        $header = $this->header($message, 'event-id');
        if ('' !== $header) {
            return $header;
        }

        $metadata = $event['metadata'] ?? null;
        $eventId = is_array($metadata) ? ($metadata['event_id'] ?? null) : null;

        return is_string($eventId) ? $eventId : '';
    }

    private function header(\RdKafka\Message $message, string $key): string
    {
        $value = $message->headers[$key] ?? null;

        return is_string($value) ? $value : '';
    }
}
