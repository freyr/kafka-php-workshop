<?php

declare(strict_types=1);

namespace Workshop\App\Consumer;

use Workshop\Kafka\Runtime\PoisonMessageException;
use Workshop\Kafka\Serde\MessageSerializer;

/**
 * Turns a raw consumed record into a typed ConsumedMessage, or null for anything
 * this consumer does not handle. The pipeline is the consume-side mirror of the
 * producer's envelope, split into two halves so the decoded record is observable
 * before it becomes a DTO:
 *
 *   decode:    message-name header → route to a DTO class (skip unhandled types
 *              WITHOUT decoding) → AVRO-decode the payload to an array → strip the
 *              reserved metadata → a DecodedRecord (the raw business fields).
 *   denormalize: DecodedRecord → denormalize the business fields into the DTO.
 *
 * The split is what lets kafka:consume --print show the raw wire fields even for a
 * record that then fails to hydrate its DTO (the reader=writer evolution skip).
 *
 * The idempotency key (event_id) is read from the `event-id` header — available
 * before/without a decode — and falls back to the envelope's metadata.event_id for
 * records produced before the header existed. A record with no resolvable identity,
 * non-AVRO bytes, or a payload that does not fit its DTO is treated as unhandled
 * (null), never a crash: a shared-topic consumer must tolerate bytes it cannot use.
 *
 * With $poisonGate (kafka:consume --errors) the decode half distinguishes "not
 * mine" from "mine but broken": an unrouted name is still a silent skip, but a
 * ROUTED record whose bytes cannot be decoded — broken AVRO, missing Confluent
 * framing, no resolvable event id — throws PoisonMessageException so the command
 * can dead-letter it. The denormalize half keeps its null-skip contract either
 * way: DTO-hydration drift is the Block 4 exercise, not poison.
 */
final readonly class MessageInterpreter
{
    public function __construct(
        private DtoRouting $routing,
        private MessageSerializer $serializer,
        private MessageDenormalizer $denormalizer,
    ) {
    }

    public function interpret(\RdKafka\Message $message, ?\AvroSchema $readerSchema = null): ?ConsumedMessage
    {
        $decoded = $this->decode($message, $readerSchema);

        return null === $decoded ? null : $this->denormalize($decoded);
    }

    /**
     * The decode half: route by name, AVRO-decode, strip metadata. Stops short of
     * the DTO so callers can inspect the raw business fields (kafka:consume --print).
     *
     * @throws PoisonMessageException with $poisonGate, for a routed record whose
     *                                bytes can never decode
     */
    public function decode(\RdKafka\Message $message, ?\AvroSchema $readerSchema = null, bool $poisonGate = false): ?DecodedRecord
    {
        $name = $this->header($message, 'message-name');
        if ('' === $name) {
            // The header is not AVRO — it is the envelope CONVENTION, and without
            // it the record can never be routed. On a tolerant consumer that is a
            // silent skip; under the gate it is the contract-violation poison.
            if ($poisonGate) {
                throw new PoisonMessageException('Message carries no message-name header — the envelope convention is broken, so the record can never be routed.');
            }

            return null;
        }

        $dtoClass = $this->routing->for($name);
        if (null === $dtoClass) {
            return null; // a type this consumer does not handle — skip, no decode
        }

        try {
            // With a reader schema the record is resolved writer→reader (old records
            // gain fields added since, from their defaults); without one it decodes
            // in its own writer shape. kafka:consume --reader selects which.
            $decoded = $this->serializer->decode((string) $message->payload, $readerSchema);
        } catch (\Throwable $e) {
            if ($poisonGate) {
                throw new PoisonMessageException(sprintf('AVRO decode failed for routed message "%s": %s', $name, $e->getMessage()), previous: $e);
            }

            return null; // genuine decode failure (poison) — tolerate, do not crash
        }
        if (! is_array($decoded)) {
            if ($poisonGate) {
                throw new PoisonMessageException(sprintf('Routed message "%s" carries bytes that are not Confluent-framed AVRO.', $name));
            }

            return null; // not Confluent-framed AVRO (decode returned null)
        }
        /** @var array<string, mixed> $event */
        $event = $decoded;

        $eventId = $this->resolveEventId($message, $event);
        if ('' === $eventId) {
            if ($poisonGate) {
                throw new PoisonMessageException(sprintf('Routed message "%s" has no resolvable event id — it cannot be deduped, so it can never be processed safely.', $name));
            }

            return null; // no identity → cannot dedup safely, treat as unhandled
        }

        $payload = $event;
        unset($payload['metadata']);

        return new DecodedRecord($eventId, $name, $dtoClass, $payload, $message->partition, $message->offset);
    }

    /**
     * The denormalize half: shape a decoded record's business fields into its DTO.
     * Null when the payload does not fit the DTO (schema drift) — the routed-but-
     * unhydratable case the --reader exercise turns on.
     */
    public function denormalize(DecodedRecord $record): ?ConsumedMessage
    {
        try {
            $dto = $this->denormalizer->denormalize($record->payload, $record->dtoClass);
        } catch (\Throwable) {
            return null; // routed, but the payload does not fit the DTO (schema drift)
        }

        return new ConsumedMessage($record->eventId, $record->name, $dto, $record->partition, $record->offset);
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
