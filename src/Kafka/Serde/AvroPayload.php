<?php

declare(strict_types=1);

namespace Workshop\Kafka\Serde;

/**
 * What AVRO needs to encode one message: the registry subject, the schema, and the
 * enveloped record (metadata + payload). It is the `payload` a producer hands to
 * an AvroEnvelopeSerializer — keeping the MessageSerializer::encode(mixed) contract
 * uniform while carrying the AVRO-specific inputs that a plain string never needs.
 */
final readonly class AvroPayload
{
    /**
     * @param array<string, mixed> $record
     */
    public function __construct(
        public string $subject,
        public string $schemaJson,
        public array $record,
    ) {
    }
}
