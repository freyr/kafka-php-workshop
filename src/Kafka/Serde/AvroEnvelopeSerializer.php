<?php

declare(strict_types=1);

namespace Workshop\Kafka\Serde;

/**
 * Block 3 serializer: wraps the standalone AvroEventSerializer behind the
 * MessageSerializer seam. encode() takes an AvroPayload (subject + schema +
 * enveloped record) and returns Confluent wire-format bytes; decode() returns the
 * structured envelope, or null when the bytes are not Confluent-framed so a
 * dispatcher can skip non-AVRO records instead of crashing.
 */
final readonly class AvroEnvelopeSerializer implements MessageSerializer
{
    /**
     * Confluent wire format: a 0x00 magic byte, a 4-byte big-endian schema id,
     * then the AVRO body — so a framed message is at least 5 bytes.
     */
    private const string MAGIC_BYTE = "\x00";
    private const int HEADER_BYTES = 5;

    public function __construct(
        private AvroEventSerializer $avro,
    ) {
    }

    public function encode(mixed $payload): string
    {
        if (! $payload instanceof AvroPayload) {
            throw new \InvalidArgumentException(sprintf('%s expects an %s payload, got %s.', self::class, AvroPayload::class, get_debug_type($payload)));
        }

        return $this->avro->encode($payload->subject, $payload->schemaJson, $payload->record);
    }

    /**
     * @return array<string, mixed>|null the decoded envelope, or null for non-AVRO bytes
     */
    public function decode(string $bytes): mixed
    {
        if (! $this->isConfluentFramed($bytes)) {
            return null;
        }

        return $this->avro->decode($bytes);
    }

    private function isConfluentFramed(string $bytes): bool
    {
        return strlen($bytes) >= self::HEADER_BYTES && self::MAGIC_BYTE === $bytes[0];
    }
}
