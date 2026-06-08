<?php

declare(strict_types=1);

namespace Workshop\Kafka\Serde;

use FlixTech\AvroSerializer\Objects\RecordSerializer;
use FlixTech\SchemaRegistryApi\Exception\SchemaRegistryException;

/**
 * Block 3 serializer: the MessageSerializer seam over the Confluent AVRO wire
 * format (a 0x00 magic byte + 4-byte big-endian schema id + AVRO binary).
 *
 * The Schema Registry plumbing — Guzzle client → registry → RecordSerializer — is
 * assembled as data in config/services.yaml and injected ready-made, so this class
 * is pure framing logic. encode() takes an AvroPayload (subject + schema +
 * enveloped record) and returns wire-format bytes. Schemas are NOT auto-registered:
 * the registry stays a strict gate, so a subject must be registered out of band
 * (bin/console schema:register) before its messages can be produced — encode throws
 * a SchemaRegistryException otherwise. decode() returns the structured envelope, or
 * null when the bytes are not Confluent-framed so a dispatcher can skip non-AVRO
 * records instead of crashing.
 */
final readonly class AvroSerializer implements MessageSerializer
{
    /**
     * Confluent wire format: a 0x00 magic byte, a 4-byte big-endian schema id,
     * then the AVRO body — so a framed message is at least 5 bytes.
     */
    private const string MAGIC_BYTE = "\x00";
    private const int HEADER_BYTES = 5;

    public function __construct(
        private RecordSerializer $serializer,
    ) {
    }

    /**
     * @throws \AvroSchemaParseException
     * @throws SchemaRegistryException
     */
    public function encode(mixed $payload): string
    {
        if (! $payload instanceof AvroPayload) {
            throw new \InvalidArgumentException(sprintf('%s expects an %s payload, got %s.', self::class, AvroPayload::class, get_debug_type($payload)));
        }

        return $this->serializer->encodeRecord($payload->subject, \AvroSchema::parse($payload->schemaJson), $payload->record);
    }

    /**
     * Decode a Confluent-framed message back to the structured envelope, or null
     * for bytes that are not Confluent-framed — the events:dispatch robustness
     * contract: skip records you cannot decode instead of crashing.
     *
     * The message is decoded with its own *writer* schema — the one pinned by the
     * schema id in the bytes — so the structure comes back exactly as produced (an
     * old message returns in its old shape, missing fields added later). A genuine
     * decode failure throws; route that poison message to a DLQ.
     *
     * @return array<string, mixed>|null
     *
     * @throws SchemaRegistryException
     */
    public function decode(string $bytes): mixed
    {
        if (! $this->isConfluentFramed($bytes)) {
            return null;
        }

        /** @var array<string, mixed> $decoded */
        $decoded = $this->serializer->decodeMessage($bytes);

        return $decoded;
    }

    private function isConfluentFramed(string $bytes): bool
    {
        return strlen($bytes) >= self::HEADER_BYTES && self::MAGIC_BYTE === $bytes[0];
    }
}
