<?php

declare(strict_types=1);

namespace Workshop\Kafka\Serde;

use FlixTech\AvroSerializer\Objects\RecordSerializer;
use FlixTech\SchemaRegistryApi\Registry\BlockingRegistry;
use FlixTech\SchemaRegistryApi\Registry\Cache\AvroObjectCacheAdapter;
use FlixTech\SchemaRegistryApi\Registry\CachedRegistry;
use FlixTech\SchemaRegistryApi\Registry\PromisingRegistry;
use GuzzleHttp\Client;

/**
 * Block 3 serializer: the MessageSerializer seam over the Confluent AVRO wire
 * format (a 0x00 magic byte + 4-byte big-endian schema id + AVRO binary), backed
 * by a Schema Registry.
 *
 * encode() takes an AvroPayload (subject + schema + enveloped record) and returns
 * Confluent wire-format bytes, auto-registering missing schemas/subjects on first
 * encode — the workshop trade-off; production producers usually register out of
 * band so the registry stays the gate. decode() returns the structured envelope,
 * or null when the bytes are not Confluent-framed so a dispatcher can skip
 * non-AVRO records instead of crashing.
 */
final readonly class AvroEnvelopeSerializer implements MessageSerializer
{
    /**
     * Confluent wire format: a 0x00 magic byte, a 4-byte big-endian schema id,
     * then the AVRO body — so a framed message is at least 5 bytes.
     */
    private const string MAGIC_BYTE = "\x00";
    private const int HEADER_BYTES = 5;

    private RecordSerializer $serializer;

    public function __construct(string $schemaRegistryUrl)
    {
        $registry = new CachedRegistry(
            new BlockingRegistry(new PromisingRegistry(new Client([
                'base_uri' => $schemaRegistryUrl,
            ]))),
            new AvroObjectCacheAdapter(),
        );

        $this->serializer = new RecordSerializer($registry, [
            RecordSerializer::OPTION_REGISTER_MISSING_SCHEMAS => true,
            RecordSerializer::OPTION_REGISTER_MISSING_SUBJECTS => true,
        ]);
    }

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
