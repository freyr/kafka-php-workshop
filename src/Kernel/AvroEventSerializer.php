<?php

declare(strict_types=1);

namespace Workshop\Kernel;

use FlixTech\AvroSerializer\Objects\RecordSerializer;
use FlixTech\SchemaRegistryApi\Registry\BlockingRegistry;
use FlixTech\SchemaRegistryApi\Registry\Cache\AvroObjectCacheAdapter;
use FlixTech\SchemaRegistryApi\Registry\CachedRegistry;
use FlixTech\SchemaRegistryApi\Registry\PromisingRegistry;
use GuzzleHttp\Client;

/**
 * Encodes/decodes events in the Confluent wire format (magic byte + 4-byte
 * schema id + AVRO binary) against a Schema Registry. Missing schemas/subjects
 * are auto-registered on first encode — the workshop trade-off; production
 * producers usually register out of band so the registry stays the gate.
 */
final readonly class AvroEventSerializer
{
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

    /**
     * @param array<string, mixed> $record
     */
    public function encode(string $subject, string $schemaJson, array $record): string
    {
        return $this->serializer->encodeRecord($subject, \AvroSchema::parse($schemaJson), $record);
    }

    /**
     * Decode a Confluent-framed message back to a PHP array.
     *
     * With no reader schema (the default) the message is decoded with its own
     * *writer* schema — the one pinned by the schema id in the bytes — so you get
     * the structure exactly as it was produced (an old message comes back in its
     * old shape, missing fields added later).
     *
     * Pass $readerSchemaJson to decode through AVRO *schema resolution*: the writer
     * schema parses the bytes, then the result is projected onto the reader schema,
     * filling defaults for fields the writer lacked and dropping fields the reader
     * does not declare. In production the reader schema is the one your code ships
     * with (pinned to the build, not fetched as registry "latest"), so every
     * historical version resolves to one consistent shape you can normalize into a
     * DTO. This only succeeds across the version range the subject's compatibility
     * mode guarantees — FULL for both directions, the *_TRANSITIVE variant for deep
     * history. A genuine incompatibility throws; route that poison message to a DLQ.
     *
     * @return array<string, mixed>
     */
    public function decode(string $binary, ?string $readerSchemaJson = null): array
    {
        $readerSchema = null !== $readerSchemaJson ? \AvroSchema::parse($readerSchemaJson) : null;

        /** @var array<string, mixed> $decoded */
        $decoded = $this->serializer->decodeMessage($binary, $readerSchema);

        return $decoded;
    }
}
