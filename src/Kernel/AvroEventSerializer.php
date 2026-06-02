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
     * @return array<string, mixed>
     */
    public function decode(string $binary): array
    {
        /** @var array<string, mixed> $decoded */
        $decoded = $this->serializer->decodeMessage($binary);

        return $decoded;
    }
}
