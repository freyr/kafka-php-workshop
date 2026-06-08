<?php

declare(strict_types=1);

namespace Workshop\Kafka\Serde;

use Symfony\Component\PropertyInfo\Extractor\ReflectionExtractor;
use Symfony\Component\Serializer\Encoder\JsonEncoder;
use Symfony\Component\Serializer\NameConverter\CamelCaseToSnakeCaseNameConverter;
use Symfony\Component\Serializer\Normalizer\ArrayDenormalizer;
use Symfony\Component\Serializer\Normalizer\ObjectNormalizer;
use Symfony\Component\Serializer\Serializer;

/**
 * The Block 1-2 serializer: JSON on the wire via the native Symfony Serializer,
 * the structured warm-up for the AVRO envelope in Block 3. A typed message object
 * is normalized to an
 * array (camelCase properties → lower_snake keys via the name converter) and
 * JSON-encoded; decode reverses the encoding back to a plain associative array.
 * It mirrors the consume-side MessageDenormalizer's serializer setup so both
 * directions agree on the wire field names.
 */
final readonly class JsonSerializer implements MessageSerializer
{
    private Serializer $serializer;

    public function __construct()
    {
        $this->serializer = new Serializer(
            [
                new ArrayDenormalizer(),
                new ObjectNormalizer(
                    nameConverter: new CamelCaseToSnakeCaseNameConverter(),
                    propertyTypeExtractor: new ReflectionExtractor(),
                ),
            ],
            [new JsonEncoder()],
        );
    }

    public function encode(mixed $payload): string
    {
        return $this->serializer->serialize($payload, JsonEncoder::FORMAT);
    }

    public function decode(string $bytes): mixed
    {
        return $this->serializer->decode($bytes, JsonEncoder::FORMAT);
    }
}
