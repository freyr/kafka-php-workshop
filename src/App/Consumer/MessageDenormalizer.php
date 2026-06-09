<?php

declare(strict_types=1);

namespace Workshop\App\Consumer;

use Symfony\Component\PropertyInfo\Extractor\ReflectionExtractor;
use Symfony\Component\Serializer\NameConverter\CamelCaseToSnakeCaseNameConverter;
use Symfony\Component\Serializer\Normalizer\ArrayDenormalizer;
use Symfony\Component\Serializer\Normalizer\ObjectNormalizer;
use Symfony\Component\Serializer\Serializer;

/**
 * Builds a typed consumer DTO from a decoded payload array. The Symfony Serializer
 * maps lower_snake payload keys to camelCase constructor params (name converter)
 * and hydrates nested typed properties via the ReflectionExtractor — so a DTO can
 * declare only the fields it cares about and the rest of the payload is ignored.
 */
final readonly class MessageDenormalizer
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
            [],
        );
    }

    /**
     * @template T of object
     *
     * @param array<string, mixed> $payload
     * @param class-string<T>      $dto
     *
     * @return T
     */
    public function denormalize(array $payload, string $dto): object
    {
        return $this->serializer->denormalize($payload, $dto);
    }
}
