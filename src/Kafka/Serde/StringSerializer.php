<?php

declare(strict_types=1);

namespace Workshop\Kafka\Serde;

/**
 * Keeps payloads on the wire as plain strings — the Block 1-2 default, before AVRO
 * is introduced.
 */
final readonly class StringSerializer implements MessageSerializer
{
    public function encode(mixed $payload): string
    {
        if (is_string($payload)) {
            return $payload;
        }

        if (is_scalar($payload) || $payload instanceof \Stringable) {
            return (string) $payload;
        }

        throw new \InvalidArgumentException(sprintf('%s can only encode stringable payloads, got %s.', self::class, get_debug_type($payload)));
    }

    public function decode(string $bytes): mixed
    {
        return $bytes;
    }
}
