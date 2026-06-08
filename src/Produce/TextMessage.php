<?php

declare(strict_types=1);

namespace Workshop\Produce;

/**
 * The Block 1-2 message: a plain, un-enveloped record carried as JSON. Unlike the
 * Block 3 Message hierarchy (which wraps a metadata envelope around a business
 * payload for AVRO), this is deliberately flat — a sequence number, the optional
 * Kafka key echoed into the value, the templated text, and a UTC epoch-millis
 * stamp. It exists to give the native Symfony Serializer a typed object to turn
 * into JSON, so Blocks 1-2 teach structured serialization before AVRO arrives.
 */
final readonly class TextMessage
{
    public function __construct(
        public int $sequence,
        public ?string $key,
        public string $text,
        public int $timestamp,
    ) {
    }

    public static function create(int $sequence, ?string $key, string $text): self
    {
        return new self($sequence, $key, $text, (int) floor(microtime(true) * 1000));
    }
}
