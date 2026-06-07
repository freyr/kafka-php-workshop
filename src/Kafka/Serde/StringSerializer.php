<?php

declare(strict_types=1);

namespace Workshop\Kafka\Serde;

/**
 * Keeps payloads on the wire as plain strings — the Block 1-2 default, before AVRO
 * is introduced. The pure-rdkafka counterpart to the enqueue-coupled
 * Kernel\RawStringSerializer (which implements an enqueue interface and so cannot
 * be reused here).
 */
final readonly class StringSerializer implements MessageSerializer
{
    public function encode(mixed $payload): string
    {
        return (string) $payload;
    }

    public function decode(string $bytes): mixed
    {
        return $bytes;
    }
}
