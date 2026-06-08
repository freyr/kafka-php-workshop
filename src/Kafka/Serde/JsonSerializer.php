<?php

declare(strict_types=1);

namespace Workshop\Kafka\Serde;

use Symfony\Component\Serializer\Encoder\JsonEncoder;
use Symfony\Component\Serializer\Serializer;
use Workshop\Produce\Message;

/**
 * The Block 1-2 serializer: JSON on the wire via the native Symfony Serializer,
 * the structured warm-up for the AVRO envelope in Block 3. encode() builds the
 * Message's envelope (the metadata + lower_snake payload it shares with the AVRO
 * path) and JSON-encodes that array; decode reverses it back to a plain
 * associative array. The wire field names are authored directly in each Message's
 * payload, so both directions agree without a name converter. The wire name is not
 * in the body — the producer stamps it as the `message-name` Kafka header.
 */
final readonly class JsonSerializer implements MessageSerializer
{
    private Serializer $serializer;

    public function __construct()
    {
        $this->serializer = new Serializer([], [new JsonEncoder()]);
    }

    public function encode(Message $payload): string
    {
        return $this->serializer->serialize($payload->envelope(), JsonEncoder::FORMAT);
    }

    public function decode(string $bytes): mixed
    {
        return $this->serializer->decode($bytes, JsonEncoder::FORMAT);
    }
}
