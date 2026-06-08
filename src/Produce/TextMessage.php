<?php

declare(strict_types=1);

namespace Workshop\Produce;

/**
 * The Block 1-2 message: a sequence number, the optional Kafka key echoed into the
 * value, and the templated text. Like every Block 3 event it is a Message, so it
 * flows through the same producer/serializer contract — the envelope (event_id,
 * timestamp, name) is supplied by the base, and JsonSerializer turns the enveloped
 * record into JSON. The per-message timestamp now lives in that envelope metadata
 * rather than as a standalone field.
 */
#[MessageName('text')]
final class TextMessage extends Message
{
    public static function create(int $sequence, ?string $key, string $text): self
    {
        return new self($key ?? '', [
            'sequence' => $sequence,
            'key' => $key,
            'text' => $text,
        ]);
    }
}
