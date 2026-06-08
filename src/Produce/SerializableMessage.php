<?php

declare(strict_types=1);

namespace Workshop\Produce;

/**
 * A message that knows how to serialize its own business payload to a pure PHP
 * array. Envelope metadata is NOT the message's concern — the Message base class
 * adds it. Returned keys are the on-the-wire payload field names (lower_snake).
 */
interface SerializableMessage
{
    /**
     * @return array<string, mixed>
     */
    public function toPayload(): array;
}
