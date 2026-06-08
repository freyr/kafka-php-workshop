<?php

declare(strict_types=1);

namespace Workshop\Produce;

/**
 * Declares the wire name of a Message. The base Message reads this autonomously
 * (reflection) to stamp the envelope metadata and to look the message up in the
 * produce/consume routing tables. The value is the kebab-case message name, e.g.
 * 'order-created'.
 */
#[\Attribute(\Attribute::TARGET_CLASS)]
final class MessageName
{
    public function __construct(
        public readonly string $value,
    ) {
    }
}
