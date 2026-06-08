<?php

declare(strict_types=1);

namespace Workshop\Produce;

/**
 * Declares the wire name of a Message. MessageNameResolver reads this once per
 * concrete class (reflection, memoized) at the serialization stage; the resolved
 * name is stamped into the envelope metadata and used to look the message up in
 * the produce/consume routing tables. The value is the dotted message name
 * (minimum two segments), e.g. 'order.created'.
 */
#[\Attribute(\Attribute::TARGET_CLASS)]
final readonly class MessageName
{
    public function __construct(
        public string $value,
    ) {
    }
}
