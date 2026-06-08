<?php

declare(strict_types=1);

namespace Workshop\Tests\Support;

use Workshop\Produce\Message;
use Workshop\Produce\MessageName;

/**
 * A minimal Message for serde and producer tests: an arbitrary payload behind the
 * same envelope contract as the production messages, with a fixed wire name. It
 * lets those tests assert wire shape without coupling to any real event's payload.
 */
#[MessageName('fixture')]
final class FixtureMessage extends Message
{
    /**
     * @param array<string, mixed> $payload
     */
    public static function create(?string $key, array $payload): self
    {
        return new self($key ?? '', $payload);
    }
}
