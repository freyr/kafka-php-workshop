<?php

declare(strict_types=1);

namespace Workshop\App\Consumer;

/**
 * Maps a message name to the consumer DTO that reads it. Separate from the
 * produce-side routing: consumers own their read models, so this table lists only
 * the names this service consumes. Unknown names return null (ignored).
 */
final readonly class DtoRouting
{
    /**
     * @param array<string, class-string> $map
     */
    public function __construct(
        private array $map,
    ) {
    }

    /**
     * @return class-string|null
     */
    public function for(string $name): ?string
    {
        return $this->map[$name] ?? null;
    }
}
