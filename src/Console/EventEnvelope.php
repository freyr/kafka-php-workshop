<?php

declare(strict_types=1);

namespace Workshop\Console;

/**
 * Helpers for reading values out of a decoded AVRO envelope, which arrives as a
 * nested array<string, mixed>. They narrow mixed to concrete scalars at the read
 * site so the event commands stay static-analysis-clean without sprinkling casts.
 */
trait EventEnvelope
{
    /**
     * @param array<string, mixed> $event
     *
     * @return array<string, mixed>
     */
    private function metadataOf(array $event): array
    {
        $metadata = $event['metadata'] ?? [];
        if (! is_array($metadata)) {
            return [];
        }

        /** @var array<string, mixed> $metadata */
        return $metadata;
    }

    /**
     * @param array<string, mixed> $source
     */
    private function string(array $source, string $key): string
    {
        $value = $source[$key] ?? null;

        return is_scalar($value) ? (string) $value : '';
    }

    /**
     * Walk a nested key path, returning null if any hop is missing or not an array.
     *
     * @param array<string, mixed> $event
     */
    private function dig(array $event, string ...$keys): mixed
    {
        $cursor = $event;
        foreach ($keys as $key) {
            if (! is_array($cursor) || ! array_key_exists($key, $cursor)) {
                return null;
            }
            $cursor = $cursor[$key];
        }

        return $cursor;
    }

    /**
     * @param array<string, mixed> $event
     */
    private function digString(array $event, string ...$keys): string
    {
        $value = $this->dig($event, ...$keys);

        return is_scalar($value) ? (string) $value : '';
    }

    /**
     * @param array<string, mixed> $event
     */
    private function digInt(array $event, string ...$keys): int
    {
        $value = $this->dig($event, ...$keys);

        return is_numeric($value) ? (int) $value : 0;
    }
}
