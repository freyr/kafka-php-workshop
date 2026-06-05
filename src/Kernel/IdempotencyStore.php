<?php

declare(strict_types=1);

namespace Workshop\Kernel;

/**
 * A file-backed stand-in for the "processed events" table from Block 5. Records
 * which event_ids have already been applied, so an idempotent consumer can skip
 * a redelivered message instead of repeating its side-effect.
 *
 * In production this is a database table written in the same transaction as the
 * business side-effect (see the block notes). Here it is a JSON file under
 * var/ — enough to make the idempotency boundary tangible in the workshop. The
 * key property the demo relies on: the record survives a crash that happens
 * before the Kafka offset is committed, which is exactly what lets the recovery
 * run recognise the duplicate.
 */
final readonly class IdempotencyStore
{
    private string $file;

    public function __construct()
    {
        $dir = dirname(__DIR__, 2) . '/var';
        if (! is_dir($dir)) {
            @mkdir($dir, 0o777, true);
        }

        $this->file = $dir . '/idempotency-store.json';
    }

    public function has(string $eventId): bool
    {
        return isset($this->load()[$eventId]);
    }

    public function remember(string $eventId): void
    {
        $seen = $this->load();
        $seen[$eventId] = true;
        file_put_contents($this->file, (string) json_encode($seen, JSON_PRETTY_PRINT));
    }

    public function reset(): void
    {
        if (is_file($this->file)) {
            unlink($this->file);
        }
    }

    public function count(): int
    {
        return \count($this->load());
    }

    /**
     * @return array<string, true>
     */
    private function load(): array
    {
        if (! is_file($this->file)) {
            return [];
        }

        $raw = file_get_contents($this->file);
        $data = json_decode((string) ($raw ?: '{}'), true);

        return \is_array($data) ? $data : [];
    }
}
