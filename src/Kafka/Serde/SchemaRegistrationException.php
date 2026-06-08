<?php

declare(strict_types=1);

namespace Workshop\Kafka\Serde;

/**
 * Raised when the Schema Registry refuses to register a schema. The registry is
 * the gate: it runs the subject's compatibility check server-side and rejects a
 * breaking schema with 409, or a malformed one with 422 — neither is stored.
 */
final class SchemaRegistrationException extends \RuntimeException
{
    public static function rejected(string $subject, int $status, string $body): self
    {
        $hint = match ($status) {
            409 => ' — incompatible with the latest registered version under the subject\'s compatibility level.',
            422 => ' — the schema is malformed or invalid.',
            default => '.',
        };
        $detail = self::registryMessage($body);

        return new self(sprintf(
            'Schema Registry rejected "%s" (HTTP %d)%s%s',
            $subject,
            $status,
            $hint,
            '' !== $detail ? " Registry said: {$detail}" : '',
        ));
    }

    public static function unexpectedBody(string $subject, string $body): self
    {
        return new self(sprintf('Schema Registry accepted "%s" but returned an unreadable body: %s', $subject, $body));
    }

    private static function registryMessage(string $body): string
    {
        $decoded = json_decode($body, true);

        return \is_array($decoded) && \is_string($decoded['message'] ?? null) ? $decoded['message'] : '';
    }
}
