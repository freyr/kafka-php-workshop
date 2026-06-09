<?php

declare(strict_types=1);

namespace Workshop\App\Consumer;

use Workshop\App\Producer\MessageRouting;
use Workshop\Kafka\Serde\SchemaRegistryClient;

/**
 * Resolves the *latest* registered schema to use as a reader schema for a given
 * message name, by mapping name → subject (the produce-side routing) → the
 * subject's latest version in the registry. Results are memoized per name for the
 * lifetime of a consume run, so `kafka:consume --reader=latest` makes at most one
 * registry call per event type, not one per record.
 *
 * Returns null when the name is unrouted or the subject has no registered version
 * — the caller then falls back to the record's own writer schema.
 */
final class LatestSchemaResolver
{
    /**
     * @var array<string, ?\AvroSchema>
     */
    private array $cache = [];

    public function __construct(
        private readonly MessageRouting $routing,
        private readonly SchemaRegistryClient $registry,
    ) {
    }

    public function forMessageName(string $name): ?\AvroSchema
    {
        if (\array_key_exists($name, $this->cache)) {
            return $this->cache[$name];
        }

        return $this->cache[$name] = $this->resolve($name);
    }

    private function resolve(string $name): ?\AvroSchema
    {
        try {
            $subject = $this->routing->for($name)->subject;
            if ('' === $subject) {
                return null;
            }

            $json = $this->registry->latestSchemaJson($subject);

            return null !== $json ? \AvroSchema::parse($json) : null;
        } catch (\Throwable) {
            // An unrouted name, an unreachable registry, or an unparseable schema all
            // mean "no reader schema" — decode with the writer schema instead.
            return null;
        }
    }
}
