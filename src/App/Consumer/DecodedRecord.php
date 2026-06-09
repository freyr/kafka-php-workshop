<?php

declare(strict_types=1);

namespace Workshop\App\Consumer;

/**
 * A consumed record after the decode half of interpretation, before it is shaped
 * into a DTO. It carries the raw decoded business payload (the AVRO record minus
 * the reserved metadata envelope) alongside the routing identity — so kafka:consume
 * --print can show the fields actually on the wire even for a record that then
 * fails to hydrate its DTO (the reader=writer schema-evolution skip). The
 * denormalize half turns this into a ConsumedMessage.
 */
final readonly class DecodedRecord
{
    /**
     * @param class-string         $dtoClass the read-model the name routes to
     * @param array<string, mixed> $payload  decoded business fields (metadata stripped)
     */
    public function __construct(
        public string $eventId,
        public string $name,
        public string $dtoClass,
        public array $payload,
        public int $partition,
        public int $offset,
    ) {
    }
}
