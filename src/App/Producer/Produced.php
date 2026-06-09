<?php

declare(strict_types=1);

namespace Workshop\App\Producer;

/**
 * The outcome of producing one event: its resolved wire name and the route it was
 * sent on. Returned by EventProducer so a caller can report what went where
 * without re-resolving the name or re-reading the routing table.
 */
final readonly class Produced
{
    public function __construct(
        public string $name,
        public Route $route,
    ) {
    }
}
