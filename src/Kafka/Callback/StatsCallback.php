<?php

declare(strict_types=1);

namespace Workshop\Kafka\Callback;

/**
 * librdkafka emits a JSON statistics blob every statistics.interval.ms, and this
 * callback is the only window a PHP client has into client-internal numbers (PHP
 * has no JMX). Blocks 1-3 do not use it; it exists so Block 8's config:stats can
 * consume the kit rather than owning the wiring. The decoded array is handed to an
 * injected consumer.
 */
final readonly class StatsCallback implements ConfCallback
{
    /**
     * @param (\Closure(array<string, mixed>): void)|null $onStats
     */
    public function __construct(
        private ?\Closure $onStats = null,
    ) {
    }

    public function attachTo(\RdKafka\Conf $conf): void
    {
        $conf->setStatsCb(function ($client, string $json): void {
            if (null === $this->onStats) {
                return;
            }

            $stats = json_decode($json, true);
            if (is_array($stats)) {
                ($this->onStats)($stats);
            }
        });
    }
}
