<?php

declare(strict_types=1);

namespace Workshop\Kafka\Runtime;

/**
 * The stop conditions for a consume loop, as data. The combination covers both
 * styles the workshop needs:
 *
 *  - Block 1 `consume` (read until idle): stopOnIdle = true — end at the first
 *    empty poll, like the enqueue receive-timeout behavior.
 *  - Block 8 `config:stats` (run for a while): stopOnIdle = false, maxRuntimeSeconds
 *    set — keep polling so lag can build and drain.
 */
final readonly class RunLimits
{
    public function __construct(
        public int $maxMessages = 0,        // 0 = unlimited
        public int $pollTimeoutMs = 5000,
        public int $maxRuntimeSeconds = 0,  // 0 = unlimited
        public bool $stopOnIdle = true,
    ) {
    }

    public function reachedMax(int $processed): bool
    {
        return $this->maxMessages > 0 && $processed >= $this->maxMessages;
    }

    public function deadlinePassed(int $startedAt, int $now): bool
    {
        return $this->maxRuntimeSeconds > 0 && ($now - $startedAt) >= $this->maxRuntimeSeconds;
    }
}
