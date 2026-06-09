<?php

declare(strict_types=1);

namespace Workshop\Kafka\Runtime;

/**
 * The stop conditions for a consume loop, as data — three independent ways a run
 * ends, each off by default:
 *
 *  - maxMessages: stop after N handled records (a count cap, like --max).
 *  - maxRuntimeMs: a time-to-live — stop after the consumer has lived this long,
 *    regardless of traffic (like --ttl). Zero = unbounded.
 *  - stopOnIdle: end at the first empty poll — read the backlog until drained,
 *    then exit (the Block 1 batch style, like an enqueue receive-timeout).
 *
 * How long a single poll blocks is not a stop condition; it is the consumer's
 * fixed poll cadence (MessageConsumer::POLL_TIMEOUT_MS), so it lives there, not here.
 */
final readonly class RunLimits
{
    public function __construct(
        public int $maxMessages = 0,        // 0 = unlimited
        public int $maxRuntimeMs = 0,       // 0 = unlimited
        public bool $stopOnIdle = true,
    ) {
    }

    public function reachedMax(int $processed): bool
    {
        return $this->maxMessages > 0 && $processed >= $this->maxMessages;
    }

    public function deadlinePassed(int $startedAtMs, int $nowMs): bool
    {
        return $this->maxRuntimeMs > 0 && ($nowMs - $startedAtMs) >= $this->maxRuntimeMs;
    }
}
