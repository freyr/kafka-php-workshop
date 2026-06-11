<?php

declare(strict_types=1);

namespace Workshop\Kafka\Runtime;

/**
 * A consecutive-failure circuit breaker for the transient-retry path. Counts
 * consecutive TransientException failures (a success resets to zero; poison and
 * permanent failures never count — they say nothing about dependency health) and
 * trips open at the threshold. After the cooldown the next attempt is the
 * half-open probe: success closes the breaker, another transient failure re-opens
 * it for a fresh cooldown.
 *
 * State is in-memory per process on purpose: consumers are single-process per
 * partition set, and a restarted consumer probing the dependency once is correct
 * behavior anyway. The clock arrives as a parameter so tests need no sleeping.
 */
final class CircuitBreaker
{
    private int $consecutiveFailures = 0;
    private bool $open = false;
    private int $openedAtMs = 0;

    public function __construct(
        private readonly int $threshold,
        private readonly int $cooldownMs,
    ) {
    }

    /**
     * Whether the next attempt may run: always when closed, and as the half-open
     * probe once an open breaker's cooldown has elapsed.
     */
    public function allowsAttempt(int $nowMs): bool
    {
        return ! $this->open || $this->isHalfOpen($nowMs);
    }

    /**
     * Open, cooldown elapsed — the next attempt is the probe.
     */
    public function isHalfOpen(int $nowMs): bool
    {
        return $this->open && $nowMs - $this->openedAtMs >= $this->cooldownMs;
    }

    public function remainingCooldownMs(int $nowMs): int
    {
        return $this->open ? max(0, $this->cooldownMs - ($nowMs - $this->openedAtMs)) : 0;
    }

    /**
     * @return bool true when this success CLOSED an open breaker (the probe
     *              succeeded) — the caller narrates the transition
     */
    public function onSuccess(): bool
    {
        $closed = $this->open;
        $this->open = false;
        $this->consecutiveFailures = 0;

        return $closed;
    }

    /**
     * @return bool true when this failure OPENED (or re-opened) the breaker —
     *              the caller narrates the transition
     */
    public function onTransientFailure(int $nowMs): bool
    {
        if ($this->open) {
            // The half-open probe failed — re-open for a fresh cooldown.
            $this->openedAtMs = $nowMs;

            return true;
        }

        if (++$this->consecutiveFailures >= $this->threshold) {
            $this->open = true;
            $this->openedAtMs = $nowMs;

            return true;
        }

        return false;
    }

    public function consecutiveFailures(): int
    {
        return $this->consecutiveFailures;
    }

    public function isOpen(): bool
    {
        return $this->open;
    }
}
