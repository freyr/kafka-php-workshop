<?php

declare(strict_types=1);

namespace Workshop\Kafka\Runtime;

/**
 * The two error-handling lanes `kafka:consume --errors` exposes, plus Off (the
 * default — today's tolerant null-skip behavior, which keeps the Block 4/5 demos
 * untouched). A policy bundles everything lane-dependent: the in-process retry
 * budget, the backoff curve, and how the circuit breaker behaves when open.
 *
 *  - Main: the hot path. A tiny retry budget with short delays (every in-process
 *    retry blocks the partition), then the message off-loads to <topic>.retry and
 *    the partition advances. Breaker open = fail fast: skip retries entirely and
 *    off-load each transiently-failing message immediately.
 *  - Slow: the retry-topic drain. Its whole job is to wait, so attempts are
 *    unbounded with long, capped, doubling delays. Breaker open = pause: stop
 *    consuming for the cooldown instead of hammering the dependency (off-loading
 *    from the retry topic would have nowhere better to go).
 */
enum ErrorPolicy: string
{
    case Main = 'main';
    case Slow = 'slow';
    case Off = 'off';

    public static function fromOption(string $value): self
    {
        return self::tryFrom($value)
            ?? throw new \InvalidArgumentException(sprintf('Unknown error policy "%s". Use one of: %s.', $value, implode(', ', array_map(static fn (self $p): string => $p->value, self::cases()))));
    }

    public function enabled(): bool
    {
        return self::Off !== $this;
    }

    /**
     * In-process attempts before the message off-loads to the retry topic.
     * Null = unbounded (the slow lane never exhausts — only poison/permanent
     * failures leave it, straight to the DLQ).
     */
    public function maxAttempts(): ?int
    {
        return match ($this) {
            self::Main => 3,
            self::Slow, self::Off => null,
        };
    }

    /**
     * Delay before the NEXT attempt, after $attempt failed. Main doubles from 1 s
     * (1 s · 2 s · 4 s — short, because the partition is blocked while we wait);
     * slow doubles from 5 s capped at 60 s (waiting is its job).
     */
    public function retryDelayMs(int $attempt): int
    {
        $attempt = max(1, $attempt);

        return match ($this) {
            self::Main => 1000 * (2 ** ($attempt - 1)),
            self::Slow => min(5000 * (2 ** ($attempt - 1)), 60000),
            self::Off => 0,
        };
    }

    /**
     * Consecutive transient failures that trip the breaker open.
     */
    public function breakerThreshold(): int
    {
        return 5;
    }

    /**
     * How long an open breaker stays open before the next message becomes the
     * half-open probe.
     */
    public function breakerCooldownMs(): int
    {
        return match ($this) {
            self::Slow => 30000,
            self::Main, self::Off => 10000,
        };
    }

    /**
     * What "open" means for the lane: true = pause consumption for the cooldown
     * (slow); false = fail fast and off-load immediately (main).
     */
    public function pausesWhenOpen(): bool
    {
        return self::Slow === $this;
    }
}
