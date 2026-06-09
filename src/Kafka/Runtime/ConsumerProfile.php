<?php

declare(strict_types=1);

namespace Workshop\Kafka\Runtime;

/**
 * The three consumer lanes `kafka:consume` exposes via --profile. Each lane names
 * one KafkaProfile (the librdkafka config) and decides two run-loop behaviors that
 * are NOT expressible as librdkafka settings: when the loop commits, and whether
 * records reach the application at all.
 *
 *  - Ephemeral: throwaway inspector. consumer.ephemeral config (earliest, never
 *    commits, no static membership); the loop never commits and the command skips
 *    every record (prints name/id off the headers — no decode, no handler).
 *  - Default: consumer.default config (background auto-commit, eager
 *    range,roundrobin rebalancing). The loop leaves committing to librdkafka.
 *  - Modern: consumer.modern config (cooperative-sticky + static membership). The
 *    loop commits explicitly after each handler.
 *
 * Delivery semantics (at-least-once / effectively-once) are orthogonal — they live
 * in handler/DB dedup, toggled by --idempotent, not in this enum.
 */
enum ConsumerProfile: string
{
    case Ephemeral = 'ephemeral';
    case Default = 'default';
    case Modern = 'modern';

    public static function fromOption(string $value): self
    {
        return self::tryFrom($value)
            ?? throw new \InvalidArgumentException(sprintf('Unknown consumer profile "%s". Use one of: %s.', $value, implode(', ', array_map(static fn (self $p): string => $p->value, self::cases()))));
    }

    /**
     * The named KafkaProfile this lane builds.
     */
    public function profileName(): string
    {
        return match ($this) {
            self::Ephemeral => 'consumer.ephemeral',
            self::Default => 'consumer.default',
            self::Modern => 'consumer.modern',
        };
    }

    /**
     * Ephemeral inspects without consuming into the application: the command prints
     * each record's name/id and never decodes, dispatches, or commits.
     */
    public function inspectsOnly(): bool
    {
        return self::Ephemeral === $this;
    }

    /**
     * When the run-loop commits. Modern commits explicitly after each handler;
     * default leaves it to librdkafka's background auto-commit; ephemeral never
     * commits.
     *
     * $dedup reports whether the handler dedups (the --idempotent path). When it
     * does, modern can commit ASYNCHRONOUSLY — the redelivery a lost async commit
     * would cause is a no-op — trading a per-message broker round-trip for speed
     * while staying at-least-once. Without dedup, modern commits synchronously.
     */
    public function commitPolicy(bool $dedup = false): CommitPolicy
    {
        return match ($this) {
            self::Modern => $dedup ? CommitPolicy::AsyncAfterEachMessage : CommitPolicy::AfterEachMessage,
            self::Default, self::Ephemeral => CommitPolicy::None,
        };
    }
}
