<?php

declare(strict_types=1);

namespace Workshop\Kafka\Runtime;

/**
 * The three delivery-semantics the `kafka:consume` command exposes via --commit.
 * Each is a bundle of three orthogonal decisions the command reads off the case:
 * the librdkafka commit config, when the run-loop commits, and whether the
 * application dispatch is wrapped in the DB transaction + dedup middleware.
 *
 *  - PerMessage: explicit synchronous commit AFTER each handler returns
 *    (enable.auto.commit=false). At-least-once — a crash before the commit
 *    reprocesses, never loses.
 *  - Auto: librdkafka commits in the background on a timer
 *    (enable.auto.commit=true). Lowest overhead, but a commit can land before the
 *    handler finishes — at-most-once under failure.
 *  - Idempotent: explicit commit, plus the handler runs inside a DB transaction
 *    that dedups on event_id. Reprocessing is a no-op, so the visible effect is
 *    effectively-once — the only strategy that needs the database.
 *  - ReadOnly: a throwaway group that never commits and never reaches the handler.
 *    It only prints each record's name and id (read straight off the headers, no
 *    decode), so a topic can be inspected without joining a real group or touching
 *    the projection.
 */
enum ConsumeStrategy: string
{
    case PerMessage = 'per-message';
    case Auto = 'auto';
    case Idempotent = 'idempotent';
    case ReadOnly = 'readonly';

    public static function fromOption(string $value): self
    {
        return self::tryFrom($value)
            ?? throw new \InvalidArgumentException(sprintf('Unknown commit strategy "%s". Use one of: %s.', $value, implode(', ', array_map(static fn (self $c): string => $c->value, self::cases()))));
    }

    public function commitPolicy(): CommitPolicy
    {
        // Auto leaves committing to librdkafka and ReadOnly never commits at all;
        // the rest commit explicitly after the handler so the offset only advances
        // once processing succeeded.
        return match ($this) {
            self::Auto, self::ReadOnly => CommitPolicy::None,
            self::PerMessage, self::Idempotent => CommitPolicy::AfterEachMessage,
        };
    }

    /**
     * Whether the application dispatch must be wrapped in the transaction + dedup
     * middleware. Only the idempotent strategy touches the database.
     */
    public function isTransactional(): bool
    {
        return self::Idempotent === $this;
    }

    /**
     * ReadOnly inspects without consuming into the application: no commit, no
     * handler — the command prints each record's name/id and moves on.
     */
    public function isReadOnly(): bool
    {
        return self::ReadOnly === $this;
    }

    /**
     * librdkafka conf overrides this strategy layers onto the consumer profile.
     *
     * @return array<string, string>
     */
    public function confOverrides(int $autoCommitIntervalMs): array
    {
        return match ($this) {
            self::Auto => [
                'enable.auto.commit' => 'true',
                'auto.commit.interval.ms' => (string) $autoCommitIntervalMs,
            ],
            self::PerMessage, self::Idempotent, self::ReadOnly => [
                'enable.auto.commit' => 'false',
            ],
        };
    }
}
