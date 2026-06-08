<?php

declare(strict_types=1);

namespace Workshop\Kafka\Runtime;

/**
 * Where a consume run starts reading, independent of librdkafka's auto.offset.reset
 * (which only fires when a group has NO committed offset). These three model the
 * three things the room actually asks for:
 *
 *  - Committed: honour the group's stored offsets — normal resume. No seek.
 *  - Beginning: replay the whole log from the earliest offset, even if the group
 *    has already committed past it. Forces a seek on assignment.
 *  - End: skip the backlog and read only records produced from now on.
 *
 * Beginning/End are enforced in the rebalance callback by setting each assigned
 * partition's offset before it is taken up — see RebalanceCallback.
 */
enum OffsetReset: string
{
    case Committed = 'committed';
    case Beginning = 'beginning';
    case End = 'end';

    public static function fromOption(string $value): self
    {
        return self::tryFrom($value)
            ?? throw new \InvalidArgumentException(sprintf('Unknown offset reset "%s". Use one of: %s.', $value, implode(', ', array_map(static fn (self $c): string => $c->value, self::cases()))));
    }

    /**
     * The librdkafka partition offset to seek to on assignment, or null for
     * Committed (no seek — let the broker resume from the stored offset).
     */
    public function seekOffset(): ?int
    {
        return match ($this) {
            self::Committed => null,
            self::Beginning => RD_KAFKA_OFFSET_BEGINNING,
            self::End => RD_KAFKA_OFFSET_END,
        };
    }
}
