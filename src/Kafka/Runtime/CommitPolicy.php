<?php

declare(strict_types=1);

namespace Workshop\Kafka\Runtime;

/**
 * When the run-loop commits offsets.
 *
 *  - AfterEachMessage: commit synchronously AFTER the handler returns — the
 *    at-least-once guarantee (a crash before commit reprocesses, never loses).
 *    Each commit blocks on a broker round-trip, so it is the safe-but-slow choice.
 *  - AsyncAfterEachMessage: request the commit asynchronously after the handler
 *    (no per-message round-trip), then commit the last handled message
 *    synchronously on close so the final offset is durable. Faster, but a crash
 *    can lose an in-flight async commit and redeliver — only safe when the handler
 *    dedups (so reprocessing is a no-op). Still at-least-once, never at-most-once.
 *  - None: never commit — a read-only tail or a throwaway/ephemeral group, or a
 *    consumer that leaves committing to librdkafka's background auto-commit.
 *
 * Richer policies (read-committed for transactions) arrive with Block 5.
 */
enum CommitPolicy
{
    case AfterEachMessage;
    case AsyncAfterEachMessage;
    case None;
}
