<?php

declare(strict_types=1);

namespace Workshop\Kafka\Runtime;

/**
 * When the run-loop commits offsets.
 *
 *  - AfterEachMessage: commit synchronously AFTER the handler returns — the
 *    at-least-once guarantee (a crash before commit reprocesses, never loses).
 *  - None: never commit — a read-only tail or a throwaway/ephemeral group.
 *
 * Richer policies (read-committed for transactions) arrive with Block 5.
 */
enum CommitPolicy
{
    case AfterEachMessage;
    case None;
}
