<?php

declare(strict_types=1);

namespace Workshop\Kafka\Runtime;

/**
 * The commit-rejection error codes a rebalance makes expected. Between handling a
 * record and committing its offset, a rebalance can revoke the partition or advance
 * the group generation; the broker then rejects the commit. That is not a fault —
 * the handler already ran, the offset is simply no longer ours to commit, so the
 * record is redelivered to the partition's new owner and at-least-once still holds.
 *
 * Named in one place so the synchronous commit path (which catches the thrown
 * Exception and reads its code) and the asynchronous offset-commit callback (which
 * is handed the code directly) agree on exactly which rejections are benign.
 */
final class RebalanceCommitError
{
    public static function matches(int $errCode): bool
    {
        return in_array($errCode, [
            RD_KAFKA_RESP_ERR_ILLEGAL_GENERATION,
            RD_KAFKA_RESP_ERR_REBALANCE_IN_PROGRESS,
            RD_KAFKA_RESP_ERR_UNKNOWN_MEMBER_ID,
        ], true);
    }
}
