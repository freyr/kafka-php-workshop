<?php

declare(strict_types=1);

namespace Workshop\Kafka\Runtime;

/**
 * Thrown by the error-handling consume path when a SIGINT/SIGTERM arrives while
 * a message is mid-retry. Returning normally would let the run loop COMMIT a
 * message that was never handled, so the abort travels as an exception instead:
 * it propagates out of the run loop (which still closes the consumer in its
 * finally), the offset stays uncommitted, and the message redelivers on the next
 * run — at-least-once survives Ctrl+C.
 */
final class ConsumerInterrupted extends \RuntimeException
{
}
