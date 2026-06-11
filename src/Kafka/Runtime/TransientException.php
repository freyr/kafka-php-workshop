<?php

declare(strict_types=1);

namespace Workshop\Kafka\Runtime;

/**
 * Marks a handler failure as retryable: the message is fine, the handler just
 * cannot process it RIGHT NOW (a dependency timed out, a downstream is briefly
 * gone). This is the only exception that earns retries — `kafka:consume
 * --errors` retries it in-process, then off-loads to the retry topic. Every
 * other handler throwable is treated as permanent and dead-lettered with zero
 * retries: a falsely dead-lettered transient costs one manual replay, while a
 * falsely retried permanent costs partition liveness.
 */
final class TransientException extends \RuntimeException
{
}
