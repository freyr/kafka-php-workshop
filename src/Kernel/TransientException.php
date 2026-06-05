<?php

declare(strict_types=1);

namespace Workshop\Kernel;

/**
 * Block 7 error classification: a *retryable* failure. The operation failed for
 * a reason that is expected to resolve on its own — a DB timeout, an HTTP 503
 * from a downstream service, a Schema Registry blip. The consumer retries it
 * (in-process first, then via the retry-topic chain) before giving up to the DLT.
 *
 * This is the only category worth retrying. Everything else
 * ({@see PoisonMessageException}, a decode failure) goes straight to the DLT.
 */
final class TransientException extends \RuntimeException
{
}
