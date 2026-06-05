<?php

declare(strict_types=1);

namespace Workshop\Kernel;

/**
 * Block 7 error classification: a *non-retryable* failure. The message will
 * never process successfully no matter how many times it is retried — a business
 * rule violation, a reference to data that does not and will not exist, a
 * negative quantity. Retrying it is wasted compute *and* blocks the partition.
 *
 * The consumer dead-letters a poison message immediately, with zero retries.
 */
final class PoisonMessageException extends \RuntimeException
{
}
