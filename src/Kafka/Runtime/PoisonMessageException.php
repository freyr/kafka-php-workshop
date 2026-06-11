<?php

declare(strict_types=1);

namespace Workshop\Kafka\Runtime;

/**
 * Marks a message that can NEVER succeed: it was routed to this consumer (its
 * message-name is ours) but its bytes cannot be decoded — broken AVRO, missing
 * Confluent framing, or no resolvable event id to dedup on. Retrying is pure
 * waste and blocks the partition; the only correct response is to re-produce
 * the original bytes to the DLQ and advance. Thrown by MessageInterpreter's
 * decode gate when `kafka:consume --errors` enables it; without the flag the
 * interpreter keeps its tolerant null-skip contract.
 */
final class PoisonMessageException extends \RuntimeException
{
}
