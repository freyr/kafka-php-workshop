<?php

declare(strict_types=1);

namespace Workshop\Kafka\Runtime;

/**
 * The rebalance protocol librdkafka negotiates from partition.assignment.strategy,
 * and the only thing that decides which assign API the rebalance callback may call.
 *
 *  - Eager: range/roundrobin (or no strategy set — librdkafka's default). Every
 *    rebalance revokes the whole assignment, so the callback responds with
 *    assign($all) / assign(null).
 *  - Cooperative: cooperative-sticky. Only moving partitions change hands, so the
 *    callback responds incrementally with incrementalAssign() / incrementalUnassign().
 *
 * Calling the wrong API for the negotiated protocol is not lenient — librdkafka
 * throws ("Changes to the current assignment must be made using assign() when
 * rebalance protocol type is EAGER"), which kills the consumer on its first
 * assignment. So this is derived from the SAME setting the broker negotiates on,
 * never declared independently.
 */
enum RebalanceProtocol
{
    case Eager;
    case Cooperative;

    /**
     * Map a partition.assignment.strategy value to its protocol. cooperative-sticky
     * is the only cooperative assignor; range, roundrobin, and an unset strategy are
     * all eager.
     */
    public static function fromAssignmentStrategy(?string $strategy): self
    {
        return null !== $strategy && str_contains($strategy, 'cooperative')
            ? self::Cooperative
            : self::Eager;
    }
}
