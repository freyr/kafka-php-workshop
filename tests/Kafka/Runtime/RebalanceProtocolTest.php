<?php

declare(strict_types=1);

namespace Workshop\Tests\Kafka\Runtime;

use PHPUnit\Framework\TestCase;
use Workshop\Kafka\Runtime\RebalanceProtocol;

final class RebalanceProtocolTest extends TestCase
{
    public function testCooperativeStickyIsCooperative(): void
    {
        self::assertSame(
            RebalanceProtocol::Cooperative,
            RebalanceProtocol::fromAssignmentStrategy('cooperative-sticky'),
        );
    }

    public function testRangeAndRoundrobinAreEager(): void
    {
        self::assertSame(
            RebalanceProtocol::Eager,
            RebalanceProtocol::fromAssignmentStrategy('range,roundrobin'),
        );
    }

    public function testAnUnsetStrategyIsEager(): void
    {
        // No partition.assignment.strategy override → librdkafka's default is eager.
        self::assertSame(
            RebalanceProtocol::Eager,
            RebalanceProtocol::fromAssignmentStrategy(null),
        );
    }

    public function testEmptyStrategyIsEager(): void
    {
        self::assertSame(
            RebalanceProtocol::Eager,
            RebalanceProtocol::fromAssignmentStrategy(''),
        );
    }

    public function testCooperativeIsDetectedWithinACompoundStrategy(): void
    {
        // librdkafka accepts a fallback list; any cooperative member negotiates the
        // cooperative protocol.
        self::assertSame(
            RebalanceProtocol::Cooperative,
            RebalanceProtocol::fromAssignmentStrategy('cooperative-sticky,range'),
        );
    }
}
