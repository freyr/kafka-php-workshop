<?php

declare(strict_types=1);

namespace Workshop\Tests\Kafka\Runtime;

use PHPUnit\Framework\TestCase;
use Workshop\Kafka\Runtime\CommitPolicy;
use Workshop\Kafka\Runtime\RunLimits;

final class RunLimitsTest extends TestCase
{
    public function testMaxMessagesUnlimitedWhenZero(): void
    {
        $limits = new RunLimits(maxMessages: 0);

        self::assertFalse($limits->reachedMax(0));
        self::assertFalse($limits->reachedMax(1_000_000));
    }

    public function testReachedMaxAtTheBoundary(): void
    {
        $limits = new RunLimits(maxMessages: 3);

        self::assertFalse($limits->reachedMax(2));
        self::assertTrue($limits->reachedMax(3));
        self::assertTrue($limits->reachedMax(4));
    }

    public function testRuntimeUnlimitedWhenZero(): void
    {
        $limits = new RunLimits(maxRuntimeSeconds: 0);

        self::assertFalse($limits->deadlinePassed(1000, 999_999));
    }

    public function testDeadlinePassedAtTheBoundary(): void
    {
        $limits = new RunLimits(maxRuntimeSeconds: 10);

        self::assertFalse($limits->deadlinePassed(1000, 1009));
        self::assertTrue($limits->deadlinePassed(1000, 1010));
        self::assertTrue($limits->deadlinePassed(1000, 1011));
    }

    public function testDefaultsAreReadUntilIdle(): void
    {
        $limits = new RunLimits();

        self::assertTrue($limits->stopOnIdle);
        self::assertSame(5000, $limits->pollTimeoutMs);
    }

    public function testCommitPolicyHasTheThreeExpectedCases(): void
    {
        self::assertCount(3, CommitPolicy::cases());
        self::assertContains(CommitPolicy::AfterEachMessage, CommitPolicy::cases());
        self::assertContains(CommitPolicy::AsyncAfterEachMessage, CommitPolicy::cases());
        self::assertContains(CommitPolicy::None, CommitPolicy::cases());
    }
}
