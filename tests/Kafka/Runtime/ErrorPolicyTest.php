<?php

declare(strict_types=1);

namespace Workshop\Tests\Kafka\Runtime;

use PHPUnit\Framework\TestCase;
use Workshop\Kafka\Runtime\ErrorPolicy;

final class ErrorPolicyTest extends TestCase
{
    public function testResolvesFromOption(): void
    {
        self::assertSame(ErrorPolicy::Main, ErrorPolicy::fromOption('main'));
        self::assertSame(ErrorPolicy::Slow, ErrorPolicy::fromOption('slow'));
        self::assertSame(ErrorPolicy::Off, ErrorPolicy::fromOption('off'));
    }

    public function testRejectsAnUnknownOption(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        ErrorPolicy::fromOption('aggressive');
    }

    public function testOnlyOffIsDisabled(): void
    {
        self::assertTrue(ErrorPolicy::Main->enabled());
        self::assertTrue(ErrorPolicy::Slow->enabled());
        self::assertFalse(ErrorPolicy::Off->enabled());
    }

    public function testMainBudgetIsTinyAndSlowIsUnbounded(): void
    {
        self::assertSame(3, ErrorPolicy::Main->maxAttempts());
        self::assertNull(ErrorPolicy::Slow->maxAttempts(), 'the slow lane never exhausts — waiting is its job');
    }

    public function testMainDelaysDoubleFromOneSecond(): void
    {
        self::assertSame(1000, ErrorPolicy::Main->retryDelayMs(1));
        self::assertSame(2000, ErrorPolicy::Main->retryDelayMs(2));
        self::assertSame(4000, ErrorPolicy::Main->retryDelayMs(3));
    }

    public function testSlowDelaysDoubleFromFiveSecondsCappedAtSixty(): void
    {
        self::assertSame(5000, ErrorPolicy::Slow->retryDelayMs(1));
        self::assertSame(10000, ErrorPolicy::Slow->retryDelayMs(2));
        self::assertSame(20000, ErrorPolicy::Slow->retryDelayMs(3));
        self::assertSame(40000, ErrorPolicy::Slow->retryDelayMs(4));
        self::assertSame(60000, ErrorPolicy::Slow->retryDelayMs(5), 'capped');
        self::assertSame(60000, ErrorPolicy::Slow->retryDelayMs(12), 'stays capped however long it runs');
    }

    public function testBreakerOpenBehaviorIsLaneDependent(): void
    {
        self::assertFalse(ErrorPolicy::Main->pausesWhenOpen(), 'main fails fast — the partition keeps moving');
        self::assertTrue(ErrorPolicy::Slow->pausesWhenOpen(), 'slow pauses — off-loading from the retry topic has nowhere better to go');
    }

    public function testSlowCooldownOutlastsMain(): void
    {
        self::assertGreaterThan(
            ErrorPolicy::Main->breakerCooldownMs(),
            ErrorPolicy::Slow->breakerCooldownMs(),
        );
    }
}
