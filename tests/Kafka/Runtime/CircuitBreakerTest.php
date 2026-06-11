<?php

declare(strict_types=1);

namespace Workshop\Tests\Kafka\Runtime;

use PHPUnit\Framework\TestCase;
use Workshop\Kafka\Runtime\CircuitBreaker;

final class CircuitBreakerTest extends TestCase
{
    public function testStaysClosedBelowTheThreshold(): void
    {
        $breaker = new CircuitBreaker(threshold: 3, cooldownMs: 1000);

        self::assertFalse($breaker->onTransientFailure(0));
        self::assertFalse($breaker->onTransientFailure(10));
        self::assertFalse($breaker->isOpen());
        self::assertTrue($breaker->allowsAttempt(20));
        self::assertSame(2, $breaker->consecutiveFailures());
    }

    public function testOpensAtTheThresholdAndBlocksDuringCooldown(): void
    {
        $breaker = new CircuitBreaker(threshold: 3, cooldownMs: 1000);

        $breaker->onTransientFailure(0);
        $breaker->onTransientFailure(10);
        self::assertTrue($breaker->onTransientFailure(20), 'the threshold-reaching failure reports the open transition');

        self::assertTrue($breaker->isOpen());
        self::assertFalse($breaker->allowsAttempt(500), 'no attempts inside the cooldown');
        self::assertSame(520, $breaker->remainingCooldownMs(500));
    }

    public function testHalfOpenAfterTheCooldownAllowsAProbe(): void
    {
        $breaker = $this->openedAt(0, cooldownMs: 1000);

        self::assertFalse($breaker->isHalfOpen(999));
        self::assertTrue($breaker->isHalfOpen(1000));
        self::assertTrue($breaker->allowsAttempt(1000), 'the cooldown elapsed — the next attempt is the probe');
    }

    public function testProbeSuccessClosesTheBreaker(): void
    {
        $breaker = $this->openedAt(0, cooldownMs: 1000);

        self::assertTrue($breaker->onSuccess(), 'closing an open breaker reports the transition');
        self::assertFalse($breaker->isOpen());
        self::assertSame(0, $breaker->consecutiveFailures());
        self::assertTrue($breaker->allowsAttempt(1));
    }

    public function testProbeFailureReopensForAFreshCooldown(): void
    {
        $breaker = $this->openedAt(0, cooldownMs: 1000);

        self::assertTrue($breaker->onTransientFailure(1500), 're-opening reports the transition');
        self::assertFalse($breaker->allowsAttempt(2000), 'the cooldown restarts from the probe failure');
        self::assertTrue($breaker->allowsAttempt(2500));
    }

    public function testASuccessResetsTheConsecutiveCount(): void
    {
        $breaker = new CircuitBreaker(threshold: 3, cooldownMs: 1000);

        $breaker->onTransientFailure(0);
        $breaker->onTransientFailure(10);
        self::assertFalse($breaker->onSuccess(), 'a success while closed is not a transition');
        self::assertSame(0, $breaker->consecutiveFailures());

        // Two more failures stay under the threshold again.
        self::assertFalse($breaker->onTransientFailure(20));
        self::assertFalse($breaker->onTransientFailure(30));
        self::assertFalse($breaker->isOpen());
    }

    private function openedAt(int $nowMs, int $cooldownMs): CircuitBreaker
    {
        $breaker = new CircuitBreaker(threshold: 1, cooldownMs: $cooldownMs);
        $breaker->onTransientFailure($nowMs);

        return $breaker;
    }
}
