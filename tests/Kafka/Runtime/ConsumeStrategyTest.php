<?php

declare(strict_types=1);

namespace Workshop\Tests\Kafka\Runtime;

use PHPUnit\Framework\TestCase;
use Workshop\Kafka\Runtime\CommitPolicy;
use Workshop\Kafka\Runtime\ConsumeStrategy;

final class ConsumeStrategyTest extends TestCase
{
    public function testFromOptionParsesEveryCase(): void
    {
        self::assertSame(ConsumeStrategy::PerMessage, ConsumeStrategy::fromOption('per-message'));
        self::assertSame(ConsumeStrategy::Auto, ConsumeStrategy::fromOption('auto'));
        self::assertSame(ConsumeStrategy::Idempotent, ConsumeStrategy::fromOption('idempotent'));
        self::assertSame(ConsumeStrategy::ReadOnly, ConsumeStrategy::fromOption('readonly'));
    }

    public function testFromOptionRejectsUnknownValue(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Unknown commit strategy "nope"');

        ConsumeStrategy::fromOption('nope');
    }

    public function testOnlyAutoAndReadOnlyLeaveExplicitCommitsBehind(): void
    {
        self::assertSame(CommitPolicy::AfterEachMessage, ConsumeStrategy::PerMessage->commitPolicy());
        self::assertSame(CommitPolicy::AfterEachMessage, ConsumeStrategy::Idempotent->commitPolicy());
        self::assertSame(CommitPolicy::None, ConsumeStrategy::Auto->commitPolicy());
        self::assertSame(CommitPolicy::None, ConsumeStrategy::ReadOnly->commitPolicy());
    }

    public function testOnlyIdempotentIsTransactional(): void
    {
        self::assertTrue(ConsumeStrategy::Idempotent->isTransactional());
        self::assertFalse(ConsumeStrategy::PerMessage->isTransactional());
        self::assertFalse(ConsumeStrategy::Auto->isTransactional());
        self::assertFalse(ConsumeStrategy::ReadOnly->isTransactional());
    }

    public function testOnlyReadOnlyIsReadOnly(): void
    {
        self::assertTrue(ConsumeStrategy::ReadOnly->isReadOnly());
        self::assertFalse(ConsumeStrategy::PerMessage->isReadOnly());
    }

    public function testAutoEnablesBackgroundCommitWithTheGivenInterval(): void
    {
        self::assertSame(
            [
                'enable.auto.commit' => 'true',
                'auto.commit.interval.ms' => '2000',
            ],
            ConsumeStrategy::Auto->confOverrides(2000),
        );
    }

    public function testNonAutoStrategiesDisableAutoCommit(): void
    {
        foreach ([ConsumeStrategy::PerMessage, ConsumeStrategy::Idempotent, ConsumeStrategy::ReadOnly] as $strategy) {
            self::assertSame([
                'enable.auto.commit' => 'false',
            ], $strategy->confOverrides(5000));
        }
    }
}
