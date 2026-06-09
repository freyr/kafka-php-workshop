<?php

declare(strict_types=1);

namespace Workshop\Tests\Kafka\Runtime;

use PHPUnit\Framework\TestCase;
use Workshop\Kafka\Runtime\CommitPolicy;
use Workshop\Kafka\Runtime\ConsumerProfile;

final class ConsumerProfileTest extends TestCase
{
    public function testFromOptionParsesEveryCase(): void
    {
        self::assertSame(ConsumerProfile::Ephemeral, ConsumerProfile::fromOption('ephemeral'));
        self::assertSame(ConsumerProfile::DefaultLane, ConsumerProfile::fromOption('default'));
        self::assertSame(ConsumerProfile::Modern, ConsumerProfile::fromOption('modern'));
    }

    public function testFromOptionRejectsUnknownValue(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Unknown consumer profile "nope"');

        ConsumerProfile::fromOption('nope');
    }

    public function testEachLaneNamesItsKafkaProfile(): void
    {
        self::assertSame('consumer.ephemeral', ConsumerProfile::Ephemeral->profileName());
        self::assertSame('consumer.default', ConsumerProfile::DefaultLane->profileName());
        self::assertSame('consumer.modern', ConsumerProfile::Modern->profileName());
    }

    public function testOnlyEphemeralInspectsOnly(): void
    {
        self::assertTrue(ConsumerProfile::Ephemeral->inspectsOnly());
        self::assertFalse(ConsumerProfile::DefaultLane->inspectsOnly());
        self::assertFalse(ConsumerProfile::Modern->inspectsOnly());
    }

    public function testOnlyModernCommitsExplicitlyFromTheLoop(): void
    {
        self::assertSame(CommitPolicy::AfterEachMessage, ConsumerProfile::Modern->commitPolicy());
        // Default leaves committing to librdkafka's background auto-commit, and
        // ephemeral never commits — neither commits from the run-loop.
        self::assertSame(CommitPolicy::None, ConsumerProfile::DefaultLane->commitPolicy());
        self::assertSame(CommitPolicy::None, ConsumerProfile::Ephemeral->commitPolicy());
    }

    public function testModernCommitsAsyncWhenTheHandlerDedups(): void
    {
        // With dedup (--idempotent) a lost async commit only causes a no-op
        // redelivery, so modern can drop the per-message synchronous round-trip.
        self::assertSame(CommitPolicy::AsyncAfterEachMessage, ConsumerProfile::Modern->commitPolicy(true));
        // Dedup does not change the other lanes: default still auto-commits in the
        // background, ephemeral still never commits.
        self::assertSame(CommitPolicy::None, ConsumerProfile::DefaultLane->commitPolicy(true));
        self::assertSame(CommitPolicy::None, ConsumerProfile::Ephemeral->commitPolicy(true));
    }
}
