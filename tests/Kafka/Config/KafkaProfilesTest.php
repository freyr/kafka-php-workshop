<?php

declare(strict_types=1);

namespace Workshop\Tests\Kafka\Config;

use PHPUnit\Framework\TestCase;
use Workshop\Kafka\Config\ClientRole;
use Workshop\Kafka\Config\KafkaProfile;
use Workshop\Kafka\Config\KafkaProfiles;
use Workshop\Kafka\Runtime\RebalanceProtocol;

final class KafkaProfilesTest extends TestCase
{
    private KafkaProfiles $profiles;

    protected function setUp(): void
    {
        $this->profiles = new KafkaProfiles();
    }

    public function testIdempotentProducerCarriesTheFullProductionTuning(): void
    {
        $profile = $this->profiles->get('producer.idempotent');

        self::assertSame(ClientRole::Producer, $profile->role);
        $kv = $this->toKeyValue($profile);
        // Reliability + compression: the exactly-once trio.
        self::assertSame('true', $kv['enable.idempotence'] ?? null);
        self::assertSame('all', $kv['acks'] ?? null);
        self::assertSame('lz4', $kv['compression.type'] ?? null);
        // Batching, queue bounds and timeouts: the rest of the production tuning.
        self::assertSame('50', $kv['linger.ms'] ?? null);
        self::assertSame('10000', $kv['batch.num.messages'] ?? null);
        self::assertSame('1000000', $kv['batch.size'] ?? null);
        self::assertSame('100000', $kv['queue.buffering.max.messages'] ?? null);
        self::assertSame('262144', $kv['queue.buffering.max.kbytes'] ?? null);
        self::assertSame('300000', $kv['delivery.timeout.ms'] ?? null);
        self::assertSame('30000', $kv['request.timeout.ms'] ?? null);
    }

    public function testSimpleProducerHasNoOverrides(): void
    {
        $profile = $this->profiles->get('producer.simple');

        self::assertSame(ClientRole::Producer, $profile->role);
        self::assertSame([], $profile->settings);
    }

    public function testModernConsumerCommitsExplicitlyWithCooperativeStickyAndStaticMembership(): void
    {
        $profile = $this->profiles->get('consumer.modern');

        self::assertSame(ClientRole::Consumer, $profile->role);
        $kv = $this->toKeyValue($profile);
        self::assertSame('false', $kv['enable.auto.commit'] ?? null);
        self::assertSame('earliest', $kv['auto.offset.reset'] ?? null);
        self::assertSame('cooperative-sticky', $kv['partition.assignment.strategy'] ?? null);
        // ...which the factory must read as the cooperative protocol, so the rebalance
        // callback uses incrementalAssign rather than the eager assign API.
        self::assertSame(RebalanceProtocol::Cooperative, RebalanceProtocol::fromAssignmentStrategy($profile->setting('partition.assignment.strategy')));
        // Static membership: the modern consumer pins group.instance.id.
        self::assertArrayHasKey('group.instance.id', $kv);
    }

    public function testDefaultConsumerAutoCommitsWithEagerRebalancingAndNoStaticMembership(): void
    {
        $profile = $this->profiles->get('consumer.default');

        self::assertSame(ClientRole::Consumer, $profile->role);
        $kv = $this->toKeyValue($profile);
        self::assertSame('true', $kv['enable.auto.commit'] ?? null);
        self::assertSame('earliest', $kv['auto.offset.reset'] ?? null);
        // Eager rebalancing — the stop-the-world contrast to cooperative-sticky.
        self::assertSame('range,roundrobin', $kv['partition.assignment.strategy'] ?? null);
        // The factory must read this as the eager protocol — calling incrementalAssign
        // here is what librdkafka rejects.
        self::assertSame(RebalanceProtocol::Eager, RebalanceProtocol::fromAssignmentStrategy($profile->setting('partition.assignment.strategy')));
        self::assertArrayNotHasKey('group.instance.id', $kv);
    }

    public function testEphemeralConsumerNeverCommitsAndDropsStaticMembership(): void
    {
        $profile = $this->profiles->get('consumer.ephemeral');

        $kv = $this->toKeyValue($profile);
        self::assertSame('earliest', $kv['auto.offset.reset'] ?? null);
        // Never commits, even in the background — the throwaway inspector.
        self::assertSame('false', $kv['enable.auto.commit'] ?? null);
        // No static membership — a throwaway group must not be fenced on re-run.
        self::assertArrayNotHasKey('group.instance.id', $kv);
        // No assignment override — the lone member needs no rebalancing strategy.
        self::assertArrayNotHasKey('partition.assignment.strategy', $kv);
        // An unset strategy still negotiates librdkafka's eager default, so the
        // factory must derive Eager — not assume cooperative for every profile.
        self::assertSame(RebalanceProtocol::Eager, RebalanceProtocol::fromAssignmentStrategy($profile->setting('partition.assignment.strategy')));
    }

    public function testUnknownProfileThrowsWithKnownNames(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('producer.simple');

        $this->profiles->get('does-not-exist');
    }

    public function testAllReturnsTheRegisteredProfiles(): void
    {
        $names = array_map(static fn (KafkaProfile $p): string => $p->name, $this->profiles->all());

        self::assertSame([
            'producer.simple',
            'producer.idempotent',
            'producer.dlq',
            'consumer.ephemeral',
            'consumer.default',
            'consumer.modern',
        ], $names);
    }

    /**
     * @return array<string, string>
     */
    private function toKeyValue(KafkaProfile $profile): array
    {
        $out = [];
        foreach ($profile->settings as $setting) {
            $out[$setting->key] = $setting->value;
        }

        return $out;
    }
}
