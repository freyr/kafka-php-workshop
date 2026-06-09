<?php

declare(strict_types=1);

namespace Workshop\Tests\Kafka\Config;

use PHPUnit\Framework\TestCase;
use Workshop\Kafka\Config\ClientRole;
use Workshop\Kafka\Config\KafkaProfile;
use Workshop\Kafka\Config\KafkaProfiles;

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

    public function testAtLeastOnceConsumerCarriesCommitAndRebalanceSettings(): void
    {
        $profile = $this->profiles->get('consumer.at-least-once');

        self::assertSame(ClientRole::Consumer, $profile->role);
        $kv = $this->toKeyValue($profile);
        self::assertSame('false', $kv['enable.auto.commit'] ?? null);
        self::assertSame('earliest', $kv['auto.offset.reset'] ?? null);
        self::assertSame('cooperative-sticky', $kv['partition.assignment.strategy'] ?? null);
        // Static membership: the at-least-once consumer pins group.instance.id.
        self::assertArrayHasKey('group.instance.id', $kv);
    }

    public function testEphemeralConsumerDropsStaticMembership(): void
    {
        $profile = $this->profiles->get('consumer.ephemeral');

        $kv = $this->toKeyValue($profile);
        self::assertSame('earliest', $kv['auto.offset.reset'] ?? null);
        // No static membership — a throwaway group must not be fenced on re-run.
        self::assertArrayNotHasKey('group.instance.id', $kv);
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
            'consumer.at-least-once',
            'consumer.dynamic',
            'consumer.ephemeral',
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
