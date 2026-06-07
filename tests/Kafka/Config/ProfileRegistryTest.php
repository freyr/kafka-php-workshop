<?php

declare(strict_types=1);

namespace Workshop\Tests\Kafka\Config;

use PHPUnit\Framework\TestCase;
use Workshop\Kafka\Config\ClientRole;
use Workshop\Kafka\Config\KafkaProfile;
use Workshop\Kafka\Config\KafkaTuning;
use Workshop\Kafka\Config\ProfileRegistry;

final class ProfileRegistryTest extends TestCase
{
    private ProfileRegistry $registry;

    protected function setUp(): void
    {
        $this->registry = new ProfileRegistry(new KafkaTuning());
    }

    public function testIdempotentProducerCarriesTheReliabilityTrio(): void
    {
        $profile = $this->registry->get('producer.idempotent');

        self::assertSame(ClientRole::Producer, $profile->role);
        $kv = $this->toKeyValue($profile);
        self::assertSame('true', $kv['enable.idempotence'] ?? null);
        self::assertSame('all', $kv['acks'] ?? null);
        self::assertSame('lz4', $kv['compression.type'] ?? null);
    }

    public function testSimpleProducerHasNoOverrides(): void
    {
        $profile = $this->registry->get('producer.simple');

        self::assertSame(ClientRole::Producer, $profile->role);
        self::assertSame([], $profile->settings);
    }

    public function testAtLeastOnceConsumerCarriesCommitAndRebalanceSettings(): void
    {
        $profile = $this->registry->get('consumer.at-least-once');

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
        $profile = $this->registry->get('consumer.ephemeral');

        $kv = $this->toKeyValue($profile);
        self::assertSame('earliest', $kv['auto.offset.reset'] ?? null);
        // No static membership — a throwaway group must not be fenced on re-run.
        self::assertArrayNotHasKey('group.instance.id', $kv);
    }

    public function testUnknownProfileThrowsWithKnownNames(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('producer.simple');

        $this->registry->get('does-not-exist');
    }

    public function testAllReturnsTheFourRegisteredProfiles(): void
    {
        $names = array_map(static fn (KafkaProfile $p): string => $p->name, $this->registry->all());

        self::assertSame([
            'producer.simple',
            'producer.idempotent',
            'consumer.at-least-once',
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
